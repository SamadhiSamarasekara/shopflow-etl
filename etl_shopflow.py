# etl_shopflow.py
import os
import json
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------
# Setup logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------------
# Load env and DB engine
# ---------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")

ENGINE_STR = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(ENGINE_STR, pool_recycle=3600)

# ---------------------------
# Extract functions
# ---------------------------
def extract_csv(path):
    logger.info(f"Extracting CSV: {path}")
    df = pd.read_csv(path)
    return df

def extract_json(path):
    logger.info(f"Extracting JSON: {path}")
    df = pd.read_json(path, lines=True)  # adjust if single JSON array
    return df

# ---------------------------
# Transform functions
# ---------------------------
def normalize_items_column(df):
    """
    Convert 'items' column (JSON string) into a flat DataFrame of order items.
    Expects df to have 'order_uuid' and 'items'.
    """
    rows = []
    for _, r in df.iterrows():
        order_uuid = r['order_uuid']
        items = r.get('items')
        # If items is a string (JSON) -> parse
        if isinstance(items, str):
            try:
                items = json.loads(items)
            except Exception as e:
                logger.error(f"Failed to parse items JSON for order {order_uuid}: {e}")
                items = []
        if not items:
            continue
        for it in items:
            rows.append({
                'order_uuid': order_uuid,
                'sku': it.get('sku'),
                'product_name': it.get('name'),
                'quantity': int(it.get('quantity', 1)),
                'unit_price': float(it.get('unit_price', 0.0)),
                'line_total': float(it.get('quantity', 1) * it.get('unit_price', 0.0))
            })
    items_df = pd.DataFrame(rows)
    return items_df

# ---------------------------
# Load/upsert helpers
# ---------------------------
def upsert_customers(conn, customers_df):
    """
    Insert customers into customers table using INSERT ... ON DUPLICATE KEY UPDATE.
    Expects customers_df with columns: customer_uuid, name, email, phone, created_at
    """
    sql = text("""
    INSERT INTO customers (customer_uuid, name, email, phone, created_at)
    VALUES (:customer_uuid, :name, :email, :phone, :created_at)
    ON DUPLICATE KEY UPDATE name=VALUES(name), phone=VALUES(phone)
    """)
    for _, row in customers_df.iterrows():
        params = {
            "customer_uuid": row.get("customer_uuid"),
            "name": row.get("customer_name"),
            "email": row.get("customer_email"),
            "phone": row.get("customer_phone"),
            "created_at": row.get("order_date", datetime.utcnow())
        }
        conn.execute(sql, params)

def upsert_products(conn, products_df):
    sql = text("""
    INSERT INTO products (sku, name, price, stock, created_at)
    VALUES (:sku, :name, :price, :stock, :created_at)
    ON DUPLICATE KEY UPDATE name=VALUES(name), price=VALUES(price)
    """)
    for _, r in products_df.iterrows():
        conn.execute(sql, {
            "sku": r['sku'],
            "name": r.get('product_name'),
            "price": float(r.get('unit_price', 0.0)),
            "stock": int(r.get('stock', 0)) if 'stock' in r else 0,
            "created_at": datetime.utcnow()
        })

def get_customer_id(conn, email):
    # returns customer_id or None
    res = conn.execute(text("SELECT customer_id FROM customers WHERE email = :email"), {"email": email}).fetchone()
    return res[0] if res else None

def get_product_id(conn, sku):
    res = conn.execute(text("SELECT product_id FROM products WHERE sku = :sku"), {"sku": sku}).fetchone()
    return res[0] if res else None

def insert_order_and_items(conn, order_row, items_for_order):
    # Ensure customer exists
    email = order_row['customer_email']
    customer_id = get_customer_id(conn, email)
    if not customer_id:
        # Insert minimal customer
        ins = text("""
        INSERT INTO customers (customer_uuid, name, email, phone, created_at)
        VALUES (:uuid, :name, :email, :phone, :created_at)
        """)
        conn.execute(ins, {
            "uuid": order_row.get('customer_uuid'),
            "name": order_row.get('customer_name'),
            "email": order_row.get('customer_email'),
            "phone": order_row.get('customer_phone'),
            "created_at": order_row.get('order_date', datetime.utcnow())
        })
        customer_id = get_customer_id(conn, email)

    # Insert order (upsert)
    ins_order = text("""
    INSERT INTO orders (order_uuid, customer_id, order_date, total_amount, status, created_at)
    VALUES (:order_uuid, :customer_id, :order_date, :total_amount, :status, :created_at)
    ON DUPLICATE KEY UPDATE total_amount=VALUES(total_amount), status=VALUES(status)
    """)
    conn.execute(ins_order, {
        "order_uuid": order_row['order_uuid'],
        "customer_id": customer_id,
        "order_date": order_row['order_date'],
        "total_amount": float(order_row.get('total_amount', 0.0)),
        "status": order_row.get('status', 'unknown'),
        "created_at": order_row.get('order_date', datetime.utcnow())
    })

    # fetch order_id
    order_id = conn.execute(text("SELECT order_id FROM orders WHERE order_uuid = :uuid"), {"uuid": order_row['order_uuid']}).fetchone()[0]

    # Insert products (if not exist) and then items
    for _, item in items_for_order.iterrows():
        sku = item['sku']
        product_id = get_product_id(conn, sku)
        if not product_id:
            # insert minimal product
            conn.execute(text("INSERT INTO products (sku, name, price, created_at) VALUES (:sku, :name, :price, :created_at)"),
                         {"sku": sku, "name": item.get('product_name'), "price": float(item.get('unit_price', 0.0)), "created_at": datetime.utcnow()})
            product_id = get_product_id(conn, sku)

        # Insert order item (use INSERT ... ON DUPLICATE KEY UPDATE to avoid duplicates)
        ins_item = text("""
        INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total)
        VALUES (:order_id, :product_id, :quantity, :unit_price, :line_total)
        ON DUPLICATE KEY UPDATE quantity=VALUES(quantity), unit_price=VALUES(unit_price), line_total=VALUES(line_total)
        """)
        conn.execute(ins_item, {
            "order_id": order_id,
            "product_id": product_id,
            "quantity": int(item.get('quantity', 1)),
            "unit_price": float(item.get('unit_price', 0.0)),
            "line_total": float(item.get('line_total', 0.0))
        })

# ---------------------------
# Main ETL flow
# ---------------------------
def etl_from_csv(file_path):
    df = extract_csv(file_path)
    # basic cleanup & normalize column names
    df.rename(columns=lambda c: c.strip(), inplace=True)
    # parse dates
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

    # create items table from nested json
    items_df = normalize_items_column(df)

    # customers list (unique)
    # Use order_uuid instead of customer_uuid
    customers_df = df[['order_uuid', 'customer_name', 'customer_email', 'customer_phone', 'order_date']].drop_duplicates(subset=['customer_email'])


    # products list derived from items_df (unique by sku)
    products_df = items_df[['sku', 'product_name', 'unit_price']].drop_duplicates(subset=['sku'])

    # orders list
    orders_df = df[['order_uuid', 'customer_email', 'order_date', 'status', 'total_amount', 'customer_name', 'customer_phone']]

    with engine.begin() as conn:
        logger.info("Upserting customers...")
        upsert_customers(conn, customers_df)

        logger.info("Upserting products...")
        upsert_products(conn, products_df.rename(columns={'unit_price': 'price'}))

        # For each order, insert order and its items
        for order_id, order_row in orders_df.iterrows():
            uuid = order_row['order_uuid']
            order_items = items_df[items_df['order_uuid'] == uuid]
            logger.info(f"Inserting order {uuid} with {len(order_items)} items")
            insert_order_and_items(conn, order_row, order_items)

    logger.info("ETL from CSV completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ShopFlow ETL")
    parser.add_argument("--csv", help="path to orders CSV file", required=True)
    args = parser.parse_args()

    try:
        etl_from_csv(args.csv)
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        raise
