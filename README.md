# ShopFlow ETL Pipeline

This project implements a simple **ETL (Extract, Transform, Load) pipeline** for the ShopFlow system.  
It extracts raw order data (CSV), transforms it into structured tables (customers, products, orders, order_items),  
and loads it into a MySQL database for reporting and analytics.

## 🚀 Features
- Extract orders from CSV with JSON item details.
- Transform into normalized relational tables.
- Load into MySQL with upserts to avoid duplicates.
- Logging and error handling.
- Ready for scheduling (cron, Task Scheduler, Airflow)

## 🛠 Tech Stack
- Python (pandas, SQLAlchemy, dotenv)
- MySQL
- SQL
- VS Code

## 📂 Project Structure
<img width="427" height="256" alt="image" src="https://github.com/user-attachments/assets/12516271-8b30-4542-a2c9-98e733e9b220" />

## ⚡ How to Run
1. Clone the repo:
   git clone https://github.com/SamadhiSamarasekara/shopflow-etl.git
   cd shopflow-etl

2. Create .env file with your MySQL settings.

3. Install dependencies:
   pip install -r requirements.txt


4. Run the ETL:
   python etl_shopflow.py --csv data/orders.csv

