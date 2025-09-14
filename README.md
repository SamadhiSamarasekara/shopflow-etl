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
shopflow-etl/
├─ data/ # sample raw data (ignored in git)
├─ etl_shopflow.py # main ETL script
├─ requirements.txt # dependencies
├─ .env # DB connection (ignored in git)
├─ .gitignore
└─ README.md

## ⚡ How to Run

1. Clone the repo:

- git clone https://github.com/SamadhiSamarasekara/shopflow-etl.git
- cd shopflow-etl

2. Create a .env file with your MySQL settings.

3. Install dependencies:
- pip install -r requirements.txt

4. Run the ETL:
- python etl_shopflow.py --csv data/orders.csv

