# ShopFlow ETL Pipeline

This project implements a simple **ETL (Extract, Transform, Load) pipeline** for the ShopFlow system.  
It extracts raw order data (CSV), transforms it into structured tables (customers, products, orders, order_items),  
and loads it into a MySQL database for reporting and analytics.

## 🚀 Features
- Extract orders from CSV with JSON item details.
- Transform into normalized relational tables.
- Load into MySQL with upserts to avoid duplicates.
- Logging and error handling.
- Ready for scheduling (cron, Task Scheduler, Airflow).

## 🛠 Tech Stack
- Python (pandas, SQLAlchemy, dotenv)
- MySQL
- SQL
- VS Code

## 📂 Project Structure
