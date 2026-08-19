import sqlite3
import pandas as pd
import logging
from datetime import datetime

# Setup logging format
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

class PipelineConfig:
    def __init__(self, excel_path="Python_Data_Pipeline_Lab_Dataset(1).xlsx", db_path="retail_dw.db"):
        self.excel_path = excel_path
        self.db_path = db_path

class DataWarehouseManager:
    def __init__(self, db_path, excel_path):
        self.db_path = db_path
        self.excel_path = excel_path
        self._init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Create Data Warehouse Tables
            cursor.executescript("""
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT UNIQUE NOT NULL,
                customer_name TEXT,
                province TEXT,
                segment TEXT
            );

            CREATE TABLE IF NOT EXISTS dim_product (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT UNIQUE NOT NULL,
                product_name TEXT,
                category TEXT
            );

            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date TEXT UNIQUE NOT NULL,
                day INTEGER,
                month INTEGER,
                quarter INTEGER,
                year INTEGER
            );

            CREATE TABLE IF NOT EXISTS fact_sales (
                order_id TEXT PRIMARY KEY,
                date_key INTEGER REFERENCES dim_date(date_key),
                customer_key INTEGER REFERENCES dim_customer(customer_key),
                product_key INTEGER REFERENCES dim_product(product_key),
                quantity INTEGER,
                unit_price REAL,
                discount_pct REAL,
                gross_amount REAL,
                net_amount REAL,
                payment_method TEXT,
                sales_channel TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS quarantine (
                quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                customer_id TEXT,
                product_id TEXT,
                reason_code TEXT NOT NULL,
                source_batch TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS pipeline_run_log (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT NOT NULL,
                rows_read INTEGER NOT NULL,
                rows_valid INTEGER NOT NULL,
                rows_rejected INTEGER NOT NULL,
                rows_loaded INTEGER NOT NULL,
                status TEXT NOT NULL
            );
            """)

            # 2. Seed Customer and Product Dimensions if empty
            cust_count = cursor.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
            if cust_count == 0:
                logging.info("Seeding dim_customer and dim_product...")
                df_cust = pd.read_excel(self.excel_path, sheet_name='customers')
                df_prod = pd.read_excel(self.excel_path, sheet_name='products')

                df_cust[['customer_id', 'customer_name', 'province', 'segment']].to_sql(
                    'dim_customer', conn, if_exists='append', index=False
                )
                df_prod[['product_id', 'product_name', 'category']].to_sql(
                    'dim_product', conn, if_exists='append', index=False
                )
                conn.commit()

class ETLPipeline:
    def __init__(self, config):
        self.config = config
        self.dw_manager = DataWarehouseManager(config.db_path, config.excel_path)

    def extract(self, batch_name):
        return pd.read_excel(self.config.excel_path, sheet_name=batch_name)

    def validate_and_transform(self, df_raw, batch_name):
        with self.dw_manager.get_connection() as conn:
            valid_cust = set(pd.read_sql("SELECT customer_id FROM dim_customer", conn)['customer_id'])
            valid_prod = set(pd.read_sql("SELECT product_id FROM dim_product", conn)['product_id'])

        valid_records = []
        rejected_records = []

        for _, row in df_raw.iterrows():
            rec = row.to_dict()
            cust_id = rec.get('customer_id')
            prod_id = rec.get('product_id')

            # FK Checks
            if pd.isna(cust_id) or str(cust_id) not in valid_cust:
                rejected_records.append({**rec, 'reason_code': 'INVALID_CUSTOMER_FK', 'source_batch': batch_name})
                continue

            if pd.isna(prod_id) or str(prod_id) not in valid_prod:
                rejected_records.append({**rec, 'reason_code': 'INVALID_PRODUCT_FK', 'source_batch': batch_name})
                continue

            # Date Validation (รองรับ order_datetime และ order_date)
            raw_date = rec.get('order_datetime') or rec.get('order_date')
            order_dt = pd.to_datetime(raw_date, errors='coerce')
            if pd.isna(order_dt):
                rejected_records.append({**rec, 'reason_code': 'INVALID_ORDER_DATE', 'source_batch': batch_name})
                continue
            rec['order_date_dt'] = order_dt

            # Numeric Checks
            try:
                qty = float(rec.get('quantity'))
                price = float(rec.get('unit_price'))
                disc = float(rec.get('discount_pct', 0))
                if qty <= 0 or price <= 0 or disc < 0 or disc > 100:
                    rejected_records.append({**rec, 'reason_code': 'INVALID_METRICS', 'source_batch': batch_name})
                    continue
            except Exception:
                rejected_records.append({**rec, 'reason_code': 'INVALID_METRICS', 'source_batch': batch_name})
                continue

            gross = round(qty * price, 2)
            net = round(gross * (1 - disc / 100.0), 2)

            rec['quantity'] = int(qty)
            rec['unit_price'] = float(price)
            rec['discount_pct'] = float(disc)
            rec['gross_amount'] = gross
            rec['net_amount'] = net
            rec['payment_method'] = str(rec.get('payment_method')).upper().strip()
            rec['sales_channel'] = str(rec.get('sales_channel')).capitalize().strip()

            valid_records.append(rec)

        return pd.DataFrame(valid_records), pd.DataFrame(rejected_records)

    def load(self, df_valid, df_rejected, batch_name, start_time):
        rows_loaded = 0
        with self.dw_manager.get_connection() as conn:
            cursor = conn.cursor()

            # Load Quarantine
            if not df_rejected.empty:
                q_cols = ['order_id', 'customer_id', 'product_id', 'reason_code', 'source_batch']
                for col in q_cols:
                    if col not in df_rejected.columns:
                        df_rejected[col] = None
                df_rejected[q_cols].to_sql('quarantine', conn, if_exists='append', index=False)

            # Load Valid Fact
            if not df_valid.empty:
                df_valid = df_valid.sort_values('updated_at').groupby('order_id').last().reset_index()

                # Insert Date Dim
                for dt in df_valid['order_date_dt']:
                    d_key = int(dt.strftime('%Y%m%d'))
                    cursor.execute("""
                        INSERT OR IGNORE INTO dim_date (date_key, full_date, day, month, quarter, year)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (d_key, dt.strftime('%Y-%m-%d'), dt.day, dt.month, dt.quarter, dt.year))

                cust_map = dict(cursor.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
                prod_map = dict(cursor.execute("SELECT product_id, product_key FROM dim_product").fetchall())
                existing_orders = set(r[0] for r in cursor.execute("SELECT order_id FROM fact_sales").fetchall())

                fact_rows = []
                for _, r in df_valid.iterrows():
                    oid = r['order_id']
                    if oid in existing_orders:
                        continue
                    d_key = int(r['order_date_dt'].strftime('%Y%m%d'))
                    c_key = cust_map[r['customer_id']]
                    p_key = prod_map[r['product_id']]

                    fact_rows.append((
                        oid, d_key, c_key, p_key,
                        r['quantity'], r['unit_price'], r['discount_pct'],
                        r['gross_amount'], r['net_amount'],
                        r['payment_method'], r['sales_channel'], str(r['updated_at'])
                    ))

                if fact_rows:
                    cursor.executemany("""
                        INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, fact_rows)
                    rows_loaded = len(fact_rows)

            # Log execution
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute("""
                INSERT INTO pipeline_run_log 
                (batch_name, started_at, ended_at, rows_read, rows_valid, rows_rejected, rows_loaded, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'SUCCESS')
            """, (
                batch_name, start_time, end_time, 
                len(df_valid) + len(df_rejected), len(df_valid), len(df_rejected), rows_loaded
            ))
            conn.commit()

        return rows_loaded

    def run(self):
        batches = ['orders_batch_1', 'orders_batch_2', 'orders_batch_3']
        for batch in batches:
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f"Extracting batch: {batch}")
            df_raw = self.extract(batch)
            df_valid, df_rejected = self.validate_and_transform(df_raw, batch)
            loaded_cnt = self.load(df_valid, df_rejected, batch, start_time)
            logging.info(f"Batch {batch} complete: Loaded {loaded_cnt} new records.")

        # Export Quarantine and Run Log to CSV automatically
        with self.dw_manager.get_connection() as conn:
            pd.read_sql("SELECT * FROM quarantine", conn).to_csv("quarantine.csv", index=False)
            pd.read_sql("SELECT * FROM pipeline_run_log", conn).to_csv("pipeline_run_log.csv", index=False)
            logging.info("Exported quarantine.csv and pipeline_run_log.csv successfully!")

if __name__ == "__main__":
    config = PipelineConfig(
        excel_path="Python_Data_Pipeline_Lab_Dataset.xlsx",
        db_path="retail_dw.db"
    )
    pipeline = ETLPipeline(config)
    pipeline.run()
