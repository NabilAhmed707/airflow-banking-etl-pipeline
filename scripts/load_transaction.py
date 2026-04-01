import sys
import pandas as pd
import os
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

from db.db_connection import get_engine

file_path = sys.argv[1]
df = pd.read_csv(file_path)

if "create_tsp" not in df.columns:
    df["create_tsp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

required_cols = [
    "transaction_id",
    "account_id",
    "customer_id",
    "txn_date",
    "txn_amount",
    "transaction_type",
    "create_tsp"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise Exception(f"Missing columns in transaction CSV: {missing}")

engine = get_engine()

df.to_sql("transactions", engine, if_exists="append", index=False)

print("Transaction file loaded successfully")
print("Rows:", len(df))
