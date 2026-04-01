import sys
import pandas as pd
import os
from db.db_connection import get_engine
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

file_path = sys.argv[1]
df = pd.read_csv(file_path)

required_cols = [
    "account_id",
    "account_id",   # keeping same as your original logic
    "customer_id",
    "account_number",
    "branch_id",
    "account_type"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise Exception(f"Missing columns in account CSV: {missing}")

engine = get_engine()

df.to_sql("account", engine, if_exists="append", index=False)

print("Account file loaded successfully")
print("Rows:", len(df))


