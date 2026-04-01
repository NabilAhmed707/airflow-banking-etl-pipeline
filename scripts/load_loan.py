import sys
import pandas as pd
import os
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

from db.db_connection import get_engine

file_path = sys.argv[1]
df = pd.read_csv(file_path)

if "loan_id" not in df.columns:
    df["loan_id"] = ["LOAN" + str(i+1) for i in range(len(df))]

if "loan_date" not in df.columns:
    df["loan_date"] = datetime.now().strftime("%Y-%m-%d")

if "created_at" not in df.columns:
    df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

required_cols = [
    "loan_id",
    "customer_id",
    "loan_type",
    "loan_amount",
    "loan_date",
    "status",
    "created_at"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise Exception(f"Missing columns in loan CSV: {missing}")

df = df[required_cols]

engine = get_engine()

df.to_sql("loan", engine, if_exists="append", index=False)

print("Loan file loaded successfully")
print("Rows:", len(df))