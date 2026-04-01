import sys
import pandas as pd
import os

print(sys.executable)
# Import DB engine
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

from db.db_connection import get_engine

file_path = sys.argv[1]
df = pd.read_csv(file_path)

required_cols = ["customer_id", "name", "dob", "phone", "email", "address"]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise Exception(f"Missing columns in customer CSV: {missing}")

engine = get_engine()

df.to_sql("customer", engine, if_exists="append", index=False)

print("Customer file loaded successfully")
print("Rows:", len(df))