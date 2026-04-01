import pandas as pd
import os
import sys
import re
from datetime import datetime
ISO_TS = "%Y-%m-%d %H:%M:%S"

def normalize_ts(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    # ISO timestamp
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", s):
        dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_TS)

    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.strftime(ISO_TS)
# ---------------------------
# DB Connection
# ---------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

from db.db_connection import get_engine
engine = get_engine()

query = """
SELECT 
    a.account_id,
    a.customer_id,
    a.account_number,
    a.branch_id,
    a.account_type,
    a.create_tsp,
    c.name AS customer_name
FROM account a
LEFT JOIN customer c
ON a.customer_id = c.customer_id
"""
df = pd.read_sql(query, engine)

if df.empty:
    print("No data found in account table.")
    raise SystemExit(0)

df.replace("", pd.NA, inplace=True)
df["account_number"] = df["account_number"].astype(str).str.replace(".0", "", regex=False).str.strip()
df["create_tsp"] = df["create_tsp"].apply(normalize_ts)

mandatory_columns = ["account_id", "customer_id", "customer_name", "account_number", "branch_id", "account_type"]
valid_condition = df[mandatory_columns].notnull().all(axis=1)

valid_df = df[valid_condition].copy()
invalid_df = df[~valid_condition].copy()

valid_df = valid_df.drop_duplicates(subset=["account_id"])

existing_df = pd.read_sql("SELECT account_id FROM account_target", engine)
existing_ids = set(existing_df["account_id"].astype(str))

new_records = valid_df[~valid_df["account_id"].astype(str).isin(existing_ids)].copy()

final_columns = [
    "account_id",
    "customer_id",
    "account_number",
    "branch_id",
    "account_type",
    "create_tsp",
    "customer_name",
]
new_records = new_records[final_columns]

if not new_records.empty:
    new_records.to_sql("account_target", engine, if_exists="append", index=False)
    print(f"{len(new_records)} new records inserted.")
else:
    print("No new account records.")

if not invalid_df.empty:
    rejection_folder = os.path.join(current_dir, "data", "rejection")
    os.makedirs(rejection_folder, exist_ok=True)

    invalid_df["rejection_reason"] = "Mandatory field NULL/empty"
    file_name = f"account_reject_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    invalid_df.to_csv(os.path.join(rejection_folder, file_name), index=False)
    print(f"{len(invalid_df)} records written to rejection file.")
