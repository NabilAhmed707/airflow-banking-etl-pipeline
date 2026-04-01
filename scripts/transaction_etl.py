import pandas as pd
from datetime import datetime
import os
import sys
from sqlalchemy import text
import re
ISO_DATE = "%Y-%m-%d"
ISO_TS = "%Y-%m-%d %H:%M:%S"


# -----------------------------
# Robust Normalizers
# -----------------------------
def normalize_date(val):
    """Return YYYY-MM-DD or None; supports YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    # 1) Strict ISO date
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        dt = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_DATE)

    # 2) Common DMY formats
    s2 = s.replace("/", "-")
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", s2):
        dt = pd.to_datetime(s2, format="%d-%m-%Y", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_DATE)

    # 3) Fallback (try both)
    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.strftime(ISO_DATE)


def normalize_ts(val):
    """Return YYYY-MM-DD HH:MM:SS or None; supports ISO + common formats."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    # ISO timestamp like 2026-02-27 15:20:10
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", s):
        dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_TS)

    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.strftime(ISO_TS)


def is_not_future(date_iso):
    try:
        d = datetime.strptime(date_iso, ISO_DATE).date()
        return d <= datetime.today().date()
    except Exception:
        return False


def normalize_txn_type(x):
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    mapping = {
        "CREDIT": "C", "CR": "C", "C": "C",
        "DEBIT": "D", "DR": "D", "D": "D",
    }
    return mapping.get(s, None)


def clean_account_number_like(s):
    if pd.isna(s):
        return None
    t = str(s).strip()
    t = t.replace(".0", "")
    return t
# ---------------------------
# DB Connection
# ---------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))
from db.db_connection import get_engine

engine = get_engine()

# ---------------------------
# Read ONLY TODAY'S DATA
# ---------------------------
txn_df = pd.read_sql('SELECT * FROM transactions', engine)

if txn_df.empty:
    print("No transactions found.")
    raise SystemExit(0)

txn_df.replace("", pd.NA, inplace=True)

# -----------------------------
# Reference tables for validation + account_id mapping
# -----------------------------
acc_ref = pd.read_sql("SELECT account_id FROM account", engine)
acc_ref["account_id"] = acc_ref["account_id"].astype(str).str.strip()

valid_accounts = set(acc_ref["account_id"])

cust_ref = pd.read_sql("SELECT customer_id FROM customer", engine)
cust_ref["customer_id"] = cust_ref["customer_id"].astype(str).str.strip()
valid_customers = set(cust_ref["customer_id"])

# -----------------------------
# Cleaning + Normalization
# -----------------------------
txn_df["transaction_id"] = txn_df["transaction_id"].astype(str).str.strip()
txn_df["customer_id"] = txn_df["customer_id"].astype(str).str.strip()

txn_df["account_id"] = txn_df["account_id"].apply(clean_account_number_like)

def normalize_account_id(val):
    if val is None:
        return None
    v = str(val).strip()
    if not v:
        return None
    if v.upper().startswith("ACC"):
        return v
    return None

txn_df["account_id"] = txn_df["account_id"].apply(normalize_account_id)
txn_df["transaction_type"] = txn_df["transaction_type"].apply(normalize_txn_type)
txn_df["txn_amount"] = pd.to_numeric(txn_df["txn_amount"], errors="coerce")
txn_df["txn_date"] = txn_df["txn_date"].apply(normalize_date)

if "create_tsp" in txn_df.columns:
    txn_df["create_tsp"] = txn_df["create_tsp"].apply(normalize_ts)

txn_df = txn_df.drop_duplicates(subset=["transaction_id"])

# -----------------------------
# Validation
# -----------------------------
invalid_reasons = []
for _, row in txn_df.iterrows():
    reasons = []
    if pd.isna(row["transaction_id"]) or str(row["transaction_id"]).strip() == "":
        reasons.append("Missing transaction_id")
    if pd.isna(row["customer_id"]) or row["customer_id"] not in valid_customers:
        reasons.append("Invalid/Missing customer_id")
    if pd.isna(row["account_id"]) or row["account_id"] not in valid_accounts:
        reasons.append("Invalid/Missing account_id")
    if pd.isna(row["txn_amount"]) or row["txn_amount"] <= 0:
        reasons.append("Invalid txn_amount")
    if pd.isna(row["txn_date"]):
        reasons.append("Invalid txn_date")
    else:
        if not is_not_future(row["txn_date"]):
            reasons.append("Future txn_date")
    if pd.isna(row["transaction_type"]):
        reasons.append("Invalid transaction_type")

    invalid_reasons.append("; ".join(reasons) if reasons else "")

txn_df["rejection_reason"] = invalid_reasons

valid_df = txn_df[txn_df["rejection_reason"] == ""].copy()
invalid_df = txn_df[txn_df["rejection_reason"] != ""].copy()

# -----------------------------
# Incremental Load
# -----------------------------
existing_df = pd.read_sql("SELECT transaction_id FROM transaction_target", engine)
existing_ids = set(existing_df["transaction_id"].astype(str))

new_records = valid_df[~valid_df["transaction_id"].astype(str).isin(existing_ids)].copy()

current_time = datetime.now().strftime(ISO_TS)
new_records["updated_tsp"] = current_time

if "create_tsp" not in new_records.columns:
    new_records["create_tsp"] = current_time
else:
    new_records["create_tsp"] = new_records["create_tsp"].fillna(current_time)

final_columns = [
    "transaction_id",
    "account_id",
    "customer_id",
    "txn_date",
    "txn_amount",
    "transaction_type",
    "create_tsp",
    "updated_tsp",
]
new_records = new_records[final_columns]

if not new_records.empty:
    new_records.to_sql("transaction_target", engine, if_exists="append", index=False)
    print(f"{len(new_records)} new transactions inserted.")
else:
    print("No new transactions.")

# -----------------------------
# Rejection file
# -----------------------------
if not invalid_df.empty:
    rejection_folder = os.path.join(current_dir, "data", "rejection")
    os.makedirs(rejection_folder, exist_ok=True)

    file_name = f"transaction_reject_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    invalid_df.to_csv(os.path.join(rejection_folder, file_name), index=False)
    print(f"{len(invalid_df)} records written to rejection file.")


print("Transaction ETL Completed Successfully")
