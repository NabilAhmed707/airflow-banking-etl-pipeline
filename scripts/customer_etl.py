import pandas as pd
from datetime import datetime
import os
import sys
import re
ISO_DATE = "%Y-%m-%d"
ISO_TS = "%Y-%m-%d %H:%M:%S"

def normalize_date(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        dt = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_DATE)

    s2 = s.replace("/", "-")
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", s2):
        dt = pd.to_datetime(s2, format="%d-%m-%Y", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_DATE)

    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.strftime(ISO_DATE)

def normalize_ts(val):
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}", s):
        dt = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        return None if pd.isna(dt) else dt.strftime(ISO_TS)

    dt = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return None if pd.isna(dt) else dt.strftime(ISO_TS)

def split_name(name):
    parts = str(name).strip().split()
    if len(parts) == 1:
        return parts[0], None, None
    if len(parts) == 2:
        return parts[0], None, parts[1]
    return parts[0], parts[1], parts[-1]

def calculate_age_from_iso(dob_iso):
    try:
        dob_date = datetime.strptime(dob_iso, ISO_DATE)
        today = datetime.today()
        return today.year - dob_date.year - ((today.month, today.day) < (dob_date.month, dob_date.day))
    except Exception:
        return None


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "db"))

from db.db_connection import get_engine
engine = get_engine()

# -------------------------
# Read from staging
# -------------------------
df = pd.read_sql("SELECT * FROM customer", engine)

if df.empty:
    print("No data found in customer table.")
    raise SystemExit(0)

df.replace("", pd.NA, inplace=True)

df[["FirstName", "MiddleName", "LastName"]] = df["name"].apply(lambda x: pd.Series(split_name(x)))
df["dob"] = df["dob"].apply(normalize_date)
df["age"] = df["dob"].apply(calculate_age_from_iso)

if "create_tsp" in df.columns:
    df["create_tsp"] = df["create_tsp"].apply(normalize_ts)

mandatory_columns = ["customer_id", "FirstName", "dob", "phone", "email"]
valid_condition = df[mandatory_columns].notnull().all(axis=1)

valid_df = df[valid_condition].copy()
invalid_df = df[~valid_condition].copy()

valid_df = valid_df.drop_duplicates(subset=["customer_id"])

existing_df = pd.read_sql("SELECT customer_id FROM customer_target", engine)
existing_ids = set(existing_df["customer_id"].astype(str))

new_records = valid_df[~valid_df["customer_id"].astype(str).isin(existing_ids)].copy()

current_time = datetime.now().strftime(ISO_TS)
new_records["update_tsp"] = current_time

if "create_tsp" not in new_records.columns:
    new_records["create_tsp"] = current_time
else:
    new_records["create_tsp"] = new_records["create_tsp"].fillna(current_time)

final_columns = [
    "customer_id",
    "FirstName",
    "MiddleName",
    "LastName",
    "dob",
    "age",
    "phone",
    "email",
    "address",
    "create_tsp",
    "update_tsp",
]
new_records = new_records[final_columns]

if not new_records.empty:
    new_records.to_sql("customer_target", engine, if_exists="append", index=False)
    print(f"{len(new_records)} new records inserted into customer_target")
else:
    print("No new records to insert.")

if not invalid_df.empty:
    rejection_folder = os.path.join(current_dir, "data", "rejection")
    os.makedirs(rejection_folder, exist_ok=True)

    invalid_df["rejection_reason"] = "Mandatory field NULL/empty or invalid DOB format"
    file_name = f"customer_reject_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    invalid_df.to_csv(os.path.join(rejection_folder, file_name), index=False)
    print(f"{len(invalid_df)} records written to rejection file")

print("Customer ETL Completed Successfully")
