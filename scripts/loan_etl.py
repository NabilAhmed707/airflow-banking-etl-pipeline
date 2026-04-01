import pandas as pd
from datetime import datetime
import os
import sys
from sqlalchemy import text
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

def is_not_future(date_iso):
    try:
        d = datetime.strptime(date_iso, ISO_DATE)
        return d.date() <= datetime.today().date()
    except:
        return False
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
conn = engine.raw_connection()
cursor = conn.cursor()

query = """
SELECT 
    l.loan_id,
    l.customer_id,
    c.name AS customer_name,
    c.phone,
    c.dob,
    l.loan_type,
    l.loan_amount,
    l.loan_date,
    l.status,
    l.created_at AS created_dte
FROM loan l
LEFT JOIN customer c
ON l.customer_id = c.customer_id
"""
df = pd.read_sql(query, engine)

if df.empty:
    print("No loan data found.")
    raise SystemExit(0)

df.replace("", pd.NA, inplace=True)
df = df.drop_duplicates(subset=["loan_id"])

df["dob"] = df["dob"].apply(normalize_date)
df["loan_date"] = df["loan_date"].apply(normalize_date)
df["created_dte"] = df["created_dte"].apply(normalize_ts)

df["loan_amount"] = pd.to_numeric(df["loan_amount"], errors="coerce")

current_time = datetime.now().strftime(ISO_TS)

mandatory_cols = ["loan_id", "customer_id", "customer_name", "phone", "dob", "loan_type", "loan_amount", "loan_date", "status"]

valid_condition = (
    df[mandatory_cols].notnull().all(axis=1)
    & (df["loan_amount"] > 0)
    & df["loan_date"].apply(is_not_future)
)

valid_df = df[valid_condition].copy()
invalid_df = df[~valid_condition].copy()

existing_df = pd.read_sql("SELECT loan_id FROM loan_target", engine)
existing_ids = set(existing_df["loan_id"].astype(str))

updates = valid_df[valid_df["loan_id"].astype(str).isin(existing_ids)].copy()
new_df = valid_df[~valid_df["loan_id"].astype(str).isin(existing_ids)].copy()

for _, row in updates.iterrows():
    cursor.execute(
        """
        UPDATE loan_target
        SET 
            customer_id = %s,
            customer_name = %s,
            phone = %s,
            dob = %s,
            loan_type = %s,
            loan_amount = %s,
            loan_date = %s,
            status = %s,
            updated_dte = %s
        WHERE loan_id = %s
        """,
        (
            str(row["customer_id"]),
            str(row["customer_name"]),
            str(row["phone"]),
            str(row["dob"]),
            str(row["loan_type"]),
            float(row["loan_amount"]),
            str(row["loan_date"]),
            str(row["status"]),
            current_time,
            str(row["loan_id"]),
        ),
    )

if not updates.empty:
    print(f"{len(updates)} loan records updated.")

for _, row in new_df.iterrows():
    created_val = row["created_dte"] if pd.notna(row["created_dte"]) else current_time
    cursor.execute(
        """
        INSERT INTO loan_target (
            loan_id,
            customer_id,
            customer_name,
            phone,
            dob,
            loan_type,
            loan_amount,
            loan_date,
            status,
            created_dte,
            updated_dte
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(row["loan_id"]),
            str(row["customer_id"]),
            str(row["customer_name"]),
            str(row["phone"]),
            str(row["dob"]),
            str(row["loan_type"]),
            float(row["loan_amount"]),
            str(row["loan_date"]),
            str(row["status"]),
            str(created_val),
            current_time,
        ),
    )

if not new_df.empty:
    print(f"{len(new_df)} new loan records inserted.")

conn.commit()

if not invalid_df.empty:
    rejection_folder = os.path.join(current_dir, "data", "rejection")
    os.makedirs(rejection_folder, exist_ok=True)

    invalid_df["rejection_reason"] = "Mandatory field NULL/empty OR invalid date/amount"
    file_name = f"loan_reject_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    invalid_df.to_csv(os.path.join(rejection_folder, file_name), index=False)
    print(f"{len(invalid_df)} records written to rejection file.")


print("Loan ETL Completed Successfully")
