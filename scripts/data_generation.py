import os
import csv
import random
import sys
from faker import Faker
from datetime import datetime, timedelta, date

fake = Faker("en_IN")

# ---------------- CONFIG ----------------
# BASE_DIR = "data"
# OUT_DIR = os.path.join(BASE_DIR,"incoming")
OUT_DIR = "/opt/airflow/data/incoming"
# Dynamic ranges
MIN_CUSTOMERS = 30
MAX_CUSTOMERS = 80

MIN_TXN_PER_ACCOUNT = 3
MAX_TXN_PER_ACCOUNT = 8

MIN_LOANS = 10
MAX_LOANS = 30

TXN_DAYS_BACK = 30
LOAN_DAYS_BACK = 60

# % records on run date (helps daily dashboard)
TODAY_BIAS_TXN = 0.70
TODAY_BIAS_LOAN = 0.60

ACCOUNT_TYPES = ["Savings", "Current"]
LOAN_TYPES = ["Home", "Car", "Personal"]
LOAN_STATUS = ["Approved", "Pending", "Rejected"]
TRANSACTION_TYPES = ["Credit", "Debit"]

BRANCH_IDS = [f"BR{i}" for i in range(101, 111)]


# ----------------------------------------
# def ensure_out_dir():
#     os.makedirs(OUT_DIR, exist_ok=True)


def ensure_out_dir():
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)

def parse_run_date():
    """
    Reads Airflow ds format: YYYY-MM-DD
    Fallback = today's date
    """
    if len(sys.argv) > 1:
        try:
            return date.fromisoformat(sys.argv[1])
        except Exception:
            pass
    return date.today()


def current_run_timestamp(run_d: date) -> str:
    """
    Keep timestamp tied to run date
    """
    now_time = datetime.now().time().replace(microsecond=0)
    return datetime.combine(run_d, now_time).strftime("%Y-%m-%d %H:%M:%S")


def write_csv(filename, headers, rows):
    path = os.path.join(OUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Created: {path} | Rows: {len(rows)}")


def id_with_prefix(prefix: str, run_tag: str, n: int, width: int = 5) -> str:
    """
    Example:
    CUS20260227_00001
    ACC20260227_00001
    TNX20260227_0000001
    """
    return f"{prefix}{run_tag}_{str(n).zfill(width)}"


def biased_date(run_d: date, days_back: int, bias_today: float) -> str:
    """
    Most records fall on run date, rest spread in past dates
    """
    if random.random() < bias_today:
        return run_d.strftime("%Y-%m-%d")

    start_d = run_d - timedelta(days=days_back)
    rand_d = start_d + timedelta(days=random.randint(0, days_back))
    return rand_d.strftime("%Y-%m-%d")


def generate_unique_customer(existing_keys, idx, run_tag):
    """
    Ensures realistic uniqueness for customer records
    """
    while True:
        name = fake.name()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=60).strftime("%Y-%m-%d")
        phone = "".join(filter(str.isdigit, fake.phone_number()))[-10:]
        if len(phone) < 10:
            phone = str(random.randint(6000000000, 9999999999))
        clean_name = name.lower().replace(" ", ".").replace("'", "")
        email = f"{clean_name}{idx}{run_tag[-2:]}@bank.com"
        address = f"{fake.city()}"
        unique_key = (name, dob, phone)

        if unique_key not in existing_keys:
            existing_keys.add(unique_key)
        return name, dob, phone, email, address


def main():
    ensure_out_dir()

    run_d = parse_run_date()
    run_tag = run_d.strftime("%Y%m%d")
    file_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_tsp = current_run_timestamp(run_d)

    num_customers = random.randint(MIN_CUSTOMERS, MAX_CUSTOMERS)
    num_accounts = num_customers
    txn_per_account = random.randint(MIN_TXN_PER_ACCOUNT, MAX_TXN_PER_ACCOUNT)
    num_loans = random.randint(MIN_LOANS, MAX_LOANS)

    print("Run Date:", run_d.strftime("%Y-%m-%d"))
    print("Customers:", num_customers)
    print("Accounts:", num_accounts)
    print("Transactions per account:", txn_per_account)
    print("Loans:", num_loans)

    # ---------------- CUSTOMERS ----------------
    customers = []
    customer_ids = []
    existing_customers = set()

    for i in range(1, num_customers + 1):
        customer_id = id_with_prefix("CUS", run_tag, i, 5)
        name, dob, phone, email, address = generate_unique_customer(existing_customers, i, run_tag)

        customers.append([
            customer_id,
            name,
            dob,
            phone,
            email,
            address,
            run_tsp
        ])
        customer_ids.append(customer_id)

    write_csv(
        f"customer_details_{file_ts}.csv",
        ["customer_id", "name", "dob", "phone", "email", "address", "create_tsp"],
        customers
    )

    # ---------------- ACCOUNTS ----------------
    accounts = []
    used_account_numbers = set()

    for i in range(1, num_accounts + 1):
        account_id = id_with_prefix("ACC", run_tag, i, 5)
        customer_id = customer_ids[i - 1]

        while True:
            account_number = str(random.randint(1000000000, 9999999999))
            if account_number not in used_account_numbers:
                used_account_numbers.add(account_number)
                break

        branch_id = random.choice(BRANCH_IDS)
        account_type = random.choice(ACCOUNT_TYPES)

        accounts.append([
            account_id,
            customer_id,
            account_number,
            branch_id,
            account_type,
            run_tsp
        ])

    write_csv(
        f"account_details_{file_ts}.csv",
        ["account_id", "customer_id", "account_number", "branch_id", "account_type", "create_tsp"],
        accounts
    )

    # ---------------- TRANSACTIONS ----------------
    transactions = []
    txn_counter = 1

    for account in accounts:
        account_id = account[0]
        customer_id = account[1]

        for _ in range(txn_per_account):
            transaction_id = id_with_prefix("TNX", run_tag, txn_counter, 7)
            txn_counter += 1

            txn_date = biased_date(run_d, TXN_DAYS_BACK, TODAY_BIAS_TXN)
            txn_amount = round(random.uniform(100, 50000), 2)
            transaction_type = random.choice(TRANSACTION_TYPES)

            transactions.append([
                transaction_id,
                account_id,
                customer_id,
                txn_date,
                txn_amount,
                transaction_type,
                run_tsp
            ])

    write_csv(
        f"transaction_details_{file_ts}.csv",
        [
            "transaction_id",
            "account_id",
            "customer_id",
            "txn_date",
            "txn_amount",
            "transaction_type",
            "create_tsp"
        ],
        transactions
    )

    # ---------------- LOANS ----------------
    loans = []

    for i in range(1, num_loans + 1):
        loan_id = id_with_prefix("LOAN", run_tag, i, 4)
        customer_id = random.choice(customer_ids)
        loan_type = random.choice(LOAN_TYPES)
        loan_amount = round(random.uniform(50000, 900000), 2)
        loan_date = biased_date(run_d, LOAN_DAYS_BACK, TODAY_BIAS_LOAN)
        status = random.choice(LOAN_STATUS)

        loans.append([
            loan_id,
            customer_id,
            loan_type,
            loan_amount,
            loan_date,
            status,
            run_tsp
        ])

    write_csv(
        f"loan_application_details_{file_ts}.csv",
        ["loan_id", "customer_id", "loan_type", "loan_amount", "loan_date", "status", "created_at"],
        loans
    )

    print("\nAll files generated successfully.")
    print("Output Folder:", OUT_DIR)


if __name__ == "__main__":
    main()


