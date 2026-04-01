import os
import shutil
import subprocess
import sys
from datetime import datetime

# ---------------- CONFIG ----------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INCOMING_DIR = os.path.join(BASE_DIR, "data", "incoming")
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archive")
ERROR_DIR = os.path.join(BASE_DIR, "data", "error")
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")

# ✅ FIX: loan file name mismatch handle (loan_details + loan_application_details both)
FILE_SCRIPT_MAP = {
    "customer_details": "load_customer.py",
    "account_details": "load_account.py",
    "transaction_details": "load_transaction.py",

    # handle both naming conventions
    "loan_application_details": "load_loan.py",
    "loan_details": "load_loan.py",
}
# ---------------------------------------

def ensure_directories():
    for path in [ARCHIVE_DIR, ERROR_DIR]:
        os.makedirs(path, exist_ok=True)


def move_with_timestamp(src_path, target_dir):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_name = f"{timestamp}_{os.path.basename(src_path)}"
    dest_path = os.path.join(target_dir, new_name)

    shutil.move(src_path, dest_path)
    print(f"Moved to: {dest_path}")


def script_exists(script_name: str) -> bool:
    return os.path.isfile(os.path.join(SCRIPTS_DIR, script_name))


def process_file(file_name):
    file_path = os.path.join(INCOMING_DIR, file_name)
    file_name_lower = file_name.lower()

    matched = False

    for key, script in FILE_SCRIPT_MAP.items():
        if key in file_name_lower:
            matched = True

            if not script_exists(script):
                print("\n----------------------------------")
                print(f"Processing file: {file_name}")
                print(f"❌ Script not found: {script}")
                print("----------------------------------")
                move_with_timestamp(file_path, ERROR_DIR)
                return

            script_path = os.path.join(SCRIPTS_DIR, script)

            print("\n----------------------------------")
            print(f"Processing file: {file_name}")
            print(f"Matched key: {key}")
            print(f"Using script: {script}")
            print("----------------------------------")

            try:
                subprocess.run(
                    [sys.executable, script_path, file_path],
                    check=True
                )

                # ✅ SUCCESS → Move to ARCHIVE
                move_with_timestamp(file_path, ARCHIVE_DIR)
                print(f"SUCCESS: {file_name} archived\n")

            except subprocess.CalledProcessError as e:
                print(f"FAILED: {file_name}")
                print("Error:", e)

                # ❌ FAILED → Move to ERROR
                move_with_timestamp(file_path, ERROR_DIR)

            return  # stop after first match

    if not matched:
        print(f"No matching script found for {file_name}")
        move_with_timestamp(file_path, ERROR_DIR)


def main():
    ensure_directories()

    if not os.path.isdir(INCOMING_DIR):
        print("❌ Incoming folder not found:", INCOMING_DIR)
        return

    files = [
        f for f in os.listdir(INCOMING_DIR)
        if f.lower().endswith(".csv")
    ]

    if not files:
        print("No files found in incoming folder.")
        return

    # Optional: stable ordering (customer -> account -> transaction -> loan)
    priority = ["customer_details", "account_details", "transaction_details", "loan_application_details", "loan_details"]

    def sort_key(fname):
        low = fname.lower()
        for i, p in enumerate(priority):
            if p in low:
                return (i, low)
        return (999, low)

    files.sort(key=sort_key)

    print(f"\nFound {len(files)} file(s) to process...\n")

    for file_name in files:
        process_file(file_name)


if __name__ == "__main__":
    main()

