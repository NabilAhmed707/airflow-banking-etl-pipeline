import os
import sys
from datetime import datetime
from matplotlib.backends.backend_pdf import PdfPages

# ✅ Import your existing DB connection
from db.db_connection import get_engine

# ✅ Import dashboard builders
from dashboards.account_dashboard import build_account_dashboard
from dashboards.customer_dashboard import build_customer_dashboard
from dashboards.loan_dashboard import build_loan_dashboard
from dashboards.transaction_dashboard import build_transaction_dashboard


def get_paths():
    """
    Returns reports root path
    """
    scripts_dir = os.path.dirname(os.path.abspath(__file__))   # .../airflow-workspace/scripts
    project_root = os.path.dirname(scripts_dir)                # .../airflow-workspace
    reports_root = os.path.join(project_root, "dashboard_reports")

    return reports_root


def save_pdf(engine, pdf_path, run_date: str, mode: str):
    """
    mode: "daily" or "all"
    """

    figs = [
        build_account_dashboard(engine, run_date=run_date, mode=mode),
        build_customer_dashboard(engine, run_date=run_date, mode=mode),
        build_transaction_dashboard(engine, run_date=run_date, mode=mode),
        build_loan_dashboard(engine, run_date=run_date, mode=mode),
    ]

    with PdfPages(pdf_path) as pdf:
        for fig in figs:
            pdf.savefig(fig, bbox_inches="tight")
            fig.clf()


def main():

    # Get run date from Airflow argument OR default to today
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

    reports_root = get_paths()

    # Create root reports folder
    os.makedirs(reports_root, exist_ok=True)

    # Create date-wise folder
    day_folder = os.path.join(reports_root, run_date)
    os.makedirs(day_folder, exist_ok=True)

    daily_pdf = os.path.join(day_folder, f"Daily_Dashboards_{run_date}.pdf")
    all_pdf = os.path.join(day_folder, f"AllTime_Dashboards_{run_date}.pdf")

    # ✅ Connect to PostgreSQL using your connection function

    engine = get_engine()

    save_pdf(engine, daily_pdf, run_date, mode="daily")
    print(f"[OK] Daily PDF saved: {daily_pdf}")
    save_pdf(engine, all_pdf, run_date, mode="all")
    print(f"[OK] All-time PDF saved: {all_pdf}")





if __name__ == "__main__":
    main()




