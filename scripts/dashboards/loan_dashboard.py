import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .style import apply_theme, style_axes, add_kpi_card
from sqlalchemy import text
def build_loan_dashboard(engine, run_date: str, mode: str = "daily"):
    apply_theme()

    if mode == "daily":
        query=text("""
            SELECT loan_type, loan_amount, loan_date, status
            FROM loan_target
            WHERE date(loan_date) = :run_date
        """)
        df = pd.read_sql_query(query, engine,  params={"run_date": run_date})
        title = f"Loan Dashboard (DAILY - {run_date})"
        kpi_label = "Total Loans"
    else:
        query=text("""
            SELECT loan_type, loan_amount, loan_date, status
            FROM loan_target
        """)
        df = pd.read_sql_query(query, engine)
        title = "Loan Dashboard (ALL TIME)"
        kpi_label = "Total Loans"

    df["loan_amount"] = pd.to_numeric(df["loan_amount"], errors="coerce")

    total_loans = len(df)
    total_amount = round(df["loan_amount"].sum(), 2) if total_loans else 0
    approved = int((df["status"].astype(str).str.lower() == "approved").sum()) if total_loans else 0

    status_counts = df["status"].value_counts()
    loan_amount_by_type = df.groupby("loan_type")["loan_amount"].sum().sort_index()

    fig = plt.figure(figsize=(14, 8))
    fig.suptitle(title, fontsize=18, fontweight="bold")

    add_kpi_card(fig, 0.05, 0.84, 0.22, 0.10, kpi_label, total_loans)
    add_kpi_card(fig, 0.29, 0.84, 0.22, 0.10, "Total Loan Amount", total_amount)
    add_kpi_card(fig, 0.53, 0.84, 0.22, 0.10, "Approved Loans", approved)

    ax1 = fig.add_axes([0.05, 0.10, 0.42, 0.68])
    ax2 = fig.add_axes([0.53, 0.10, 0.42, 0.68])

    if total_loans == 0:
        ax1.text(0.5, 0.5, "No data", ha="center", va="center")
        ax1.set_axis_off()
        ax2.set_axis_off()
        return fig

    ax1.bar(status_counts.index.astype(str), status_counts.values)
    ax1.set_title("Loan Status Distribution")
    style_axes(ax1)

    ax2.bar(loan_amount_by_type.index.astype(str), loan_amount_by_type.values)
    ax2.set_title("Total Loan Amount by Type")
    ax2.tick_params(axis="x", rotation=15)
    style_axes(ax2)

    return fig







