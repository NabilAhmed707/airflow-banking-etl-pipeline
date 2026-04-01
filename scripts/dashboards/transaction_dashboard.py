import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .style import apply_theme, style_axes, add_kpi_card
from sqlalchemy import text
def build_transaction_dashboard(engine, run_date: str, mode: str = "daily"):
    apply_theme()

    if mode == "daily":
        query=text("""
            SELECT txn_amount, transaction_type, txn_date
            FROM transaction_target
            WHERE date(txn_date) = :run_date
        """)
        df = pd.read_sql_query(query, engine,  params={"run_date": run_date})
        title = f"Transaction Analysis Dashboard (DAILY - {run_date})"
        kpi_label = "Total Transactions"
    else:
        query=text("""
            SELECT txn_amount, transaction_type, txn_date
            FROM transaction_target
        """)
        df = pd.read_sql_query(query, engine)
        title = "Transaction Analysis Dashboard (ALL TIME)"
        kpi_label = "Total Transactions"

    df["txn_amount"] = pd.to_numeric(df["txn_amount"], errors="coerce")
    df = df.dropna(subset=["txn_amount"])

    total_txn = len(df)
    total_amount = round(df["txn_amount"].sum(), 2) if total_txn else 0
    avg_amount = round(df["txn_amount"].mean(), 2) if total_txn else 0

    fig = plt.figure(figsize=(14, 8))
    fig.suptitle(title, fontsize=18, fontweight="bold")

    add_kpi_card(fig, 0.05, 0.84, 0.22, 0.10, kpi_label, total_txn)
    add_kpi_card(fig, 0.29, 0.84, 0.22, 0.10, "Total Amount", total_amount)
    add_kpi_card(fig, 0.53, 0.84, 0.22, 0.10, "Avg Txn Amount", avg_amount)

    ax1 = fig.add_axes([0.05, 0.10, 0.42, 0.68])
    ax2 = fig.add_axes([0.53, 0.10, 0.42, 0.68])

    if total_txn == 0:
        ax1.text(0.5, 0.5, "No data", ha="center", va="center")
        ax1.set_axis_off()
        ax2.set_axis_off()
        return fig

    type_sum = df.groupby("transaction_type")["txn_amount"].sum().sort_values(ascending=False)

    ax1.bar(type_sum.index.astype(str), type_sum.values)
    ax1.set_title("Total Amount by Transaction Type")
    ax1.set_xlabel("Transaction Type")
    ax1.set_ylabel("Total Amount")
    style_axes(ax1)

    ax2.hist(df["txn_amount"], bins=12)
    ax2.set_title("Transaction Amount Distribution")
    ax2.set_xlabel("Txn Amount")
    ax2.set_ylabel("Count")
    style_axes(ax2)

    return fig



