import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .style import apply_theme, style_axes, add_kpi_card
from sqlalchemy import text
def build_account_dashboard(engine, run_date: str, mode: str = "daily"):
    apply_theme()

    if mode == "daily":
        query = text("""
            SELECT account_type, branch_id, create_tsp
            FROM account_target
            WHERE DATE(create_tsp) = :run_date
        """)
        df = pd.read_sql_query(
            query,
            engine,
            params={"run_date": run_date}
        )
        title = f"Account Performance Dashboard (DAILY - {run_date})"
        kpi_label = "New Accounts"
    else:
        query=text("""
        SELECT account_type, branch_id, create_tsp
            FROM account_target
        """
        )
        df = pd.read_sql_query(
            query,
            engine)
        title = "Account Performance Dashboard (ALL TIME)"
        kpi_label = "Total Accounts"

    total_accounts = len(df)
    total_branches = df["branch_id"].nunique() if total_accounts else 0
    top_branch = df["branch_id"].value_counts().index[0] if total_accounts else "N/A"

    account_type_counts = df["account_type"].value_counts()
    branch_counts = df["branch_id"].value_counts().head(10)

    fig = plt.figure(figsize=(14, 8))
    fig.suptitle(title, fontsize=18, fontweight="bold")

    add_kpi_card(fig, 0.05, 0.84, 0.22, 0.10, kpi_label, total_accounts)
    add_kpi_card(fig, 0.29, 0.84, 0.22, 0.10, "Branches Active", total_branches)
    add_kpi_card(fig, 0.53, 0.84, 0.22, 0.10, "Top Branch", top_branch)

    ax1 = fig.add_axes([0.05, 0.10, 0.42, 0.68])
    ax2 = fig.add_axes([0.53, 0.10, 0.42, 0.68])

    if total_accounts == 0:
        ax1.text(0.5, 0.5, "No data", ha="center", va="center")
        ax1.set_axis_off()
        ax2.set_axis_off()
        return fig

    ax1.bar(account_type_counts.index.astype(str), account_type_counts.values)
    ax1.set_title("Account Type Distribution")
    ax1.set_xlabel("Account Type")
    ax1.set_ylabel("Accounts")
    style_axes(ax1)

    ax2.bar(branch_counts.index.astype(str), branch_counts.values)
    ax2.set_title("Top 10 Branches by Accounts")
    ax2.set_xlabel("Branch ID")
    ax2.set_ylabel("Accounts")
    ax2.tick_params(axis="x", rotation=20)
    style_axes(ax2)

    return fig













