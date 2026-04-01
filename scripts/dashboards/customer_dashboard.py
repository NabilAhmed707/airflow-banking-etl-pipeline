import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from .style import apply_theme, style_axes, add_kpi_card
from sqlalchemy import text

def build_customer_dashboard(engine, run_date: str, mode: str = "daily"):
    apply_theme()

    if mode == "daily":
        query = text("""
            SELECT age, create_tsp
            FROM customer_target
            WHERE date(create_tsp) = :run_date
              AND age IS NOT NULL
        """)
        df = pd.read_sql_query(query, engine, params={"run_date": run_date})
        title = f"Customer Demographic Dashboard (DAILY - {run_date})"
        kpi_label = "New Customers"
    else:
        query=text("""
            SELECT age, create_tsp
            FROM customer_target
            WHERE age IS NOT NULL
        """)
        df = pd.read_sql_query(query, engine)
        title = "Customer Demographic Dashboard (ALL TIME)"
        kpi_label = "Total Customers"

    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df = df.dropna(subset=["age"])

    total_customers = len(df)
    avg_age = round(df["age"].mean(), 1) if total_customers else 0
    max_age = int(df["age"].max()) if total_customers else 0

    bins = [18, 25, 35, 45, 55, 65, 200]
    labels = ["18-25", "26-35", "36-45", "46-55", "56-65", "65+"]
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, include_lowest=True)

    age_group_counts = df["age_group"].value_counts().reindex(labels).fillna(0)
    age_counts = df["age"].value_counts().sort_index()

    fig = plt.figure(figsize=(14, 8))
    fig.suptitle(title, fontsize=18, fontweight="bold")

    add_kpi_card(fig, 0.05, 0.84, 0.22, 0.10, kpi_label, total_customers)
    add_kpi_card(fig, 0.29, 0.84, 0.22, 0.10, "Average Age", avg_age)
    add_kpi_card(fig, 0.53, 0.84, 0.22, 0.10, "Max Age", max_age)

    ax1 = fig.add_axes([0.05, 0.10, 0.42, 0.68])
    ax2 = fig.add_axes([0.53, 0.10, 0.42, 0.68])

    if total_customers == 0:
        ax1.text(0.5, 0.5, "No data", ha="center", va="center")
        ax1.set_axis_off()
        ax2.set_axis_off()
        return fig

    ax1.bar(age_group_counts.index.astype(str), age_group_counts.values)
    ax1.set_title("Age Group Distribution")
    ax1.set_xlabel("Age Group")
    ax1.set_ylabel("Customers")
    style_axes(ax1)

    ax2.plot(age_counts.index, age_counts.values)
    ax2.set_title("Age Distribution Curve")
    ax2.set_xlabel("Age")
    ax2.set_ylabel("Customers")
    style_axes(ax2)

    return fig











