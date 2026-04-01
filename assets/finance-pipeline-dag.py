from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 2),  # use today’s date
    "retries": 1,
}

with DAG(
    dag_id="finance_data_pipeline",
    default_args=default_args,
    schedule="@daily",     # runs once per day
    catchup=False,         # VERY IMPORTANT
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python3 /opt/airflow/scripts/data_generation.py {{ ds }}"
    )

    run_ingestion = BashOperator(
        task_id="run_bank_ingestion",
        bash_command="python3 /opt/airflow/scripts/bank_detail_script.py {{ ds }}"
    )

    customer_etl = BashOperator(
        task_id="customer_etl",
        bash_command="python3 /opt/airflow/scripts/customer_etl.py {{ ds }}"
    )

    account_etl = BashOperator(
        task_id="account_etl",
        bash_command="python3 /opt/airflow/scripts/account_etl.py {{ ds }}"
    )

    loan_etl = BashOperator(
        task_id="loan_etl",
        bash_command="python3 /opt/airflow/scripts/loan_etl.py {{ ds }}"
    )

    transaction_etl = BashOperator(
        task_id="transaction_etl",
        bash_command="python3 /opt/airflow/scripts/transaction_etl.py {{ ds }}"
    )
    generate_dashboards = BashOperator(
    task_id="generate_dashboards",
    bash_command="python3 /opt/airflow/scripts/generate_dashboards_pdf.py {{ ds }}"
    )


    generate_data >> run_ingestion >> customer_etl >> account_etl >> loan_etl >> transaction_etl >> generate_dashboards
   