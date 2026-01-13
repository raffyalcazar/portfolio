from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import ETL functions
from etl.bank_marketing_etl import (
    extract_bank_marketing,
    transform_bank_marketing,
    validate_df,
    load_dataframes_to_postgres
)


def run_bank_marketing_etl():
    """
    Orchestrates the Bank Marketing ETL process:
    Extract -> Transform -> Validate -> Load
    """
    raw_df = extract_bank_marketing()

    client_df, campaign_df, economics_df = transform_bank_marketing(raw_df)

    validate_df(client_df, campaign_df, economics_df)

    load_dataframes_to_postgres(
        client_df,
        campaign_df,
        economics_df
    )


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

# Define DAG
dag = DAG(
    dag_id="bank_marketing_etl",
    default_args=default_args,
    schedule_interval=None,   # Manual trigger (one-time load)
    catchup=False,
    description="One-time ETL for Bank Marketing data into PostgreSQL"
)

# Define task
run_etl_task = PythonOperator(
    task_id="run_bank_marketing_etl",
    python_callable=run_bank_marketing_etl,
    dag=dag
)
