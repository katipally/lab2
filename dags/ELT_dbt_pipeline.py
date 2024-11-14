from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

DBT_PROJECT_DIR = "/opt/airflow/dbt/lab2_dbt/"

conn = BaseHook.get_connection('snowflake_default')

with DAG(
    "ELT_dbt_pipeline",
    start_date=datetime(2024, 10, 14),
    description="An Airflow DAG to execute dbt commands for ELT",
    schedule_interval="@daily",  
    catchup=False,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_SCHEMA": conn.schema,
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:

    dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt/lab2_dbt/ && dbt run --select input_stock_data moving_average_7d rsi_7d',
    dag=dag
)

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/opt/airflow/dbt/lab2_dbt/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --models moving_average_7d rsi_7d",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/opt/airflow/dbt/lab2_dbt/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR} --select moving_average_7d_snapshot rsi_7d_snapshot",
    )

    dbt_run >> dbt_test >> dbt_snapshot
