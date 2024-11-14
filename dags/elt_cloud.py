from pendulum import datetime
from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# Define the DAG
with DAG(
    "elt_cloud",
    start_date=datetime(2024, 10, 14),
    description="An Airflow DAG to trigger dbt Cloud jobs for ELT",
    schedule_interval="@daily",  # Adjust scheduling as needed
    catchup=False,
    default_args={
        'retries': 3,  # Setting retries here applies to all tasks
    },
) as dag:

    # Run dbt models in dbt Cloud
    dbt_run_job = DbtCloudRunJobOperator(
        task_id="dbt_run_job",
        job_id=70471823397461,       # Replace with the job ID in dbt Cloud
        dbt_cloud_conn_id="dbt_cloud",         # Airflow connection ID configured for dbt Cloud
        timeout=3600,                          # Optional: specify timeout in seconds
        check_interval=60,                     # Optional: check every 60 seconds
    )

    # Test models in dbt Cloud
    dbt_test_job = DbtCloudRunJobOperator(
        task_id="dbt_test_job",
        job_id=70471823397462,   # Replace with a separate test job ID in dbt Cloud, if available
        dbt_cloud_conn_id="dbt_cloud",
        timeout=1800,                          # Optional: shorter timeout for tests
        check_interval=60,
    )

    # Snapshot models in dbt Cloud
    dbt_snapshot_job = DbtCloudRunJobOperator(
        task_id="dbt_snapshot_job",
        job_id=70471823397464, # Replace with the snapshot job ID in dbt Cloud
        dbt_cloud_conn_id="dbt_cloud",
        timeout=3600,
        check_interval=60,
    )

    # Define task dependencies
    dbt_run_job >> dbt_test_job >> dbt_snapshot_job
