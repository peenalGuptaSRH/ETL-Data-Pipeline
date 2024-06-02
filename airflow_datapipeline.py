"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionCreatePipelineOperator,CloudDataFusionStartPipelineOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'airflow_datapipeline',
    default_args=default_args, 
    description='ETL/ELT Datapipeline',
    schedule_interval='*/10 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )
    start_pipeline = CloudDataFusionStartPipelineOperator(
        location='us-west1',
        pipeline_name='etl_datapipeline',
        instance_name='datapipeline-fusion',
        task_id="start_pipeline",
    )

    run_script_task >> start_pipeline

