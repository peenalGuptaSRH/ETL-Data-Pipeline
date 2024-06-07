import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.datafusion import (
    CloudDataFusionStartPipelineOperator,
    PipelineStates
)
from airflow.providers.google.cloud.sensors.datafusion import CloudDataFusionPipelineStateSensor
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta, datetime

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'datapipeline-ETL',
    default_args=default_args,
    description='ETL/ELT Datapipeline',
    schedule_interval='*/15 * * * *',  # Run once a day
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),  # Increased timeout to 60 minutes
)

def set_task_success(**kwargs):
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='status', value='success')


with dag:
    # Task to extract data
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract-main.py',
    )

    # Task to start the Data Fusion pipeline
    start_pipeline = CloudDataFusionStartPipelineOperator(
        task_id='start_pipeline',
        location='europe-west1',
        project_id='datapipeline-etl',
        pipeline_name='etl-pipeline',
        instance_name='data-fusion-instance',
        asynchronous=True,
        success_states=[PipelineStates.RUNNING],
    )

    # Task to monitor the state of the Data Fusion pipeline
    # start_pipeline_sensor = CloudDataFusionPipelineStateSensor(
    #     task_id='pipeline_state_sensor',
    #     project_id='datapipeline-etl',
    #     location='europe-west1',
    #     instance_name='data-fusion-instance',
    #     pipeline_name='etl-pipeline',
    #     pipeline_id='etl-pipeline',
    #     expected_statuses=['STARTING'],
    #     failure_statuses=['FAILED', 'KILLED', 'STOPPED'],
    #     timeout=timedelta(minutes=30),
    #     poke_interval=60,  # Check every 60 seconds
    #     mode='poke'
    # )
    
    
    # Task to send an email notification
    # send_email = EmailOperator(
    #     task_id='send_email',
    #     to='peenal.gupta@srh-heidelberg.org',
    #     subject='Airflow Alert: Data Fusion Pipeline Status',
    #     html_content='The Data Fusion pipeline has completed successfully.',
    # )
     # Task to mark the sensor task as success after 8 minutes

    # Define task dependencies
    # Define task dependencies
    run_script_task >> start_pipeline
    # start_pipeline_sensor  
    #>> send_email
