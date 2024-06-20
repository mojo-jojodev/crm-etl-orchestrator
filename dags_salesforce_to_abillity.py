from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ajay',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'salesforce_to_abillity_etl',
    default_args=default_args,
    description='Daily Go-Live reconciliation pipeline',
    schedule_interval='0 2 * * *', # 2 AM Daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_sf = BashOperator(
        task_id='extract_salesforce_reports',
        bash_command='python /opt/etl/extract_sf.py --target s3://raw-crm/'
    )

    reconcile = BashOperator(
        task_id='execute_pandas_reconciliation',
        bash_command='python /opt/etl/reconciler.py --input s3://raw-crm/ --output s3://processed-billing/'
    )

    load_abillity = BashOperator(
        task_id='load_abillity_api',
        bash_command='python /opt/etl/abillity_loader.py --source s3://processed-billing/'
    )

    extract_sf >> reconcile >> load_abillity
