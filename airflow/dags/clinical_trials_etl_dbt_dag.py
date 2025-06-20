from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
import sys
import os

# Add src directory to Python path
sys.path.append('/opt/airflow/src')

from data_ingestion.clinical_trials_api import ingest_clinical_trials_data
from data_ingestion.s3_uploader import upload_to_s3
from snowflake.data_loader import load_data_to_snowflake
from utils.config import get_config

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': [os.getenv('NOTIFICATION_EMAIL', 'admin@company.com')]
}

# Create DAG
dag = DAG(
    'clinical_trials_etl_dbt',
    default_args=default_args,
    description='Clinical Trials ETL Pipeline with DBT',
    schedule_interval='0 7 * * 1',  # Every Monday at 7 AM
    catchup=False,
    max_active_runs=1,
    tags=['clinical-trials', 'etl', 'aws', 'snowflake', 'dbt']
)

def extract_clinical_trials_data(**context):
    """Extract data from Clinical Trials API"""
    execution_date = context['execution_date']
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
    
    config = get_config()
    data = ingest_clinical_trials_data(
        api_key=config['clinical_trial_api']['api_key'],
        base_url=config['clinical_trial_api']['base_url'],
        batch_id=batch_id
    )
    
    return data

def upload_data_to_s3(**context):
    """Upload extracted data to S3"""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='extract_data')
    execution_date = context['execution_date']
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
    
    config = get_config()
    s3_key = upload_to_s3(
        data=data['data'],
        bucket_name=config['aws']['s3_bucket_name'],
        batch_id=batch_id
    )
    
    return s3_key

def load_to_snowflake(**context):
    """Load data from S3 to Snowflake"""
    ti = context['ti']
    s3_key = ti.xcom_pull(task_ids='upload_to_s3')
    
    config = get_config()
    result = load_data_to_snowflake(
        s3_key=s3_key,
        snowflake_config=config['snowflake']
    )
    
    return result

def run_dbt_staging(**context):
    """Run DBT staging models"""
    return BashOperator(
        task_id='run_dbt_staging_internal',
        bash_command='cd /opt/airflow/dbt && dbt run --models staging',
        dag=dag
    ).execute(context)

def run_dbt_marts(**context):
    """Run DBT mart models"""
    return BashOperator(
        task_id='run_dbt_marts_internal',
        bash_command='cd /opt/airflow/dbt && dbt run --models marts',
        dag=dag
    ).execute(context)

def run_dbt_tests(**context):
    """Run DBT tests"""
    return BashOperator(
        task_id='run_dbt_tests_internal',
        bash_command='cd /opt/airflow/dbt && dbt test',
        dag=dag
    ).execute(context)

# Task 1: Extract data from Clinical Trials API
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_clinical_trials_data,
    dag=dag
)

# Task 2: Upload data to S3
upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_data_to_s3,
    dag=dag
)

# Task 3: Wait for S3 file to be available
s3_sensor = S3KeySensor(
    task_id='wait_for_s3_file',
    bucket_name="{{ var.value.s3_bucket_name }}",
    bucket_key="{{ ti.xcom_pull(task_ids='upload_to_s3') }}",
    aws_conn_id='aws_default',
    timeout=300,
    poke_interval=30,
    dag=dag
)

# Task 4: Create staging tables in Snowflake (legacy SQL)
create_staging_tables = SnowflakeOperator(
    task_id='create_staging_tables',
    snowflake_conn_id='snowflake_default',
    sql='sql/staging/create_staging_tables.sql',
    dag=dag
)

# Task 5: Load data to Snowflake
load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

# Task 6: Install DBT dependencies
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd /opt/airflow/dbt && dbt deps',
    dag=dag
)

# Task 7: Run DBT staging models
dbt_staging = PythonOperator(
    task_id='run_dbt_staging',
    python_callable=run_dbt_staging,
    dag=dag
)

# Task 8: Run DBT mart models
dbt_marts = PythonOperator(
    task_id='run_dbt_marts',
    python_callable=run_dbt_marts,
    dag=dag
)

# Task 9: Run DBT tests
dbt_tests = PythonOperator(
    task_id='run_dbt_tests',
    python_callable=run_dbt_tests,
    dag=dag
)

# Task 10: Generate DBT documentation
dbt_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='cd /opt/airflow/dbt && dbt docs generate',
    dag=dag
)

# Task 11: Data quality checks (legacy)
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/src/utils/data_quality_checks.py',
    dag=dag
)

# Task 12: Success notification
success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "Clinical Trials ETL pipeline with DBT completed successfully"',
    dag=dag
)

# Define task dependencies
extract_task >> upload_task >> s3_sensor >> create_staging_tables
create_staging_tables >> load_task >> dbt_deps
dbt_deps >> dbt_staging >> dbt_marts >> dbt_tests
dbt_tests >> dbt_docs >> data_quality_check >> success_notification