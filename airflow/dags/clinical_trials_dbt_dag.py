# dags/run_dbt_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import boto3
import os


def get_ssm_param(name):
    ssm = boto3.client("ssm", region_name="us-east-1")
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

# Set environment variables from SSM before DAG definition
os.environ['SF_ACCOUNT'] = get_ssm_param('/clinical-pipeline/SF_ACCOUNT')
os.environ['SF_USER'] = get_ssm_param('/clinical-pipeline/SF_USER')
os.environ['SF_PASSWORD'] = get_ssm_param('/clinical-pipeline/SF_PASSWORD')
os.environ['SF_ROLE'] = get_ssm_param('/clinical-pipeline/SF_ROLE')
os.environ['SF_WAREHOUSE'] = get_ssm_param('/clinical-pipeline/SF_WAREHOUSE')
os.environ['SNOWFLAKE_DB'] = get_ssm_param('/clinical-pipeline/SNOWFLAKE_DB')
os.environ['SNOWFLAKE_SCHEMA'] = get_ssm_param('/clinical-pipeline/SNOWFLAKE_SCHEMA')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 0
}

with DAG(
    dag_id='Initiallize_validate_dbt_run',
    description='Test and execute DBT transformations',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:

    validate_dbt = BashOperator(
        task_id='Initialize_dbt',
        bash_command='cd /opt/airflow/dbt && dbt deps --profiles-dir .',
    )

    test_dbt = BashOperator(
        task_id='Test_and_validate_dbt',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir .',
    )

    run_dbt = BashOperator(
        task_id='dbt_Transformations_and_Executions',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir .',
    )

    validate_dbt >> test_dbt >> run_dbt
