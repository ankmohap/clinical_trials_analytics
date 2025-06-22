# dags/fetch_and_transform.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, json, sys, logging, boto3
import snowflake.connector

logger = logging.getLogger("airflow")

def get_ssm_param(name):
    ssm = boto3.client("ssm", region_name="us-east-1")
    return ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

def fetch_and_upload():
    AWS_ACCESS_KEY = get_ssm_param('/clinical-pipeline/AWS_ACCESS_KEY')
    AWS_SECRET_KEY = get_ssm_param('/clinical-pipeline/AWS_SECRET_KEY')
    S3_BUCKET = 'clinical-pipeline-data'

    def fetch_ctgov_data(page_token=None):
        params = {
            "query.term": "(COVERAGE[FullMatch]Phase 2 AND COVERAGE[FullMatch]Interventional AND AREA[StudyFirstSubmitDate]RANGE[01/01/2015, MAX]) AND (COVERAGE[FullMatch]Phase 3 AND COVERAGE[FullMatch]Interventional AND AREA[StudyFirstSubmitDate]RANGE[01/01/2015, MAX])",
            "format": "json",
            "pageSize": 500
        }
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get("https://clinicaltrials.gov/api/v2/studies", params=params)
        resp.raise_for_status()
        return resp.json().get("studies", []), resp.json().get("nextPageToken")

    def save_to_s3(data, filename):
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=json.dumps(data))
        logger.info(f"✅ Uploaded to S3: {filename}")

    page_token = None
    total_studies = []
    max_memory = 16 * 1024 * 1024
    chunk_count = 0

    while True:
        studies, next_page_token = fetch_ctgov_data(page_token)
        total_studies.extend(studies)
        if not next_page_token:
            break
        page_token = next_page_token

        if sys.getsizeof(json.dumps(total_studies)) >= max_memory:
            chunk_filename = f"data_{chunk_count}.json"
            chunk_data = total_studies[:len(total_studies) // 2]
            total_studies = total_studies[len(total_studies) // 2:]
            save_to_s3(chunk_data, chunk_filename)
            chunk_count += 1

    if total_studies:
        save_to_s3(total_studies, f"data_{chunk_count}.json")
        logger.info(f"✅ Final upload complete with {len(total_studies)} studies")

def load_into_snowflake():
    SF_USER = get_ssm_param('/clinical-pipeline/SF_USER')
    SF_PASSWORD = get_ssm_param('/clinical-pipeline/SF_PASSWORD')
    SF_ACCOUNT = get_ssm_param('/clinical-pipeline/SF_ACCOUNT')
    SF_ROLE = get_ssm_param('/clinical-pipeline/SF_ROLE')
    SF_WAREHOUSE = get_ssm_param('/clinical-pipeline/SF_WAREHOUSE')
    SNOWFLAKE_DB = get_ssm_param('/clinical-pipeline/SNOWFLAKE_DB')
    SNOWFLAKE_SCHEMA = get_ssm_param('/clinical-pipeline/SNOWFLAKE_SCHEMA')
    SNOWFLAKE_STAGE = get_ssm_param('/clinical-pipeline/SNOWFLAKE_STAGE')
    SNOWFLAKE_TABLE = get_ssm_param('/clinical-pipeline/SNOWFLAKE_TABLE')

    ctx = snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()
    cs.execute(f"COPY INTO {SNOWFLAKE_TABLE}(raw_json) FROM {SNOWFLAKE_STAGE} FILE_FORMAT=(TYPE=JSON STRIP_OUTER_ARRAY=TRUE) ON_ERROR=CONTINUE")
    cs.close()
    ctx.close()

# DAG Definition
with DAG(
    dag_id='clinical_trials_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@weekly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['clinical', 'ctgov']
) as dag:

    fetch_upload = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_and_upload
    )

    snowflake_load = PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_into_snowflake
    )

    fetch_upload >> snowflake_load
