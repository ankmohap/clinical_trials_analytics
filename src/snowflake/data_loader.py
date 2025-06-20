import pandas as pd
from typing import Dict, Any, List
import logging
from .connection import SnowflakeConnection, get_snowflake_config
from utils.logger import get_logger

logger = get_logger(__name__)

class SnowflakeDataLoader:
    """Data loader for Snowflake operations"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.connection_manager = SnowflakeConnection(config)
    
    def load_data_from_s3(self, s3_path: str, table_name: str, batch_id: str) -> Dict[str, Any]:
        """Load data from S3 into Snowflake staging table"""
        
        logger.info(f"Loading data from {s3_path} to {table_name}")
        
        try:
            conn = self.connection_manager.get_connection()
            
            # Prepare COPY INTO statement
            copy_sql = f"""
            COPY INTO {table_name} (
                NCT_ID, BRIEF_TITLE, OFFICIAL_TITLE, OVERALL_STATUS, STUDY_TYPE,
                PHASE, CONDITION, INTERVENTION_NAME, PRIMARY_OUTCOME_MEASURE,
                STUDY_FIRST_SUBMIT_DATE, LAST_UPDATE_SUBMIT_DATE, COMPLETION_DATE,
                LOCATION_CITY, LOCATION_STATE, LOCATION_COUNTRY, SPONSOR_NAME,
                EXTRACTION_TIMESTAMP, DATA_SOURCE, BATCH_ID
            )
            FROM (
                SELECT 
                    $1:nct_id::VARCHAR(50),
                    $1:brief_title::VARCHAR(1000),
                    $1:official_title::VARCHAR(2000),
                    $1:overall_status::VARCHAR(100),
                    $1:study_type::VARCHAR(100),
                    $1:phase::VARCHAR(100),
                    $1:condition::VARCHAR(2000),
                    $1:intervention_name::VARCHAR(2000),
                    $1:primary_outcome_measure::VARCHAR(5000),
                    TO_DATE($1:study_first_submit_date::VARCHAR),
                    TO_DATE($1:last_update_submit_date::VARCHAR),
                    TO_DATE($1:completion_date::VARCHAR),
                    $1:location_city::VARCHAR(200),
                    $1:location_state::VARCHAR(100),
                    $1:location_country::VARCHAR(100),
                    $1:sponsor_name::VARCHAR(500),
                    TO_TIMESTAMP($1:extraction_timestamp::VARCHAR),
                    $1:data_source::VARCHAR(100),
                    '{batch_id}'
                FROM @CLINICAL_TRIALS_STAGE
                WHERE METADATA$FILENAME LIKE '%{batch_id}%'
            )
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
            """
            
            with conn.cursor() as cursor:
                cursor.execute(copy_sql)
                rows_loaded = cursor.rowcount
            
            logger.info(f"Successfully loaded {rows_loaded} rows")
            
            return {
                'status': 'success',
                'rows_loaded': rows_loaded,
                'batch_id': batch_id,
                'table_name': table_name
            }
            
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {e}")
            raise
    
    def run_dbt_models(self, models: List[str] = None) -> Dict[str, Any]:
        """Run DBT models"""
        
        import subprocess
        import os
        
        logger.info("Running DBT models")
        
        try:
            # Change to DBT directory
            dbt_dir = "/opt/airflow/dbt"
            os.chdir(dbt_dir)
            
            # Build command
            if models:
                cmd = ["dbt", "run", "--models"] + models
            else:
                cmd = ["dbt", "run"]
            
            # Run DBT
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("DBT models ran successfully")
                return {
                    'status': 'success',
                    'output': result.stdout,
                    'models_run': models or 'all'
                }
            else:
                logger.error(f"DBT run failed: {result.stderr}")
                raise Exception(f"DBT run failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error running DBT models: {e}")
            raise
    
    def run_dbt_tests(self) -> Dict[str, Any]:
        """Run DBT tests"""
        
        import subprocess
        import os
        
        logger.info("Running DBT tests")
        
        try:
            dbt_dir = "/opt/airflow/dbt"
            os.chdir(dbt_dir)
            
            result = subprocess.run(["dbt", "test"], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("DBT tests passed")
                return {
                    'status': 'success',
                    'output': result.stdout
                }
            else:
                logger.warning(f"Some DBT tests failed: {result.stderr}")
                return {
                    'status': 'warning',
                    'output': result.stdout,
                    'errors': result.stderr
                }
                
        except Exception as e:
            logger.error(f"Error running DBT tests: {e}")
            raise

def load_data_to_snowflake(s3_key: str, snowflake_config: Dict[str, str]) -> Dict[str, Any]:
    """
    Main function to load data to Snowflake and run DBT transformations
    
    Args:
        s3_key: S3 key of the data file
        snowflake_config: Snowflake connection configuration
    
    Returns:
        Dictionary containing the load results
    """
    
    logger.info(f"Starting Snowflake data load for {s3_key}")
    
    try:
        loader = SnowflakeDataLoader(snowflake_config)
        
        # Extract batch ID from S3 key
        batch_id = s3_key.split('/')[-2] if '/' in s3_key else 'unknown'
        
        # Load raw data
        load_result = loader.load_data_from_s3(
            s3_path=s3_key,
            table_name='STAGING_CLINICAL_TRIALS',
            batch_id=batch_id
        )
        
        # Run DBT transformations
        dbt_result = loader.run_dbt_models()
        
        # Run DBT tests
        test_result = loader.run_dbt_tests()
        
        return {
            'load_result': load_result,
            'dbt_result': dbt_result,
            'test_result': test_result,
            'overall_status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error in Snowflake data load: {e}")
        raise