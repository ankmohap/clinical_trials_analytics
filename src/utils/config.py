import os
import yaml
from typing import Dict, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class Config:
    """Configuration management for the Clinical Trials ETL project"""
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.getenv('ENVIRONMENT', 'dev')
        self.config_data = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file and environment variables"""
        
        # Load base configuration from YAML
        config_file = Path(f"/opt/airflow/config/{self.environment}.yaml")
        
        if not config_file.exists():
            logger.warning(f"Config file {config_file} not found, using defaults")
            return self._get_default_config()
        
        try:
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
            
            # Replace environment variable placeholders
            config = self._substitute_env_vars(config)
            
            logger.info(f"Configuration loaded for environment: {self.environment}")
            return config
            
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return self._get_default_config()
    
    def _substitute_env_vars(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively substitute environment variables in config"""
        
        if isinstance(config, dict):
            return {key: self._substitute_env_vars(value) for key, value in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith('${') and config.endswith('}'):
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        else:
            return config
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        
        return {
            'aws': {
                'region': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                's3_bucket_name': os.getenv('S3_BUCKET_NAME', 'clinical-trials-data')
            },
            'snowflake': {
                'account': os.getenv('SNOWFLAKE_ACCOUNT'),
                'user': os.getenv('SNOWFLAKE_USER'),
                'password': os.getenv('SNOWFLAKE_PASSWORD'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                'database': os.getenv('SNOWFLAKE_DATABASE', 'CLINICAL_TRIALS'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA')
            },
            'clinical_trial_api': {
                'base_url': os.getenv('CLINICAL_TRIAL_BASE_URL', 'https://clinicaltrials.gov/api/query'),
                'api_key': os.getenv('CLINICAL_TRIAL_API_KEY'),
                'rate_limit_per_second': 10,
                'max_studies_per_batch': 1000,
                'days_lookback': 7
            },
            'data_processing': {
                'batch_size': 1000,
                'file_format': 'parquet',
                'compression': 'snappy',
                'max_file_size_mb': 100
            },
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        }
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'aws.region')"""
        
        keys = key_path.split('.')
        value = self.config_data
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_aws_config(self) -> Dict[str, str]:
        """Get AWS configuration"""
        return self.config_data.get('aws', {})
    
    def get_snowflake_config(self) -> Dict[str, str]:
        """Get Snowflake configuration"""
        return self.config_data.get('snowflake', {})
    
    def get_api_config(self) -> Dict[str, Any]:
        """Get Clinical Trial API configuration"""
        return self.config_data.get('clinical_trial_api', {})
    
    def get_processing_config(self) -> Dict[str, Any]:
        """Get data processing configuration"""
        return self.config_data.get('data_processing', {})

# Global config instance
_config_instance = None

def get_config(environment: str = None) -> Dict[str, Any]:
    """Get configuration dictionary"""
    global _config_instance
    
    if _config_instance is None or (environment and environment != _config_instance.environment):
        _config_instance = Config(environment)
    
    return _config_instance.config_data

def get_config_instance(environment: str = None) -> Config:
    """Get configuration instance"""
    global _config_instance
    
    if _config_instance is None or (environment and environment != _config_instance.environment):
        _config_instance = Config(environment)
    
    return _config_instance