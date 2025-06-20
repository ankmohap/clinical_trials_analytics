import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import os
from typing import Dict, Any, Optional
import logging
from utils.logger import get_logger

logger = get_logger(__name__)

class SnowflakeConnection:
    """Snowflake connection manager"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.connection = None
        self.engine = None
    
    def get_connection(self):
        """Get Snowflake connection"""
        
        if not self.connection or self.connection.is_closed():
            try:
                self.connection = snowflake.connector.connect(
                    user=self.config['user'],
                    password=self.config['password'],
                    account=self.config['account'],
                    warehouse=self.config['warehouse'],
                    database=self.config['database'],
                    schema=self.config['schema'],
                    client_session_keep_alive=True
                )
                
                logger.info("Successfully connected to Snowflake")
                
            except Exception as e:
                logger.error(f"Error connecting to Snowflake: {e}")
                raise
        
        return self.connection
    
    def get_engine(self):
        """Get SQLAlchemy engine for Snowflake"""
        
        if not self.engine:
            try:
                engine_url = URL(
                    user=self.config['user'],
                    password=self.config['password'],
                    account=self.config['account'],
                    warehouse=self.config['warehouse'],
                    database=self.config['database'],
                    schema=self.config['schema']
                )
                
                self.engine = create_engine(engine_url)
                logger.info("Successfully created Snowflake SQLAlchemy engine")
                
            except Exception as e:
                logger.error(f"Error creating Snowflake engine: {e}")
                raise
        
        return self.engine
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute query and return results"""
        
        conn = self.get_connection()
        
        try:
            with conn.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                
            logger.info(f"Query executed successfully. Rows returned: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def execute_script(self, script_path: str, params: Optional[Dict] = None) -> bool:
        """Execute SQL script from file"""
        
        try:
            with open(script_path, 'r') as file:
                script_content = file.read()
            
            # Split script into individual statements
            statements = script_content.split(';')
            
            conn = self.get_connection()
            
            with conn.cursor() as cursor:
                for statement in statements:
                    statement = statement.strip()
                    if statement:
                        cursor.execute(statement, params)
            
            logger.info(f"Script {script_path} executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error executing script {script_path}: {e}")
            raise
    
    def close(self):
        """Close connections"""
        
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            logger.info("Snowflake connection closed")
        
        if self.engine:
            self.engine.dispose()
            logger.info("Snowflake engine disposed")

def get_snowflake_config() -> Dict[str, str]:
    """Get Snowflake configuration from environment variables"""
    
    return {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'CLINICAL_TRIALS'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'RAW_DATA')
    }

def test_connection() -> bool:
    """Test Snowflake connection"""
    
    try:
        config = get_snowflake_config()
        conn_manager = SnowflakeConnection(config)
        
        # Test query
        result = conn_manager.execute_query("SELECT CURRENT_VERSION()")
        
        conn_manager.close()
        
        logger.info(f"Connection test successful. Snowflake version: {result[0]['CURRENT_VERSION()']}")
        return True
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False