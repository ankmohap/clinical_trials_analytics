import pandas as pd
import logging
from typing import Dict, List, Any
from snowflake.connection import SnowflakeConnection, get_snowflake_config
from utils.logger import get_logger

logger = get_logger(__name__)

class DataQualityChecker:
    """Data quality validation for clinical trials data"""
    
    def __init__(self, snowflake_config: Dict[str, str]):
        self.connection_manager = SnowflakeConnection(snowflake_config)
    
    def check_record_counts(self) -> Dict[str, Any]:
        """Check record counts across tables"""
        
        queries = {
            'staging_records': "SELECT COUNT(*) as count FROM RAW_DATA.STAGING_CLINICAL_TRIALS",
            'staging_clean_records': "SELECT COUNT(*) as count FROM STAGING.STG_CLINICAL_TRIALS",
            'mart_trials': "SELECT COUNT(*) as count FROM MARTS.DIM_TRIALS",
            'mart_metrics': "SELECT COUNT(*) as count FROM MARTS.FACT_TRIAL_METRICS"
        }
        
        results = {}
        
        for check_name, query in queries.items():
            try:
                result = self.connection_manager.execute_query(query)
                count = result[0]['COUNT'] if result else 0
                results[check_name] = {
                    'count': count,
                    'status': 'pass' if count > 0 else 'fail'
                }
                logger.info(f"{check_name}: {count} records")
            except Exception as e:
                logger.error(f"Error in {check_name}: {e}")
                results[check_name] = {
                    'count': 0,
                    'status': 'error',
                    'error': str(e)
                }
        
        return results
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness"""
        
        query = """
        SELECT 
            MAX(extraction_date) as latest_extraction,
            DATEDIFF('day', MAX(extraction_date), CURRENT_DATE()) as days_old
        FROM STAGING.STG_CLINICAL_TRIALS
        """
        
        try:
            result = self.connection_manager.execute_query(query)
            if result:
                days_old = result[0]['DAYS_OLD']
                status = 'pass' if days_old <= 7 else 'fail'
                return {
                    'latest_extraction': result[0]['LATEST_EXTRACTION'],
                    'days_old': days_old,
                    'status': status
                }
            else:
                return {'status': 'error', 'error': 'No data found'}
                
        except Exception as e:
            logger.error(f"Error checking data freshness: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def check_data_completeness(self) -> Dict[str, Any]:
        """Check data completeness"""
        
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN nct_id IS NULL THEN 1 END) as null_nct_ids,
            COUNT(CASE WHEN brief_title IS NULL OR brief_title = '' THEN 1 END) as null_titles,
            COUNT(CASE WHEN overall_status IS NULL THEN 1 END) as null_status
        FROM STAGING.STG_CLINICAL_TRIALS
        """
        
        try:
            result = self.connection_manager.execute_query(query)
            if result:
                data = result[0]
                total = data['TOTAL_RECORDS']
                
                completeness_checks = {
                    'nct_id_completeness': 1 - (data['NULL_NCT_IDS'] / total) if total > 0 else 0,
                    'title_completeness': 1 - (data['NULL_TITLES'] / total) if total > 0 else 0,
                    'status_completeness': 1 - (data['NULL_STATUS'] / total) if total > 0 else 0
                }
                
                overall_status = 'pass' if all(score >= 0.95 for score in completeness_checks.values()) else 'fail'
                
                return {
                    'total_records': total,
                    'completeness_scores': completeness_checks,
                    'status': overall_status
                }
            else:
                return {'status': 'error', 'error': 'No data found'}
                
        except Exception as e:
            logger.error(f"Error checking data completeness: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all data quality checks"""
        
        logger.info("Starting comprehensive data quality checks")
        
        results = {
            'record_counts': self.check_record_counts(),
            'data_freshness': self.check_data_freshness(),
            'data_completeness': self.check_data_completeness(),
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Determine overall status
        all_statuses = []
        for check_category, check_results in results.items():
            if isinstance(check_results, dict) and 'status' in check_results:
                all_statuses.append(check_results['status'])
            elif isinstance(check_results, dict):
                for sub_check, sub_result in check_results.items():
                    if isinstance(sub_result, dict) and 'status' in sub_result:
                        all_statuses.append(sub_result['status'])
        
        if 'error' in all_statuses:
            overall_status = 'error'
        elif 'fail' in all_statuses:
            overall_status = 'fail'
        else:
            overall_status = 'pass'
        
        results['overall_status'] = overall_status
        
        logger.info(f"Data quality checks completed with status: {overall_status}")
        return results

def main():
    """Main function to run data quality checks"""
    
    try:
        config = get_snowflake_config()
        checker = DataQualityChecker(config)
        results = checker.run_all_checks()
        
        # Print results
        print("=" * 50)
        print("DATA QUALITY CHECK RESULTS")
        print("=" * 50)
        
        for category, result in results.items():
            if category != 'timestamp':
                print(f"\n{category.upper()}:")
                if isinstance(result, dict):
                    for key, value in result.items():
                        print(f"  {key}: {value}")
                else:
                    print(f"  {result}")
        
        print(f"\nOverall Status: {results['overall_status']}")
        print(f"Timestamp: {results['timestamp']}")
        
        # Exit with appropriate code
        if results['overall_status'] == 'error':
            exit(1)
        elif results['overall_status'] == 'fail':
            exit(2)
        else:
            exit(0)
            
    except Exception as e:
        logger.error(f"Error running data quality checks: {e}")
        exit(1)

if __name__ == "__main__":
    main()