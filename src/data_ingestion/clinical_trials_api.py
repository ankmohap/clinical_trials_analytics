import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
from utils.logger import get_logger

logger = get_logger(__name__)

class ClinicalTrialsAPIClient:
    """Client for Clinical Trials API"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ClinicalTrialsETL/1.0',
            'Accept': 'application/json'
        })
    
    def get_studies(self, 
                   query_params: Dict[str, Any] = None,
                   max_studies: int = 1000) -> List[Dict]:
        """Fetch clinical trial studies from API"""
        
        studies = []
        page_size = 100
        start_index = 0
        
        default_params = {
            'fmt': 'json',
            'min_rnk': 1,
            'max_rnk': page_size,
            'fields': 'NCTId,BriefTitle,OfficialTitle,OverallStatus,StudyType,'
                     'Phase,Condition,InterventionName,PrimaryOutcomeMeasure,'
                     'StudyFirstSubmitDate,LastUpdateSubmitDate,CompletionDate,'
                     'LocationCity,LocationState,LocationCountry,SponsorName'
        }
        
        if query_params:
            default_params.update(query_params)
        
        try:
            while len(studies) < max_studies:
                params = default_params.copy()
                params['min_rnk'] = start_index + 1
                params['max_rnk'] = min(start_index + page_size, max_studies)
                
                logger.info(f"Fetching studies {params['min_rnk']} to {params['max_rnk']}")
                
                response = self.session.get(f"{self.base_url}/study_fields", params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'StudyFieldsResponse' not in data:
                    logger.warning("No StudyFieldsResponse in API response")
                    break
                
                study_fields = data['StudyFieldsResponse'].get('StudyFields', [])
                
                if not study_fields:
                    logger.info("No more studies found")
                    break
                
                studies.extend(study_fields)
                start_index += page_size
                
                # Rate limiting
                import time
                time.sleep(0.1)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching clinical trials data: {e}")
            raise
        
        logger.info(f"Successfully fetched {len(studies)} studies")
        return studies
    
    def get_recent_studies(self, days_back: int = 7) -> List[Dict]:
        """Get studies updated in the last N days"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        query_params = {
            'lup_s': start_date.strftime('%m/%d/%Y'),
            'lup_e': end_date.strftime('%m/%d/%Y')
        }
        
        return self.get_studies(query_params=query_params)

def ingest_clinical_trials_data(api_key: str, base_url: str, batch_id: str) -> Dict[str, Any]:
    """
    Main function to ingest clinical trials data
    
    Args:
        api_key: API key for clinical trials API
        base_url: Base URL for the API
        batch_id: Unique identifier for this batch
    
    Returns:
        Dictionary containing the extracted data and metadata
    """
    
    logger.info(f"Starting clinical trials data ingestion for batch {batch_id}")
    
    try:
        # Initialize API client
        client = ClinicalTrialsAPIClient(api_key, base_url)
        
        # Get recent studies (last 7 days)
        studies = client.get_recent_studies(days_back=7)
        
        if not studies:
            logger.warning("No studies found for the specified criteria")
            return {
                'batch_id': batch_id,
                'data': [],
                'metadata': {
                    'extraction_timestamp': datetime.now().isoformat(),
                    'record_count': 0,
                    'status': 'no_data'
                }
            }
        
        # Convert to DataFrame for easier processing
        df = pd.json_normalize(studies)
        
        # Data cleaning and transformation
        df = clean_clinical_trials_data(df)
        
        # Convert back to list of dictionaries
        cleaned_data = df.to_dict('records')
        
        result = {
            'batch_id': batch_id,
            'data': cleaned_data,
            'metadata': {
                'extraction_timestamp': datetime.now().isoformat(),
                'record_count': len(cleaned_data),
                'status': 'success',
                'columns': list(df.columns)
            }
        }
        
        logger.info(f"Successfully extracted {len(cleaned_data)} clinical trial records")
        return result
        
    except Exception as e:
        logger.error(f"Error in clinical trials data ingestion: {e}")
        raise

def clean_clinical_trials_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform clinical trials data"""
    
    logger.info("Starting data cleaning and transformation")
    
    # Handle missing values
    df = df.fillna('')
    
    # Standardize column names
    column_mapping = {
        'NCTId': 'nct_id',
        'BriefTitle': 'brief_title',
        'OfficialTitle': 'official_title',
        'OverallStatus': 'overall_status',
        'StudyType': 'study_type',
        'Phase': 'phase',
        'Condition': 'condition',
        'InterventionName': 'intervention_name',
        'PrimaryOutcomeMeasure': 'primary_outcome_measure',
        'StudyFirstSubmitDate': 'study_first_submit_date',
        'LastUpdateSubmitDate': 'last_update_submit_date',
        'CompletionDate': 'completion_date',
        'LocationCity': 'location_city',
        'LocationState': 'location_state',
        'LocationCountry': 'location_country',
        'SponsorName': 'sponsor_name'
    }
    
    # Rename columns if they exist
    existing_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=existing_columns)
    
    # Convert date columns
    date_columns = ['study_first_submit_date', 'last_update_submit_date', 'completion_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Add processing metadata
    df['extraction_timestamp'] = datetime.now()
    df['data_source'] = 'clinical_trials_api'
    
    logger.info(f"Data cleaning completed. Final shape: {df.shape}")
    return df