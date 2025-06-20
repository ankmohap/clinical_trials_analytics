import boto3
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
import io
import logging
from botocore.exceptions import ClientError
from utils.logger import get_logger

logger = get_logger(__name__)

class S3DataUploader:
    """Handler for uploading data to S3 in micro-batches"""
    
    def __init__(self, bucket_name: str, aws_region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
    def upload_json_data(self, data: List[Dict], s3_key: str) -> str:
        """Upload JSON data to S3"""
        
        try:
            json_data = json.dumps(data, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully uploaded JSON data to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
            
        except ClientError as e:
            logger.error(f"Error uploading JSON data to S3: {e}")
            raise
    
    def upload_parquet_data(self, data: List[Dict], s3_key: str) -> str:
        """Upload data as Parquet to S3"""
        
        try:
            df = pd.DataFrame(data)
            
            # Convert to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully uploaded Parquet data to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
            
        except Exception as e:
            logger.error(f"Error uploading Parquet data to S3: {e}")
            raise
    
    def upload_csv_data(self, data: List[Dict], s3_key: str) -> str:
        """Upload data as CSV to S3"""
        
        try:
            df = pd.DataFrame(data)
            
            # Convert to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Successfully uploaded CSV data to s3://{self.bucket_name}/{s3_key}")
            return f"s3://{self.bucket_name}/{s3_key}"
            
        except ClientError as e:
            logger.error(f"Error uploading CSV data to S3: {e}")
            raise
    
    def create_micro_batches(self, data: List[Dict], batch_size: int = 1000) -> List[List[Dict]]:
        """Split data into micro-batches"""
        
        batches = []
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"Created {len(batches)} micro-batches of size {batch_size}")
        return batches
    
    def upload_micro_batches(self, 
                           data: List[Dict], 
                           batch_id: str,
                           file_format: str = 'parquet',
                           batch_size: int = 1000) -> List[str]:
        """Upload data in micro-batches"""
        
        if not data:
            logger.warning("No data to upload")
            return []
        
        batches = self.create_micro_batches(data, batch_size)
        uploaded_files = []
        
        for i, batch in enumerate(batches):
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            s3_key = f"raw_data/clinical_trials/{batch_id}/batch_{i+1:04d}_{timestamp}.{file_format}"
            
            try:
                if file_format.lower() == 'json':
                    file_path = self.upload_json_data(batch, s3_key)
                elif file_format.lower() == 'parquet':
                    file_path = self.upload_parquet_data(batch, s3_key)
                elif file_format.lower() == 'csv':
                    file_path = self.upload_csv_data(batch, s3_key)
                else:
                    raise ValueError(f"Unsupported file format: {file_format}")
                
                uploaded_files.append(file_path)
                
            except Exception as e:
                logger.error(f"Error uploading batch {i+1}: {e}")
                raise
        
        logger.info(f"Successfully uploaded {len(uploaded_files)} micro-batches")
        return uploaded_files

def upload_to_s3(data: List[Dict], bucket_name: str, batch_id: str) -> str:
    """
    Main function to upload clinical trials data to S3
    
    Args:
        data: List of dictionaries containing the data
        bucket_name: S3 bucket name
        batch_id: Unique identifier for this batch
    
    Returns:
        S3 key of the uploaded file
    """
    
    logger.info(f"Starting S3 upload for batch {batch_id}")
    
    try:
        uploader = S3DataUploader(bucket_name)
        
        # Upload as micro-batches in Parquet format for better compression and query performance
        uploaded_files = uploader.upload_micro_batches(
            data=data,
            batch_id=batch_id,
            file_format='parquet',
            batch_size=1000
        )
        
        # Also create a manifest file
        manifest_data = {
            'batch_id': batch_id,
            'upload_timestamp': datetime.now().isoformat(),
            'total_records': len(data),
            'files': uploaded_files,
            'file_count': len(uploaded_files)
        }
        
        manifest_key = f"manifests/clinical_trials/{batch_id}/manifest.json"
        manifest_s3_path = uploader.upload_json_data([manifest_data], manifest_key)
        
        logger.info(f"Upload completed. Manifest: {manifest_s3_path}")
        
        # Return the primary data key (first file) for downstream processing
        return uploaded_files[0] if uploaded_files else manifest_s3_path
        
    except Exception as e:
        logger.error(f"Error in S3 upload: {e}")
        raise