output "s3_bucket_name" {
  description = "Name of the S3 bucket for clinical trials data"
  value       = aws_s3_bucket.clinical_trials_data.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket for clinical trials data"
  value       = aws_s3_bucket.clinical_trials_data.arn
}

output "airflow_logs_bucket" {
  description = "Name of the S3 bucket for Airflow logs"
  value       = aws_s3_bucket.airflow_logs.bucket
}

output "iam_role_arn" {
  description = "ARN of the IAM role for Airflow"
  value       = aws_iam_role.airflow_role.arn
}

output "snowflake_user_access_key" {
  description = "Access key for Snowflake user"
  value       = aws_iam_access_key.snowflake_user_key.id
}

output "snowflake_user_secret_key" {
  description = "Secret key for Snowflake user"
  value       = aws_iam_access_key.snowflake_user_key.secret
  sensitive   = true
}

output "aws_region" {
  description = "AWS region"
  value       = local.region
}

output "aws_account_id" {
  description = "AWS account ID"
  value       = local.account_id
}