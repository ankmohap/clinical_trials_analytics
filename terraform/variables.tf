variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "clinical-trials-etl"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data storage"
  type        = string
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.medium"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "allowed_ip_ranges" {
  description = "IP ranges allowed to access Airflow"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
  sensitive   = true
}

variable "notification_email" {
  description = "Email for notifications"
  type        = string
}