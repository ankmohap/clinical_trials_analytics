variable "aws_region" {
  default = "us-east-1"
}

variable "project_name" {
  default = "clinical-pipeline"
}

variable "ec2_ami" {
  description = "Amazon Linux 2 AMI ID"
}

variable "ec2_instance_type" {
  default = "t3.medium"
}

variable "key_name" {
  description = "EC2 SSH Key Name"
}

variable "secret_values" {
  type = map(string)
  default = {
    AWS_ACCESS_KEY      = "your-access-key"
    AWS_SECRET_KEY      = "your-secret-key"
    SF_USER             = "snowflake-user"
    SF_PASSWORD         = "snowflake-pass"
    SF_ACCOUNT          = "snowflake-account"
    SF_ROLE             = "ACCOUNTADMIN"
    SF_WAREHOUSE        = "COMPUTE_WH"
    SNOWFLAKE_DB        = "CLINICAL_DB"
    SNOWFLAKE_SCHEMA    = "PUBLIC"
    SNOWFLAKE_STAGE     = "@raw_stage"
    SNOWFLAKE_TABLE     = "raw_trials"
  }
}