resource "aws_kms_key" "airflow_kms" {
  description         = "KMS key for encrypting SSM secrets"
  enable_key_rotation = true
}