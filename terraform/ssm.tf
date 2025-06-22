resource "aws_ssm_parameter" "airflow_secrets" {
  for_each = var.secret_values

  name  = "/clinical-pipeline/${each.key}"
  type  = "SecureString"
  value = each.value
  key_id = aws_kms_key.airflow_kms.id
}