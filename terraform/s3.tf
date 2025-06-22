resource "aws_s3_bucket" "airflow_dags" {
  bucket        = "${var.project_name}-dags"
  force_destroy = true
}