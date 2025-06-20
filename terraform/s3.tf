# S3 bucket for clinical trials data
resource "aws_s3_bucket" "clinical_trials_data" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_bucket_versioning" "clinical_trials_data_versioning" {
  bucket = aws_s3_bucket.clinical_trials_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "clinical_trials_data_encryption" {
  bucket = aws_s3_bucket.clinical_trials_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "clinical_trials_data_lifecycle" {
  bucket = aws_s3_bucket.clinical_trials_data.id

  rule {
    id     = "clinical_trials_lifecycle"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    transition {
      days          = 365
      storage_class = "DEEP_ARCHIVE"
    }
  }
}

# S3 bucket for Airflow logs
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${var.s3_bucket_name}-airflow-logs"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "airflow_logs_encryption" {
  bucket = aws_s3_bucket.airflow_logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}