# IAM role for Airflow EC2 instance
resource "aws_iam_role" "airflow_role" {
  name = "${var.project_name}-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

# IAM policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "${var.project_name}-s3-access"
  description = "Policy for S3 access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.clinical_trials_data.arn,
          "${aws_s3_bucket.clinical_trials_data.arn}/*",
          aws_s3_bucket.airflow_logs.arn,
          "${aws_s3_bucket.airflow_logs.arn}/*"
        ]
      }
    ]
  })
}

# IAM policy for Snowflake integration
resource "aws_iam_policy" "snowflake_integration_policy" {
  name        = "${var.project_name}-snowflake-integration"
  description = "Policy for Snowflake integration"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:ListBucketVersions"
        ]
        Resource = [
          aws_s3_bucket.clinical_trials_data.arn,
          "${aws_s3_bucket.clinical_trials_data.arn}/*"
        ]
      }
    ]
  })
}

# Attach policies to role
resource "aws_iam_role_policy_attachment" "airflow_s3_access" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "aws_iam_role_policy_attachment" "airflow_snowflake_integration" {
  role       = aws_iam_role.airflow_role.name
  policy_arn = aws_iam_policy.snowflake_integration_policy.arn
}

# Instance profile for EC2
resource "aws_iam_instance_profile" "airflow_profile" {
  name = "${var.project_name}-airflow-profile"
  role = aws_iam_role.airflow_role.name
}

# IAM user for Snowflake external stage
resource "aws_iam_user" "snowflake_user" {
  name = "${var.project_name}-snowflake-user"
  tags = local.common_tags
}

resource "aws_iam_user_policy_attachment" "snowflake_user_policy" {
  user       = aws_iam_user.snowflake_user.name
  policy_arn = aws_iam_policy.snowflake_integration_policy.arn
}

resource "aws_iam_access_key" "snowflake_user_key" {
  user = aws_iam_user.snowflake_user.name
}