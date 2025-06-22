# Clinical Trials ETL Pipeline (Airflow + DBT + Snowflake on AWS)

## 🚀 Overview
This project orchestrates an end-to-end data pipeline:
- Extract clinical trial data from ClinicalTrials.gov API
- Store as micro-batches in S3
- Load into Snowflake using `COPY INTO`
- Transform and validate using DBT
- All steps are orchestrated by Apache Airflow running in Docker on an EC2 instance

## 🧱 Folder Structure
```
clinical-pipeline/
├── dags/               # Airflow DAGs
├── dbt/                # DBT models and profile
├── docker/             # Docker Compose & Dockerfile
├── terraform/          # Terraform IaC (EC2, VPC, KMS, SSM)
├── plugins/            # Optional Airflow plugins
├── bootstrap.sh        # Provisions infrastructure
├── deploy.sh           # Pushes code to EC2 & starts Airflow
├── .env.template       # Template for secrets
├── .gitignore          # Git ignored files
└── README.md           # This file
```

## ⚙️ Setup & Deployment

### 1. 📦 Provision Infra
```bash
./bootstrap.sh
```
This script sets up:
- VPC + subnets
- EC2 instance for Airflow
- S3 bucket for DAGs/data
- KMS key + SSM Parameter Store for secrets

### 2. 🚀 Deploy Code to EC2
```bash
./deploy.sh
```
Copies your code and starts Airflow via Docker Compose.

### 3. 🔗 Access Airflow
```
http://<EC2_PUBLIC_IP>:8080
```
Use the output from Terraform to get the IP.

## 🗃️ Secrets Management
All secrets (AWS, Snowflake) are stored in AWS SSM with encryption via KMS.
They are fetched inside DAGs using `boto3` and injected as environment variables.

## 📅 DAGs
### `clinical_trials_dag`
- Fetches data from API
- Saves in S3 in chunks
- Loads into Snowflake using COPY INTO

### `run_dbt_pipeline`
- Debug, test and run DBT models using secrets from SSM

## 🧪 Testing
To manually run Airflow in EC2:
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<EC2_PUBLIC_IP>
cd clinical-pipeline
docker-compose up airflow-webserver airflow-scheduler
```

## ✅ Prerequisites
- AWS CLI configured
- SSH key pair
- Terraform installed
- Docker installed locally (optional for testing)

## 📌 Notes
- Edit `.env.template` and rename to `.env` with your real credentials
- Customize `dbt_project.yml`, Snowflake targets, and models
- Consider setting up `dbt docs generate` as a future task

---
Enjoy building reliable, scalable, and reproducible ETL pipelines! 🧪
