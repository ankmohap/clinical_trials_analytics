# Clinical Trials ETL Pipeline with DBT

An AWS-ready data engineering project that consumes clinical trial data, processes it in micro-batches, stores in S3, loads to Snowflake, and uses DBT for staging and data mart transformations.

## Architecture Overview

```
Clinical Trial API → Python Ingestion → S3 Bucket → Snowflake → DBT Transformations → Data Mart
                                          ↑                           ↑
                                    Airflow Orchestration    DBT Models & Tests
                                          ↑
                                   Terraform Infrastructure
```

## Project Structure

```
clinical-trials-etl/
├── terraform/              # Infrastructure as Code
├── airflow/                # Orchestration
├── src/                    # Source code
├── dbt/                    # DBT models and transformations
├── sql/                    # Legacy database scripts (maintained)
├── config/                 # Configuration files
├── docker/                 # Docker configurations
└── docs/                   # Documentation
```

## Quick Start

1. **Setup Infrastructure**:
   ```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
   ```

2. **Setup DBT**:
   ```bash
   cd dbt
   dbt deps
   dbt debug
   dbt run
   dbt test
   ```

3. **Start Airflow**:
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI**: http://localhost:8080

## Features

- **Data Ingestion**: Consumes clinical trial API data
- **Micro-batching**: Processes data in small batches
- **S3 Storage**: Raw and processed data storage
- **Snowflake Integration**: Data warehouse loading
- **DBT Transformations**: Modern data modeling and transformations
- **Data Quality**: DBT tests and data validation
- **Airflow Orchestration**: Scheduled every Monday at 7 AM
- **Terraform**: Complete AWS infrastructure setup
- **Monitoring**: Comprehensive logging and alerting

## DBT Features

- **Staging Models**: Clean and standardize raw data
- **Data Mart Models**: Business-ready aggregated tables
- **Data Tests**: Automated data quality checks
- **Documentation**: Auto-generated data lineage and docs
- **Incremental Models**: Efficient processing of large datasets

## Environment Variables

Copy `.env.example` to `.env` and configure:
- AWS credentials
- Snowflake connection details
- Clinical trial API keys
- Airflow settings
- DBT profiles