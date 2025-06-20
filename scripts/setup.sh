#!/bin/bash

# Clinical Trials ETL Setup Script

set -e

echo "🚀 Setting up Clinical Trials ETL Pipeline with DBT..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please update the .env file with your actual credentials before proceeding."
    echo "   Edit .env file and then run this script again."
    exit 0
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs
mkdir -p dbt/target
mkdir -p dbt/dbt_packages

# Set Airflow UID
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=${AIRFLOW_UID}" >> .env

# Initialize Airflow
echo "🔧 Initializing Airflow..."
docker-compose up airflow-init

# Start services
echo "🚀 Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Install DBT dependencies
echo "📦 Installing DBT dependencies..."
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt deps"

# Test DBT connection
echo "🔍 Testing DBT connection..."
docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug"

echo "✅ Setup complete!"
echo ""
echo "🌐 Access points:"
echo "   Airflow UI: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo ""
echo "📚 Next steps:"
echo "   1. Configure your Snowflake connection in Airflow UI"
echo "   2. Configure your AWS connection in Airflow UI"
echo "   3. Update variables in Airflow UI (s3_bucket_name, etc.)"
echo "   4. Enable the 'clinical_trials_etl_dbt' DAG"
echo "   5. Run: dbt docs generate && dbt docs serve (for DBT documentation)"