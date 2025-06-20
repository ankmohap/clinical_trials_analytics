#!/bin/bash

# DBT Commands Helper Script

set -e

COMMAND=${1:-help}

case $COMMAND in
    "deps")
        echo "📦 Installing DBT dependencies..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt deps"
        ;;
    "debug")
        echo "🔍 Testing DBT connection..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt debug"
        ;;
    "run")
        echo "🚀 Running DBT models..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt run"
        ;;
    "test")
        echo "🧪 Running DBT tests..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt test"
        ;;
    "docs")
        echo "📚 Generating DBT documentation..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt docs generate"
        echo "📖 To serve docs, run: docker-compose exec airflow-webserver bash -c 'cd /opt/airflow/dbt && dbt docs serve --port 8081'"
        ;;
    "seed")
        echo "🌱 Loading DBT seeds..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt seed"
        ;;
    "snapshot")
        echo "📸 Running DBT snapshots..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt snapshot"
        ;;
    "compile")
        echo "🔨 Compiling DBT models..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt compile"
        ;;
    "clean")
        echo "🧹 Cleaning DBT artifacts..."
        docker-compose exec airflow-webserver bash -c "cd /opt/airflow/dbt && dbt clean"
        ;;
    "help"|*)
        echo "🛠️  DBT Commands Helper"
        echo ""
        echo "Usage: ./scripts/dbt_commands.sh [command]"
        echo ""
        echo "Available commands:"
        echo "  deps      - Install DBT dependencies"
        echo "  debug     - Test DBT connection"
        echo "  run       - Run DBT models"
        echo "  test      - Run DBT tests"
        echo "  docs      - Generate DBT documentation"
        echo "  seed      - Load DBT seeds"
        echo "  snapshot  - Run DBT snapshots"
        echo "  compile   - Compile DBT models"
        echo "  clean     - Clean DBT artifacts"
        echo "  help      - Show this help message"
        ;;
esac