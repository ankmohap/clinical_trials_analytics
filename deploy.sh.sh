#!/bin/bash

set -e

KEY_NAME="my-ec2-key"         # Replace with your key name
KEY_PATH="$HOME/.ssh/$KEY_NAME.pem"
USER="ec2-user"
REMOTE_DIR="clinical-pipeline"
EC2_IP=$(terraform -chdir=terraform output -raw ec2_public_ip)

# Step 1: SSH into EC2 and prepare environment
echo "📦 Connecting to $EC2_IP and preparing environment..."
ssh -i "$KEY_PATH" $USER@$EC2_IP << 'EOF'
  sudo yum update -y
  sudo yum install -y docker git
  sudo service docker start
  sudo usermod -aG docker ec2-user
  mkdir -p $HOME/clinical-pipeline
EOF

# Step 2: Copy project code
echo "📤 Copying project to EC2..."
scp -i "$KEY_PATH" -r dags dbt docker plugins $USER@$EC2_IP:$REMOTE_DIR
scp -i "$KEY_PATH" docker/docker-compose.yml $USER@$EC2_IP:$REMOTE_DIR

# Step 3: Start Airflow with Docker Compose
echo "🚀 Starting Airflow on remote EC2..."
ssh -i "$KEY_PATH" $USER@$EC2_IP << EOF
  cd $REMOTE_DIR
  docker-compose up -d
EOF

echo "✅ Airflow deployed. Access it at: http://$EC2_IP:8080"