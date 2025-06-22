#!/bin/bash

set -e

TERRAFORM_DIR="terraform"
KEY_NAME="my-ec2-key"  # Replace with your actual key name

# Step 1: Initialize and apply Terraform
cd $TERRAFORM_DIR
echo "🔧 Initializing Terraform..."
terraform init

echo "🚀 Applying Terraform configuration..."
terraform apply -auto-approve -var="key_name=$KEY_NAME" -var="ec2_ami=ami-0c55b159cbfafe1f0"

# Step 2: Extract EC2 Public IP
EC2_IP=$(terraform output -raw ec2_public_ip)
cd ..

# Step 3: Show deployment instructions
echo "\n✅ Infrastructure provisioned."
echo "🌐 Access Airflow Web UI at: http://$EC2_IP:8080"
echo "💻 SSH into EC2: ssh -i ~/.ssh/$KEY_NAME.pem ec2-user@$EC2_IP"
echo "📁 Next, run deploy.sh to copy code and start Airflow containers."