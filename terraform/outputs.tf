output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}