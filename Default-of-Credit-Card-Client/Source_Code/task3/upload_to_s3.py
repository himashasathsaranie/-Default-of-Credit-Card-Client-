# upload_to_s3.py
import boto3
import os

# AWS credentials (replace with your own or use IAM role)
aws_access_key = ""
aws_secret_key = ""
bucket_name = "bigdata-credit-snowflake-bucket"
file_path = "credit_card.csv"

# Initialize S3 client
s3 = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

# Upload file
s3.upload_file(file_path, bucket_name, "credit_card.csv")
print("File uploaded to S3 successfully.")
