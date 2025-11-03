#!/bin/bash
# Script to load AWS credentials from CSV files into environment variables
# This script reads the credentials from the CSV files and exports them as environment variables

# Read AWS credentials from CSV file
# Update CSV_FILE_NAME to match your actual CSV file name
CSV_FILE_NAME="${AWS_CREDENTIALS_CSV:-your-terraform-accessKeys.csv}"
if [ -f "$CSV_FILE_NAME" ]; then
    # Skip header line and read credentials, trimming whitespace
    IFS=',' read -r access_key secret_key < <(tail -n +2 "$CSV_FILE_NAME" | tr -d '\r\n')
    
    # Trim whitespace from both keys
    access_key=$(echo "$access_key" | tr -d ' ')
    secret_key=$(echo "$secret_key" | tr -d ' ')
    
    # Export environment variables
    export AWS_ACCESS_KEY_ID="$access_key"
    export AWS_SECRET_ACCESS_KEY="$secret_key"
    export AWS_DEFAULT_REGION="us-east-1"
    
    echo "✅ AWS credentials loaded successfully"
    echo "   Access Key ID: ${AWS_ACCESS_KEY_ID:0:8}..."
    echo "   Secret Key: ${AWS_SECRET_ACCESS_KEY:0:8}..."
    echo "   Region: $AWS_DEFAULT_REGION"
else
    echo "❌ Error: $CSV_FILE_NAME not found"
    echo "   Set AWS_CREDENTIALS_CSV environment variable or create a CSV file with your credentials"
    exit 1
fi
