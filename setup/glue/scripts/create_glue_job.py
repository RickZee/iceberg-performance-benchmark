#!/usr/bin/env python3
"""
Create and deploy AWS Glue ETL job for creating Iceberg tables
This script uploads the Glue ETL job script to S3 and creates the Glue job
"""

import os
import sys
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

def load_env_vars():
    """Load environment variables"""
    project_root = Path(__file__).parent.parent.parent.parent
    env_file = project_root / '.env'
    
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def upload_script_to_s3(script_path, bucket_name, s3_key):
    """Upload Glue script to S3"""
    s3_client = boto3.client('s3')
    
    print(f"Uploading script to s3://{bucket_name}/{s3_key}")
    try:
        s3_client.upload_file(str(script_path), bucket_name, s3_key)
        script_url = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ Script uploaded: {script_url}")
        return script_url
    except Exception as e:
        print(f"❌ Error uploading script: {e}")
        return None

def create_glue_job(job_name, script_location, role_arn, database, warehouse_path, region, bucket_name):
    """Create or update AWS Glue job"""
    glue_client = boto3.client('glue', region_name=region)
    
    account_id = os.getenv('AWS_ACCOUNT_ID', 'YOUR_AWS_ACCOUNT_ID')
    
    job_params = {
        'Name': job_name,
        'Role': role_arn,
        'Command': {
            'Name': 'glueetl',
            'ScriptLocation': script_location,
            'PythonVersion': '3'
        },
        'DefaultArguments': {
            '--job-language': 'python',
            '--job-bookmark-option': 'job-bookmark-disable',
            '--GLUE_DATABASE': database,
            '--S3_WAREHOUSE_PATH': warehouse_path,
            '--AWS_REGION': region,
            '--enable-metrics': '',
            '--enable-spark-ui': 'true',
            '--spark-event-logs-path': f's3://aws-glue-assets-{region}-{account_id}/sparkHistoryLogs/',
            '--enable-continuous-cloudwatch-log': 'true',
            # Iceberg extensions configuration (must be set via job arguments)
            # Note: --conf is used for Spark configuration
            '--conf': 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
            '--additional-python-modules': 'pyiceberg',
            # Add Iceberg JARs - Glue 4.0 uses Spark 3.3, so we need Spark 3.3-compatible Iceberg JARs
            # Include AWS SDK bundle for Glue catalog dependencies
            '--extra-jars': f's3://{bucket_name}/glue-scripts/jars/aws-sdk-bundle-2.20.66.jar,s3://{bucket_name}/glue-scripts/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar,s3://{bucket_name}/glue-scripts/jars/iceberg-aws-1.4.2.jar',
        },
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
        'MaxRetries': 0,
    }
    
    try:
        # Try to get existing job
        try:
            existing_job = glue_client.get_job(JobName=job_name)
            print(f"Job '{job_name}' already exists, updating...")
            # Remove 'Name' from job_params for update
            update_params = {k: v for k, v in job_params.items() if k != 'Name'}
            glue_client.update_job(JobName=job_name, JobUpdate=update_params)
            print(f"✅ Updated Glue job: {job_name}")
        except glue_client.exceptions.EntityNotFoundException:
            print(f"Creating new Glue job: {job_name}")
            glue_client.create_job(**job_params)
            print(f"✅ Created Glue job: {job_name}")
        
        return True
    except Exception as e:
        print(f"❌ Error creating/updating Glue job: {e}")
        return False

def start_glue_job(job_name, region):
    """Start the Glue job"""
    glue_client = boto3.client('glue', region_name=region)
    
    try:
        response = glue_client.start_job_run(JobName=job_name)
        run_id = response['JobRunId']
        print(f"✅ Started Glue job run: {run_id}")
        print(f"\nMonitor the job at:")
        print(f"  https://console.aws.amazon.com/glue/home?region={region}#etl:tab=jobs")
        return run_id
    except Exception as e:
        print(f"❌ Error starting Glue job: {e}")
        return None

def main():
    """Main function"""
    print("=" * 70)
    print("AWS GLUE ETL JOB CREATION AND DEPLOYMENT")
    print("=" * 70)
    
    load_env_vars()
    
    # Configuration
    job_name = "create-iceberg-tables-tpcds"
    bucket_name = os.getenv('AWS_S3_BUCKET', 'your-bucket-name')
    database = os.getenv('AWS_GLUE_DATABASE', 'iceberg_performance_test')
    warehouse_path = os.getenv('AWS_S3_ICEBERG_GLUE_PATH', f's3://{bucket_name}/iceberg-performance-test/iceberg_glue_format')
    region = os.getenv('AWS_REGION', 'us-east-1')
    account_id = os.getenv('AWS_ACCOUNT_ID', 'YOUR_AWS_ACCOUNT_ID')
    
    if account_id == 'YOUR_AWS_ACCOUNT_ID':
        print("❌ Error: AWS_ACCOUNT_ID environment variable not set")
        print("   Please set AWS_ACCOUNT_ID in your .env file or environment")
        sys.exit(1)
    
    # Get IAM role for Glue (should be created by Terraform)
    glue_role_name = os.getenv('AWS_GLUE_ROLE_NAME', 'your-glue-service-role')
    glue_role_arn = f"arn:aws:iam::{account_id}:role/{glue_role_name}"
    
    print(f"\nConfiguration:")
    print(f"  Job Name: {job_name}")
    print(f"  S3 Bucket: {bucket_name}")
    print(f"  Glue Database: {database}")
    print(f"  Warehouse Path: {warehouse_path}")
    print(f"  Region: {region}")
    print(f"  Glue Role: {glue_role_arn}")
    
    # Find the script file
    project_root = Path(__file__).parent.parent.parent.parent
    script_file = project_root / "setup" / "glue" / "scripts" / "create_all_iceberg_tables_glue_etl.py"
    
    if not script_file.exists():
        print(f"\n❌ Script file not found: {script_file}")
        print("   Please run: python3 setup/glue/scripts/generate_glue_etl_job.py first")
        return 1
    
    # Upload script to S3
    s3_key = "glue-scripts/create_iceberg_tables.py"
    script_location = upload_script_to_s3(script_file, bucket_name, s3_key)
    
    if not script_location:
        return 1
    
    # Create Glue job
    if not create_glue_job(job_name, script_location, glue_role_arn, database, warehouse_path, region, bucket_name):
        return 1
    
    # Ask to start the job
    print("\n" + "=" * 70)
    response = input("Start the Glue job now? (yes/no): ").strip().lower()
    if response in ['yes', 'y']:
        run_id = start_glue_job(job_name, region)
        if run_id:
            print(f"\n✅ Job started successfully!")
            print(f"   Run ID: {run_id}")
    else:
        print("\nTo start the job later, run:")
        print(f"  aws glue start-job-run --job-name {job_name} --region {region}")
    
    print("\n" + "=" * 70)
    return 0

if __name__ == "__main__":
    sys.exit(main())

