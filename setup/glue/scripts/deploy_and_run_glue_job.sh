#!/bin/bash
# Deploy and run AWS Glue ETL job to create Iceberg tables

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    source "$PROJECT_ROOT/load_env_vars.sh"
fi

# Configuration
JOB_NAME="create-iceberg-tables-tpcds"
BUCKET_NAME="${AWS_S3_BUCKET:-your-bucket-name}"
DATABASE="${AWS_GLUE_DATABASE:-iceberg_performance_test}"
WAREHOUSE_PATH="${AWS_S3_ICEBERG_GLUE_PATH:-s3://your-bucket-name/iceberg-performance-test/iceberg_glue_format}"
REGION="${AWS_REGION:-us-east-1}"
ACCOUNT_ID="${AWS_ACCOUNT_ID}"

# Glue script location
GLUE_SCRIPT="$SCRIPT_DIR/create_all_iceberg_tables_glue_etl.py"
S3_SCRIPT_KEY="glue-scripts/create_iceberg_tables.py"

echo "=============================================================================="
echo "AWS GLUE ETL JOB DEPLOYMENT"
echo "=============================================================================="
echo ""
echo "Configuration:"
echo "  Job Name: $JOB_NAME"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  Glue Database: $DATABASE"
echo "  Warehouse Path: $WAREHOUSE_PATH"
echo "  Region: $REGION"
echo ""

# Check if script exists
if [ ! -f "$GLUE_SCRIPT" ]; then
    echo -e "${RED}❌ Script not found: $GLUE_SCRIPT${NC}"
    echo "   Generating script..."
    python3 "$SCRIPT_DIR/generate_glue_etl_job.py"
fi

# Upload script to S3
echo "📤 Uploading script to S3..."
aws s3 cp "$GLUE_SCRIPT" "s3://$BUCKET_NAME/$S3_SCRIPT_KEY" --region "$REGION"
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Script uploaded successfully${NC}"
else
    echo -e "${RED}❌ Failed to upload script${NC}"
    exit 1
fi

# Get Glue service role ARN
GLUE_ROLE_NAME="${AWS_GLUE_ROLE_NAME:-your-glue-service-role}"
if [ -z "$ACCOUNT_ID" ]; then
    echo -e "${RED}❌ Error: AWS_ACCOUNT_ID environment variable not set${NC}"
    echo "   Please set AWS_ACCOUNT_ID in your .env file or environment"
    exit 1
fi
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}"

echo ""
echo "📋 Creating/Updating Glue job..."

# Check if job exists
if aws glue get-job --job-name "$JOB_NAME" --region "$REGION" &>/dev/null; then
    echo "  Job exists, updating..."
    aws glue update-job \
        --job-name "$JOB_NAME" \
        --job-update "{
            \"Role\": \"$GLUE_ROLE_ARN\",
            \"Command\": {
                \"Name\": \"glueetl\",
                \"ScriptLocation\": \"s3://$BUCKET_NAME/$S3_SCRIPT_KEY\",
                \"PythonVersion\": \"3\"
            },
            \"DefaultArguments\": {
                \"--job-language\": \"python\",
                \"--job-bookmark-option\": \"job-bookmark-disable\",
                \"--GLUE_DATABASE\": \"$DATABASE\",
                \"--S3_WAREHOUSE_PATH\": \"$WAREHOUSE_PATH\",
                \"--AWS_REGION\": \"$REGION\",
                \"--enable-metrics\": \"\",
                \"--enable-spark-ui\": \"true\",
                \"--enable-continuous-cloudwatch-log\": \"true\"
            },
            \"GlueVersion\": \"4.0\",
            \"NumberOfWorkers\": 2,
            \"WorkerType\": \"G.1X\",
            \"MaxRetries\": 0
        }" \
        --region "$REGION"
else
    echo "  Creating new job..."
    aws glue create-job \
        --name "$JOB_NAME" \
        --role "$GLUE_ROLE_ARN" \
        --command "{
            \"Name\": \"glueetl\",
            \"ScriptLocation\": \"s3://$BUCKET_NAME/$S3_SCRIPT_KEY\",
            \"PythonVersion\": \"3\"
        }" \
        --default-arguments "{
            \"--job-language\": \"python\",
            \"--job-bookmark-option\": \"job-bookmark-disable\",
            \"--GLUE_DATABASE\": \"$DATABASE\",
            \"--S3_WAREHOUSE_PATH\": \"$WAREHOUSE_PATH\",
            \"--AWS_REGION\": \"$REGION\",
            \"--enable-metrics\": \"\",
            \"--enable-spark-ui\": \"true\",
            \"--enable-continuous-cloudwatch-log\": \"true\"
        }" \
        --glue-version "4.0" \
        --number-of-workers 2 \
        --worker-type "G.1X" \
        --max-retries 0 \
        --region "$REGION"
fi

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Glue job created/updated successfully${NC}"
else
    echo -e "${RED}❌ Failed to create/update Glue job${NC}"
    exit 1
fi

echo ""
echo "=============================================================================="
echo "JOB READY"
echo "=============================================================================="
echo ""
echo "To start the job, run:"
echo "  aws glue start-job-run --job-name $JOB_NAME --region $REGION"
echo ""
echo "Or start it now? (y/n)"
read -r response

if [[ "$response" =~ ^[Yy]$ ]]; then
    echo ""
    echo "🚀 Starting Glue job..."
    RUN_ID=$(aws glue start-job-run --job-name "$JOB_NAME" --region "$REGION" --query 'JobRunId' --output text)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Job started successfully!${NC}"
        echo "  Run ID: $RUN_ID"
        echo ""
        echo "Monitor the job at:"
        echo "  https://console.aws.amazon.com/glue/home?region=$REGION#etl:tab=jobs"
        echo ""
        echo "To check job status:"
        echo "  aws glue get-job-run --job-name $JOB_NAME --run-id $RUN_ID --region $REGION"
    else
        echo -e "${RED}❌ Failed to start job${NC}"
        exit 1
    fi
fi

echo ""
echo "=============================================================================="

