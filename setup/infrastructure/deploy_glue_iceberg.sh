#!/bin/bash
# ========================================
# Deploy Glue Iceberg Tables Infrastructure
# ========================================
# This script deploys the AWS infrastructure for Glue-managed Iceberg tables
# ========================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "main.tf" ]; then
    print_error "Please run this script from the aws/ directory"
    exit 1
fi

# Check if terraform is installed
if ! command -v terraform &> /dev/null; then
    print_error "Terraform is not installed. Please install Terraform first."
    exit 1
fi

# Check if AWS credentials are configured
if ! aws sts get-caller-identity &> /dev/null; then
    print_error "AWS credentials not configured. Please run 'aws configure' or set environment variables."
    exit 1
fi

print_status "Starting Glue Iceberg tables infrastructure deployment..."

# Initialize Terraform
print_status "Initializing Terraform..."
terraform init

# Validate Terraform configuration
print_status "Validating Terraform configuration..."
terraform validate

# Plan the deployment
print_status "Planning Terraform deployment..."
terraform plan -out=tfplan

# Ask for confirmation
echo ""
print_warning "This will create AWS resources for Glue-managed Iceberg tables."
print_warning "Please review the plan above before proceeding."
echo ""
read -p "Do you want to proceed with the deployment? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_status "Deployment cancelled by user."
    exit 0
fi

# Apply the deployment
print_status "Applying Terraform configuration..."
terraform apply tfplan

# Clean up plan file
rm -f tfplan

# Show outputs
print_status "Deployment completed! Here are the outputs:"
echo ""
terraform output

print_success "Glue Iceberg tables infrastructure deployed successfully!"
print_status "Next steps:"
echo "1. Create Snowflake external volume and catalog integration:"
echo "   - Run: python3 run_create_tpcds_objects.py"
echo "   - Or execute: @setup/schemas/create_tpcds_objects.sql in Snowflake"
echo ""
echo "2. ⚠️ IMPORTANT: Update IAM role trust policy for cross-account access"
echo "   Automated method (recommended):"
echo "   - Run: python3 setup/infrastructure/get_snowflake_iam_details.py"
echo "   - Then: python3 setup/infrastructure/update_iam_trust_policy.py"
echo ""
echo "   Manual method:"
echo "   - Run: DESC EXTERNAL VOLUME iceberg_glue_volume; in Snowflake"
echo "   - Run: DESC CATALOG INTEGRATION aws_glue_catalog; in Snowflake"
echo "   - Update trust policy in AWS IAM Console with the retrieved values"
echo "   - See README_GLUE_ICEBERG.md for detailed instructions"
echo ""
echo "3. Use the glue-table-setup scripts to create and populate tables"
echo "4. Configure Snowflake to reference the Glue tables"
echo "5. Run performance tests"

# Show important information
echo ""
print_status "Important Information:"
echo "• Glue Database: $(terraform output -raw glue_database_name)"
echo "• S3 Bucket: $(terraform output -raw s3_bucket_name)"
echo "• IAM Role ARN: $(terraform output -raw snowflake_role_arn)"
echo "• External ID: $(terraform output -raw snowflake_external_id)"
