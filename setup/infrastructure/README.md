# AWS Infrastructure Setup for Snowflake Iceberg Performance Testing

> **Main Documentation**: See the [main README](../README.md) for complete project overview and setup instructions.

This directory contains Terraform configuration to provision AWS infrastructure required for the Snowflake Iceberg performance testing project.

## Table of Contents

- [Overview](#overview)
- [What Terraform Creates](#what-terraform-creates)
  - [S3 Bucket Configuration](#s3-bucket-configuration)
  - [AWS Glue Catalog](#aws-glue-catalog)
  - [IAM Configuration](#iam-configuration)
  - [Cross-Account Setup](#cross-account-setup)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration Files](#configuration-files)
- [Architecture](#architecture)
- [Created Resources](#created-resources)
- [Table Schemas](#table-schemas)
- [Configuration Options](#configuration-options)
- [Integration with Snowflake](#integration-with-snowflake)
- [Cross-Account Configuration](#cross-account-configuration)
- [Monitoring and Maintenance](#monitoring-and-maintenance)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)
- [Next Steps](#next-steps)
- [Support](#support)

## Overview

The Terraform configuration creates the following AWS resources:

- **S3 Bucket**: Storage for Iceberg table data with versioning and lifecycle policies
- **AWS Glue Database**: Catalog for Iceberg table metadata
- **IAM Role**: Role with trust policy for Snowflake integration
- **IAM Policies**: Permissions for S3 and Glue access

## What Terraform Creates

### S3 Bucket Configuration

- **Bucket Name**: Auto-generated or custom name for Iceberg data storage
- **Versioning**: Enabled for data protection and rollback capabilities
- **Encryption**: Server-side encryption (AES256) for data at rest
- **Lifecycle Rules**:
  - Temporary data cleanup (7 days)
  - Results archiving (30 days to IA, 90 days to Glacier)
  - Incomplete multipart upload cleanup
- **Security**: Public access blocked, HTTPS-only policy
- **CORS**: Configured for web-based tools and Snowflake access

### AWS Glue Catalog

- **Database**: `iceberg_performance_test` (or custom name)
- **Location**: Points to S3 bucket with Iceberg prefix
- **Parameters**: Iceberg-specific configuration for table metadata
- **Tables**: Pre-configured TPC-DS benchmark tables (24 tables total):
  - **Dimension Tables (17)**: `call_center`, `catalog_page`, `customer`, `customer_address`, `customer_demographics`, `date_dim`, `household_demographics`, `income_band`, `item`, `promotion`, `reason`, `ship_mode`, `store`, `time_dim`, `warehouse`, `web_page`, `web_site`
  - **Fact Tables (7)**: `catalog_returns`, `catalog_sales`, `inventory`, `store_returns`, `store_sales`, `web_returns`, `web_sales`

### IAM Configuration

- **Trust Policy**: Allows Snowflake to assume the role for data access
  - **Note**: After creating Snowflake integrations, you must update the trust policy with Snowflake's IAM user ARN and external ID (see Cross-Account Setup below)
- **S3 Permissions**: Full read/write access to the Iceberg data bucket
- **Glue Permissions**: Database and table management capabilities
- **KMS Permissions**: Encryption key access (if customer-managed keys used)
- **CloudWatch**: Logging permissions for monitoring and debugging

### Cross-Account Setup

Since Snowflake runs on its own AWS backend accounts, cross-account access requires:

1. Create Snowflake external volume and catalog integration:
   ```bash
   python3 run_create_tpcds_objects.py
   ```
   Or execute `setup/schemas/create_tpcds_objects.sql` in Snowflake

2. Retrieve Snowflake's IAM user ARN and external ID:
   
   **Automated (recommended):**
   ```bash
   cd setup/infrastructure
   python3 get_snowflake_iam_details.py
   ```
   
   **Manual SQL:**
   ```sql
   DESC EXTERNAL VOLUME iceberg_glue_volume;
   DESC CATALOG INTEGRATION aws_glue_catalog;
   ```

3. Update the IAM role trust policy:
   
   **Automated (recommended):**
   ```bash
   cd setup/infrastructure
   python3 update_iam_trust_policy.py
   ```
   
   **Manual:** Update the trust policy in AWS IAM Console with the retrieved values

**For detailed cross-account setup instructions**, see the [Cross-Account Configuration](#cross-account-configuration) section below.

### Security Features

- **Data Encryption**: S3 server-side encryption enabled
- **Access Control**: IAM role-based access with least privilege
- **Network Security**: VPC endpoints for private access (optional)
- **Audit Logging**: CloudTrail integration for API call tracking

### Cost Optimization

- **Lifecycle Policies**: Automatic data archival to reduce storage costs
- **Intelligent Tiering**: S3 Intelligent Tiering for cost optimization
- **Monitoring**: CloudWatch metrics for cost tracking
- **Tagging**: Resource tagging for cost allocation and management

### Integration Points

- **Snowflake External Volume**: S3 bucket configured for Snowflake external volumes
- **Storage Integration**: S3 integration for Snowflake stages and external tables
- **Catalog Integration**: Glue catalog integration for Iceberg tables
- **Cross-Account Access**: Support for Snowflake cross-account data access

## Prerequisites

### Software Requirements

1. **Terraform** (>= 1.0)

   ```bash
   # Install via Homebrew (macOS)
   brew install terraform
   
   # Or download from https://www.terraform.io/downloads.html
   ```

2. **AWS CLI** (>= 2.0)

   ```bash
   # Install via Homebrew (macOS)
   brew install awscli
   
   # Or download from https://aws.amazon.com/cli/
   ```

3. **AWS Credentials**

   ```bash
   # Configure AWS credentials
   aws configure
   
   # Or set environment variables
   export AWS_ACCESS_KEY_ID="your-access-key"
   export AWS_SECRET_ACCESS_KEY="your-secret-key"
   export AWS_DEFAULT_REGION="us-east-1"
   ```

### AWS Permissions

Your AWS user/role needs the following permissions to run Terraform:

- `s3:CreateBucket`, `s3:DeleteBucket`, `s3:ListBucket`
- `s3:PutBucketPolicy`, `s3:GetBucketPolicy`, `s3:DeleteBucketPolicy`
- `s3:PutBucketVersioning`, `s3:GetBucketVersioning`
- `s3:PutBucketLifecycleConfiguration`, `s3:GetBucketLifecycleConfiguration`
- `s3:PutBucketPublicAccessBlock`, `s3:GetBucketPublicAccessBlock`
- `glue:CreateDatabase`, `glue:DeleteDatabase`, `glue:GetDatabase`
- `glue:CreateTable`, `glue:DeleteTable`, `glue:GetTable`
- `iam:CreateRole`, `iam:DeleteRole`, `iam:GetRole`
- `iam:CreatePolicy`, `iam:DeletePolicy`, `iam:GetPolicy`
- `iam:AttachRolePolicy`, `iam:DetachRolePolicy`
- `iam:PassRole`

## Quick Start

### 1. Configure Variables

Copy the example variables file and update with your values:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` with your configuration:

```hcl
# Required variables
aws_region = "us-east-1"
project_name = "iceberg-performance-test"
snowflake_account = "your-snowflake-account-id"

# Optional variables
s3_bucket_name = ""  # Leave empty to auto-generate
glue_database_name = "iceberg_performance_test"
environment = "dev"
```

### 2. Initialize Terraform

```bash
terraform init
```

### 3. Review the Plan

```bash
terraform plan
```

### 4. Apply the Configuration

```bash
terraform apply
```

### 5. Get Output Values

```bash
terraform output
```

**Important Next Step**: After creating Snowflake integrations, you must update the IAM role trust policy. See [Cross-Account Setup](#cross-account-setup) section above.

## Configuration Files

### Core Files

- `main.tf` - Provider configuration and main resource definitions
- `variables.tf` - Input variables and their descriptions
- `outputs.tf` - Output values for integration
- `s3.tf` - S3 bucket configuration
- `glue.tf` - AWS Glue catalog configuration
- `iam.tf` - IAM roles and policies

### Glue Iceberg Specific Files

- `glue_iceberg_tables.tf` - Core TPC-DS table definitions (call_center, customer, item, store_sales)
- `glue_iceberg_tables_complete.tf` - Complete TPC-DS table definitions (all 24 tables)
- `glue_iceberg_variables.tf` - Iceberg-specific variables
- `glue_iceberg_outputs.tf` - Iceberg-specific outputs
- `get_snowflake_iam_details.py` - Script to retrieve Snowflake IAM details
- `update_iam_trust_policy.py` - Script to update IAM trust policy

### Deployment Scripts

- `deploy_glue_iceberg.sh` - Deployment script
- `terraform.tfvars.example` - Example variable values

## Architecture

```text
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   AWS Glue      │    │   S3 Bucket      │    │   Snowflake     │
│   Catalog       │◄───┤   (Iceberg Data) │◄───┤   (Queries)     │
│   (Metadata)    │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Created Resources

### Glue Database

- **Name**: `iceberg_performance_test` (or custom name)
- **Description**: AWS Glue database for Iceberg performance testing tables
- **Tables**: 24 TPC-DS benchmark tables (17 dimension + 7 fact tables)

### S3 Structure

```text
s3://your-bucket-name/
└── iceberg-performance-test/
    └── iceberg_glue_format/
        ├── call_center/
        ├── catalog_page/
        ├── catalog_returns/
        ├── catalog_sales/
        ├── customer/
        ├── customer_address/
        ├── customer_demographics/
        ├── date_dim/
        ├── household_demographics/
        ├── income_band/
        ├── inventory/
        ├── item/
        ├── promotion/
        ├── reason/
        ├── ship_mode/
        ├── store/
        ├── store_returns/
        ├── store_sales/
        ├── time_dim/
        ├── warehouse/
        ├── web_page/
        ├── web_returns/
        ├── web_sales/
        └── web_site/
```

### IAM Role

- **Name**: `snowflake-iceberg-role`
- **Purpose**: Allows Snowflake to access Glue and S3
- **Trust Policy**: Snowflake account integration (requires cross-account setup)

## Table Schemas

The infrastructure creates **24 TPC-DS benchmark tables**:

### Dimension Tables (17)

- **call_center** - Call center information with location and operational data
- **catalog_page** - Catalog page details
- **customer** - Customer demographics and contact information
- **customer_address** - Customer address information
- **customer_demographics** - Detailed customer demographics
- **date_dim** - Date dimension table for time-based analysis
- **household_demographics** - Household demographic information
- **income_band** - Income band classifications
- **item** - Product/item information with pricing and categorization
- **promotion** - Promotion and marketing campaign details
- **reason** - Return reason codes
- **ship_mode** - Shipping mode information
- **store** - Store information with location and management details
- **time_dim** - Time dimension table
- **warehouse** - Warehouse information
- **web_page** - Web page information
- **web_site** - Web site information

### Fact Tables (7)

- **catalog_returns** - Catalog return transactions
- **catalog_sales** - Catalog sales transactions
- **inventory** - Inventory levels by date, item, and warehouse
- **store_returns** - Store return transactions
- **store_sales** - Store sales transactions (primary fact table)
- **web_returns** - Web return transactions
- **web_sales** - Web sales transactions

All tables are configured with:
- **Format**: Iceberg with Parquet storage
- **Compression**: Snappy (configurable)
- **Target File Size**: 128MB
- **Distribution**: Hash-based (for fact tables)

## Configuration Options

### Iceberg Settings

- **Compression**: Snappy (configurable)
- **File Size**: 128MB target (configurable)
- **Distribution**: Hash-based (configurable)
- **Format**: Parquet (configurable)

### Optimization Features

- **Compaction**: Automatic small file merging
- **Time Travel**: 7-day retention (configurable)
- **Metrics**: Performance monitoring enabled
- **Retention**: 30-day snapshot retention

## Integration with Snowflake

After deployment, configure Snowflake to reference the Glue tables:

1. **Create External Volume**:

```sql
CREATE EXTERNAL VOLUME iceberg_glue_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_storage'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://your-bucket-name/iceberg-performance-test/iceberg_glue_format/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = FALSE;
```

2. **Create Catalog Integration**:

```sql
CREATE OR REPLACE CATALOG INTEGRATION aws_glue_catalog
  CATALOG_SOURCE = 'GLUE'
  CATALOG_NAMESPACE = 'iceberg_performance_test'
  GLUE_CATALOG_ID = 'YOUR_AWS_ACCOUNT_ID'
  GLUE_REGION = 'us-east-1'
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-iceberg-role'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE;
```

3. **Reference Tables**:

```sql
-- All 24 TPC-DS tables are automatically available through the catalog integration
-- Dimension tables
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center;
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.customer;
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.item;

-- Fact tables
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.store_sales;
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.web_sales;
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.catalog_sales;
```

## Cross-Account Configuration

**Snowflake runs on its own AWS backend accounts**, so you need to configure cross-account access by updating the IAM role trust policy after creating Snowflake integrations.

### Step 1: Retrieve Snowflake's IAM Details

**Option A: Automated Script (Recommended)**

After creating the external volume and catalog integration in Snowflake, run the automated script:

```bash
# From project root or infrastructure directory
python3 setup/infrastructure/get_snowflake_iam_details.py
# Or:
cd setup/infrastructure && python3 get_snowflake_iam_details.py
```

This script will:
- Connect to Snowflake and retrieve IAM details automatically
- Display the IAM user ARN and external ID
- Generate the trust policy JSON
- Save the values to files for use in the next step

**Option B: Manual SQL Commands**

Alternatively, you can run these SQL commands manually in Snowflake:

```sql
-- Get Snowflake's IAM user ARN and External ID for external volume
DESC EXTERNAL VOLUME iceberg_glue_volume;

-- Get Snowflake's IAM user ARN and External ID for catalog integration
DESC CATALOG INTEGRATION aws_glue_catalog;
```

**Record these values:**
- `STORAGE_AWS_IAM_USER_ARN` (from external volume) - Snowflake's IAM user ARN
- `STORAGE_AWS_EXTERNAL_ID` (from external volume) - Unique external ID
- `GLUE_AWS_IAM_USER_ARN` (from catalog integration) - May differ from storage ARN
- `GLUE_AWS_EXTERNAL_ID` (from catalog integration) - May differ from storage external ID

**Important**: The external volume and catalog integration may use the **same IAM user ARN** but have **different external IDs**. You need both in the trust policy.

### Step 2: Update IAM Role Trust Policy

**Option A: Automated Script (Recommended)**

After retrieving the IAM details, update the trust policy automatically:

```bash
# From project root or infrastructure directory
python3 setup/infrastructure/update_iam_trust_policy.py
# Or:
cd setup/infrastructure && python3 update_iam_trust_policy.py
```

This script will:
- Load the IAM details from the previous step
- Connect to AWS IAM
- Update the trust policy automatically
- Provide verification steps

**Option B: Manual AWS Console Update**

1. Go to AWS IAM Console → Roles → `snowflake-iceberg-role`
2. Click "Trust relationships" tab
3. Click "Edit trust policy"
4. Update the trust policy to include both external IDs if they differ:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<GLUE_AWS_IAM_USER_ARN>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<GLUE_AWS_EXTERNAL_ID>"
        }
      }
    }
  ]
}
```

**Note:** If the external volume and catalog integration use the same IAM user ARN but different external IDs, you need both statements. If they use the same external ID, you can use a single statement.

### Step 3: Verify Cross-Account Access

After updating the trust policy, test access from Snowflake:

```sql
-- Test catalog access
SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center LIMIT 1;
```

**📚 For detailed troubleshooting**, see `docs/glue-integration-journey.md` which includes:
- Complete cross-account workflow
- Troubleshooting cross-account issues
- Multiple AWS account scenarios
- Common error solutions

## Monitoring and Maintenance

### CloudWatch Metrics

- Glue job execution metrics
- S3 storage and request metrics
- IAM role usage metrics

### Cost Optimization

- S3 lifecycle policies for data archival
- Glue job scheduling for off-peak hours
- Iceberg table compaction for storage efficiency

## Troubleshooting

### Common Issues

#### Cross-Account Access Denied

- Verify IAM trust policy uses Snowflake's IAM user ARN (from `DESC EXTERNAL VOLUME` or `DESC CATALOG INTEGRATION`)
- Ensure external ID matches exactly (case-sensitive)
- Check that the trust policy includes both external IDs if they differ
- Verify the trust policy was updated after creating Snowflake integrations
- If Lake Formation is enabled, grant permissions in Lake Formation console

#### Invalid Trust Policy or "Confused Deputy" Error

- Mismatched external ID - verify it matches the value from `DESC` commands
- Incorrect IAM user ARN - ensure you're using Snowflake's ARN, not your account's ARN
- Missing external IDs - ensure both external volume and catalog integration external IDs are included if they differ

#### Table Not Found in Snowflake

- Ensure Glue tables were created in AWS Glue first (via Spark or Terraform)
- Verify catalog integration is properly configured
- Check that Glue database name matches in both AWS and Snowflake
- Ensure external volume path matches S3 metadata location

#### Permission Errors

- Verify IAM role policies include `glue:GetTable`, `glue:GetDatabase`, etc.
- Check S3 bucket policy allows the IAM role access
- Review CloudTrail logs for denied API calls

#### Other Issues

- Check CloudWatch logs for Glue job failures
- Verify IAM permissions for Snowflake access
- Monitor S3 access patterns and costs
- See `docs/glue-integration-journey.md` for detailed troubleshooting guide

## Cleanup

To remove all resources:

```bash
terraform destroy
```

**Warning**: This will delete all data in the S3 bucket and Glue tables.

## Next Steps

1. **Populate Tables**: Use the `setup/glue/` scripts to load TPC-DS data
2. **Configure Snowflake**: Set up external volume and catalog integration (see above)
3. **Run Tests**: Execute performance tests comparing different table formats
4. **Monitor**: Set up monitoring and alerting for the infrastructure

## Support

For issues or questions:

1. Check Terraform logs: `terraform logs`
2. Review AWS CloudWatch logs
3. Verify IAM permissions
4. Check Snowflake connectivity
5. See `docs/glue-integration-journey.md` for detailed troubleshooting
