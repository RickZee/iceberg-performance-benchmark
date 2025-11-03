# Variables for AWS Infrastructure Setup
# Snowflake Iceberg Performance Testing Project

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name of the project for resource naming"
  type        = string
  default     = "iceberg-performance-test"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for Iceberg data storage"
  type        = string
  default     = ""
}

variable "glue_database_name" {
  description = "Name of the AWS Glue database"
  type        = string
  default     = "iceberg_performance_test"
}

variable "glue_catalog_id" {
  description = "AWS Glue catalog ID (account ID)"
  type        = string
  default     = "YOUR_AWS_ACCOUNT_ID"
}

variable "snowflake_account" {
  description = "Snowflake account identifier for IAM trust policy"
  type        = string
  default     = "YOUR_SNOWFLAKE_ACCOUNT_ID"
}

variable "snowflake_external_id" {
  description = <<-EOT
    Snowflake external ID for additional security.
    
    ⚠️ IMPORTANT: This is optional for initial deployment. After creating Snowflake
    integrations, you must retrieve the actual external ID from:
    - DESC EXTERNAL VOLUME iceberg_glue_volume; (STORAGE_AWS_EXTERNAL_ID)
    - DESC CATALOG INTEGRATION aws_glue_catalog; (GLUE_AWS_EXTERNAL_ID)
    
    Then update the IAM role trust policy in AWS Console to include this external ID
    in the Condition block.
    
    See: setup/infrastructure/README_GLUE_ICEBERG.md#-important-cross-account-configuration
  EOT
  type        = string
  default     = ""
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = false
}

variable "enable_lifecycle" {
  description = "Enable S3 lifecycle rules for cost optimization"
  type        = bool
  default     = true
}

variable "temp_data_retention_days" {
  description = "Number of days to retain temporary test data"
  type        = number
  default     = 7
}

variable "results_retention_days" {
  description = "Number of days to retain test results before archiving"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "kms_key_id" {
  description = "KMS key ID for S3 bucket encryption (optional)"
  type        = string
  default     = ""
}

variable "s3_prefix" {
  description = "S3 prefix for Iceberg data storage"
  type        = string
  default     = "iceberg-performance-test"
}

variable "glue_database_description" {
  description = "Description for the Glue database"
  type        = string
  default     = "AWS Glue database for Iceberg performance testing tables"
}

variable "iam_role_name" {
  description = "Name for the IAM role for Snowflake integration"
  type        = string
  default     = "snowflake-iceberg-role"
}

variable "iam_policy_name" {
  description = "Name for the IAM policy for S3 and Glue access"
  type        = string
  default     = "snowflake-iceberg-policy"
}

variable "snowflake_user_arn" {
  description = <<-EOT
    ARN of the Snowflake IAM user for trust policy.
    
    ⚠️ IMPORTANT: This is a PLACEHOLDER. After creating Snowflake integrations, you must:
    1. Run: DESC EXTERNAL VOLUME iceberg_glue_volume;
    2. Run: DESC CATALOG INTEGRATION aws_glue_catalog;
    3. Update the IAM role trust policy in AWS Console with the actual IAM user ARN
       from STORAGE_AWS_IAM_USER_ARN or GLUE_AWS_IAM_USER_ARN
    
    This variable is only used during initial Terraform deployment. The trust policy
    MUST be manually updated after Snowflake integration creation.
    
    See: setup/infrastructure/README_GLUE_ICEBERG.md#-important-cross-account-configuration
  EOT
  type        = string
  # Placeholder ARN - must be replaced with actual Snowflake IAM user ARN after integration creation
  default     = "arn:aws:iam::000000000000:user/PLACEHOLDER_REPLACE_AFTER_SNOWFLAKE_INTEGRATION"
}

variable "snowflake_database_name" {
  description = "Name of the Snowflake-specific Glue database"
  type        = string
  default     = "iceberg-performance-test"
}

variable "glue_table_name" {
  description = "Name of the Glue table for Iceberg integration"
  type        = string
  default     = "loans_iceberg_glue"
}

variable "table_uuid" {
  description = "UUID for the Iceberg table"
  type        = string
  default     = "12345678-1234-1234-1234-123456789012"
}

variable "last_updated_ms" {
  description = "Last updated timestamp in milliseconds"
  type        = number
  default     = 1728746400000
}

# Complete setup configuration variables
variable "enable_complete_setup" {
  description = "Enable complete setup with all table formats"
  type        = bool
  default     = true
}

variable "enable_glue_tables" {
  description = "Enable Glue-managed Iceberg tables"
  type        = bool
  default     = true
}

variable "enable_snowflake_tables" {
  description = "Enable Snowflake-managed Iceberg tables"
  type        = bool
  default     = true
}
