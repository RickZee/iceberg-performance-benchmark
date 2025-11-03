# Outputs for AWS Infrastructure
# Snowflake Iceberg Performance Testing Project

# S3 Bucket Information
output "s3_bucket_name" {
  description = "Name of the S3 bucket for Iceberg data storage"
  value       = aws_s3_bucket.iceberg_data.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.iceberg_data.arn
}

output "s3_bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = aws_s3_bucket.iceberg_data.bucket_domain_name
}

output "s3_bucket_regional_domain_name" {
  description = "Regional domain name of the S3 bucket"
  value       = aws_s3_bucket.iceberg_data.bucket_regional_domain_name
}

# AWS Glue Database Information
output "glue_database_name" {
  description = "Name of the AWS Glue database"
  value       = var.glue_database_name
}

output "glue_database_arn" {
  description = "ARN of the AWS Glue database"
  value       = "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:database/${var.glue_database_name}"
}

output "glue_catalog_id" {
  description = "AWS Glue catalog ID (account ID)"
  value       = local.glue_catalog_id
}

# AWS Glue Table Information
# Note: Individual table outputs removed - tables are defined in glue_tables_detailed.tf

# IAM Role Information
output "snowflake_role_arn" {
  description = "ARN of the IAM role for Snowflake integration"
  value       = aws_iam_role.snowflake_role.arn
}

output "snowflake_role_name" {
  description = "Name of the IAM role for Snowflake integration"
  value       = aws_iam_role.snowflake_role.name
}

output "snowflake_policy_arn" {
  description = "ARN of the IAM policy for Snowflake access"
  value       = aws_iam_policy.snowflake_policy.arn
}

# S3 Paths for Configuration
output "s3_iceberg_prefix" {
  description = "S3 prefix for Iceberg data storage"
  value       = var.s3_prefix
}

output "s3_iceberg_path" {
  description = "Full S3 path for Iceberg data storage"
  value       = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.s3_prefix}/"
}

output "s3_warehouse_path" {
  description = "S3 path for Iceberg warehouse"
  value       = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.s3_prefix}/warehouse/"
}

# Configuration Values for Snowflake Setup
output "snowflake_external_volume_config" {
  description = "Configuration for Snowflake external volume"
  value = {
    name = "iceberg_glue_volume"
    storage_locations = [
      {
        name                 = "s3_storage"
        url                  = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.s3_prefix}/"
        storage_aws_role_arn = aws_iam_role.snowflake_role.arn
      }
    ]
    comment = "External volume for Iceberg performance testing"
  }
}

output "snowflake_storage_integration_config" {
  description = "Configuration for Snowflake storage integration"
  value = {
    name                 = "s3_integration"
    storage_provider     = "S3"
    storage_aws_role_arn = aws_iam_role.snowflake_role.arn
    enabled              = true
    comment              = "Storage integration for Iceberg performance testing"
  }
}

output "snowflake_external_stage_config" {
  description = "Configuration for Snowflake external stage"
  value = {
    name                = "iceberg_external_stage"
    url                 = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.s3_prefix}/"
    storage_integration = "s3_integration"
    file_format         = "PARQUET"
    comment             = "External stage for Iceberg performance testing"
  }
}

# Environment Variables for Application
output "environment_variables" {
  description = "Environment variables for the application configuration"
  value = {
    AWS_REGION          = var.aws_region
    AWS_S3_BUCKET       = aws_s3_bucket.iceberg_data.bucket
    AWS_GLUE_DATABASE   = var.glue_database_name
    AWS_GLUE_CATALOG_ID = local.glue_catalog_id
    AWS_ROLE_ARN        = aws_iam_role.snowflake_role.arn
    AWS_ROLE_NAME       = aws_iam_role.snowflake_role.name
  }
}

# Resource Tags
output "resource_tags" {
  description = "Common tags applied to all resources"
  value       = local.common_tags
}

# AWS Account Information
output "aws_account_id" {
  description = "AWS Account ID"
  value       = local.glue_catalog_id
}

output "aws_region" {
  description = "AWS Region"
  value       = var.aws_region
}

# Project Information
output "project_name" {
  description = "Project name"
  value       = var.project_name
}

output "environment" {
  description = "Environment name"
  value       = var.environment
}

# Security Information
output "s3_bucket_policy" {
  description = "S3 bucket policy JSON"
  value       = aws_s3_bucket_policy.iceberg_data.policy
  sensitive   = true
}

output "iam_role_trust_policy" {
  description = "IAM role trust policy JSON"
  value       = aws_iam_role.snowflake_role.assume_role_policy
  sensitive   = true
}

# Monitoring and Logging
output "cloudwatch_log_group_arn" {
  description = "ARN of the CloudWatch log group (if created)"
  value       = var.enable_lifecycle ? "arn:aws:logs:${var.aws_region}:${local.glue_catalog_id}:log-group:/aws/glue/iceberg-performance-test" : null
}

# Cost Optimization Information
output "s3_lifecycle_rules" {
  description = "S3 lifecycle rules configuration"
  value = var.enable_lifecycle ? {
    temp_data_retention_days = var.temp_data_retention_days
    results_retention_days   = var.results_retention_days
    archive_to_ia_days       = 30
    archive_to_glacier_days  = 90
  } : null
}
