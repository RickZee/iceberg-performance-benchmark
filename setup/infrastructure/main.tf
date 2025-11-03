# Main Terraform configuration for AWS Infrastructure
# Snowflake Iceberg Performance Testing Project

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(
      {
        Project     = var.project_name
        Environment = var.environment
        ManagedBy   = "Terraform"
        Purpose     = "Snowflake Iceberg Performance Testing"
      },
      var.tags
    )
  }
}

# Data sources
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# Local values for computed values
locals {
  # Generate S3 bucket name if not provided
  bucket_name = var.s3_bucket_name != "" ? var.s3_bucket_name : "${var.project_name}-${var.environment}-${random_id.bucket_suffix.hex}"

  # Glue catalog ID (account ID)
  glue_catalog_id = var.glue_catalog_id != "" ? var.glue_catalog_id : data.aws_caller_identity.current.account_id

  # Snowflake account for trust policy
  snowflake_account = var.snowflake_account != "" ? var.snowflake_account : "snowflake"

  # Common tags
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Purpose     = "Snowflake Iceberg Performance Testing"
  }
}

# Random ID for bucket suffix to ensure uniqueness
resource "random_id" "bucket_suffix" {
  byte_length = 4
}
