# IAM Configuration for Snowflake Integration
# Snowflake Iceberg Performance Testing Project
#
# ⚠️ IMPORTANT: Cross-Account Trust Policy Setup
# ================================================
# Snowflake runs on its own AWS backend accounts. After creating Snowflake integrations
# (external volume and catalog integration), you MUST update this trust policy with
# Snowflake's actual IAM user ARN and external ID.
#
# Workflow:
# 1. Deploy this Terraform configuration (creates IAM role with initial trust policy)
# 2. Create Snowflake external volume and catalog integration (see create_tpcds_objects.sql)
# 3. Retrieve Snowflake's IAM details:
#    - Run: DESC EXTERNAL VOLUME iceberg_glue_volume;
#    - Run: DESC CATALOG INTEGRATION aws_glue_catalog;
# 4. Update the trust policy in AWS IAM Console with:
#    - Snowflake's IAM user ARN (from STORAGE_AWS_IAM_USER_ARN or GLUE_AWS_IAM_USER_ARN)
#    - Snowflake's external ID (from STORAGE_AWS_EXTERNAL_ID or GLUE_AWS_EXTERNAL_ID)
#
# For detailed instructions, see:
# - setup/infrastructure/README.md#cross-account-configuration
# - docs/glue-integration-journey.md
#
# ================================================

# IAM Role for Snowflake Integration
resource "aws_iam_role" "snowflake_role" {
  name = var.iam_role_name
  path = "/"

  # Trust Policy
  # NOTE: This initial trust policy allows Terraform management and includes a placeholder
  # for Snowflake. After creating Snowflake integrations, you MUST manually update this
  # trust policy in AWS IAM Console with Snowflake's actual IAM user ARN and external ID.
  #
  # The trust policy should ultimately contain:
  # - Statement for Snowflake's IAM user ARN with external ID condition
  # - Statement for Terraform management (if needed)
  #
  # Example final trust policy (update after Snowflake integration creation):
  # {
  #   "Version": "2012-10-17",
  #   "Statement": [
  #     {
  #       "Effect": "Allow",
  #       "Principal": {
  #         "AWS": "arn:aws:iam::ACCOUNT_ID:user/SNOWFLAKE_USER"
  #       },
  #       "Action": "sts:AssumeRole",
  #       "Condition": {
  #         "StringEquals": {
  #           "sts:ExternalId": "SNOWFLAKE_EXTERNAL_ID"
  #         }
  #       }
  #     }
  #   ]
  # }
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Placeholder for Snowflake IAM user (UPDATE AFTER CREATING SNOWFLAKE INTEGRATIONS)
      # Replace var.snowflake_user_arn with Snowflake's actual IAM user ARN from:
      # DESC EXTERNAL VOLUME iceberg_glue_volume; or DESC CATALOG INTEGRATION aws_glue_catalog;
      {
        Effect = "Allow"
        Principal = {
          AWS = var.snowflake_user_arn
        }
        Action = "sts:AssumeRole"
        # Add external ID condition after retrieving it from Snowflake DESC commands
        # Condition = {
        #   StringEquals = {
        #     "sts:ExternalId" = "SNOWFLAKE_EXTERNAL_ID_FROM_DESC_COMMAND"
        #   }
        # }
      },
      # Allow Terraform management (for infrastructure deployment)
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${local.glue_catalog_id}:user/YOUR_TERRAFORM_USER"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name        = "Snowflake Iceberg Integration Role"
    Description = "IAM role for Snowflake to access S3 and Glue resources. ⚠️ Trust policy must be updated after creating Snowflake integrations."
  })
}

# IAM Policy for Snowflake S3 and Glue Access
resource "aws_iam_policy" "snowflake_policy" {
  name        = var.iam_policy_name
  description = "Policy for Snowflake to access S3 and Glue resources for Iceberg tables"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketAccess"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning"
        ]
        Resource = aws_s3_bucket.iceberg_data.arn
      },
      {
        Sid    = "S3ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObjectVersion",
          "s3:DeleteObjectVersion"
        ]
        Resource = "${aws_s3_bucket.iceberg_data.arn}/*"
      },
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetCatalog",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreateDatabase",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchUpdatePartition",
          "glue:BatchDeletePartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:database/${var.glue_database_name}",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:table/${var.glue_database_name}/*",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:database/${var.snowflake_database_name}",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:table/${var.snowflake_database_name}/*",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:database/default",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:table/default/*",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:database/events",
          "arn:aws:glue:${var.aws_region}:${local.glue_catalog_id}:table/events/*"
        ]
      },
      {
        Sid    = "GlueDataCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDataCatalogEncryptionSettings",
          "glue:PutDataCatalogEncryptionSettings"
        ]
        Resource = "*"
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:DescribeKey",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:ReEncrypt*"
        ]
        Resource = var.kms_key_id != "" ? var.kms_key_id : "*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "Snowflake Iceberg Access Policy"
  })
}

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "snowflake_policy_attachment" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_policy.arn
}

# IAM Role for Glue Service (if using Glue crawler)
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "Glue Service Role"
  })
}

# Attach AWS managed policy for Glue service role
resource "aws_iam_role_policy_attachment" "glue_service_role_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Additional IAM policy for Glue service role with S3 access
resource "aws_iam_role_policy" "glue_service_s3_policy" {
  name = "${var.project_name}-glue-service-s3-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.iceberg_data.arn,
          "${aws_s3_bucket.iceberg_data.arn}/*"
        ]
      }
    ]
  })
}

# IAM Instance Profile for EC2 instances (if needed for testing)
resource "aws_iam_instance_profile" "iceberg_testing" {
  name = "${var.project_name}-testing-profile"
  role = aws_iam_role.snowflake_role.name

  tags = merge(local.common_tags, {
    Name = "Iceberg Testing Instance Profile"
  })
}

# IAM Policy for CloudWatch Logs (for monitoring and debugging)
resource "aws_iam_policy" "cloudwatch_logs_policy" {
  name        = "${var.project_name}-cloudwatch-logs-policy"
  description = "Policy for CloudWatch Logs access"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${local.glue_catalog_id}:*"
      }
    ]
  })

  tags = merge(local.common_tags, {
    Name = "CloudWatch Logs Policy"
  })
}

# Attach CloudWatch Logs policy to Snowflake role
resource "aws_iam_role_policy_attachment" "snowflake_cloudwatch_logs" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}
