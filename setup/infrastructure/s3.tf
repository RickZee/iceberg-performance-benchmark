# S3 Bucket Configuration for Iceberg Data Storage
# Snowflake Iceberg Performance Testing Project

# S3 Bucket for Iceberg data storage
resource "aws_s3_bucket" "iceberg_data" {
  bucket = local.bucket_name

  tags = merge(local.common_tags, {
    Name        = "Iceberg Performance Test Data"
    Description = "S3 bucket for storing Iceberg table data and metadata"
  })
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "iceberg_data" {
  count  = var.enable_versioning ? 1 : 0
  bucket = aws_s3_bucket.iceberg_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Server-Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_id != "" ? var.kms_key_id : null
    }
    bucket_key_enabled = var.kms_key_id != "" ? true : null
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "iceberg_data" {
  count  = var.enable_lifecycle ? 1 : 0
  bucket = aws_s3_bucket.iceberg_data.id

  rule {
    id     = "temp_data_cleanup"
    status = "Enabled"

    filter {
      prefix = "${var.s3_prefix}/temp/"
    }

    expiration {
      days = var.temp_data_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.temp_data_retention_days
    }
  }

  rule {
    id     = "results_archive"
    status = "Enabled"

    filter {
      prefix = "${var.s3_prefix}/results/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "incomplete_multipart_cleanup"
    status = "Enabled"

    filter {
      prefix = ""
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# S3 Bucket Notification Configuration (for future CloudWatch integration)
resource "aws_s3_bucket_notification" "iceberg_data" {
  count  = var.enable_lifecycle ? 1 : 0
  bucket = aws_s3_bucket.iceberg_data.id

  # This can be extended to send notifications to SNS, SQS, or Lambda
  # For now, it's a placeholder for future monitoring integration
}

# S3 Bucket CORS Configuration (if needed for web-based tools)
resource "aws_s3_bucket_cors_configuration" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

# S3 Bucket Policy for additional security
resource "aws_s3_bucket_policy" "iceberg_data" {
  bucket = aws_s3_bucket.iceberg_data.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "DenyInsecureConnections"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.iceberg_data.arn,
          "${aws_s3_bucket.iceberg_data.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid    = "AllowSnowflakeAccess"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.snowflake_role.arn
        }
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

  depends_on = [aws_iam_role.snowflake_role]
}
