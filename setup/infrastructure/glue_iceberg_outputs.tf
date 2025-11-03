# ========================================
# GLUE ICEBERG TABLES OUTPUTS
# ========================================
# Outputs for Glue-managed Iceberg tables
# ========================================

# Glue Database Information (already defined in outputs.tf)
# output "glue_database_name" {
#   description = "Name of the Glue database for Iceberg tables"
#   value       = aws_glue_catalog_database.iceberg_database.name
# }

# output "glue_database_arn" {
#   description = "ARN of the Glue database"
#   value       = aws_glue_catalog_database.iceberg_database.arn
# }

# Glue Table Information
output "glue_iceberg_tables" {
  description = "List of created Glue Iceberg tables"
  value = {
    call_center = var.enable_glue_tables ? aws_glue_catalog_table.call_center_iceberg[0].name : null
    customer    = var.enable_glue_tables ? aws_glue_catalog_table.customer_iceberg[0].name : null
    item        = var.enable_glue_tables ? aws_glue_catalog_table.item_iceberg[0].name : null
    store_sales = var.enable_glue_tables ? aws_glue_catalog_table.store_sales_iceberg[0].name : null
  }
}

# S3 Paths for Iceberg Tables
output "iceberg_table_paths" {
  description = "S3 paths for Iceberg table data"
  value = {
    call_center = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/call_center/"
    customer    = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/customer/"
    item        = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/item/"
    store_sales = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/store_sales/"
  }
}

# Glue Catalog Information (already defined in outputs.tf)
# output "glue_catalog_id" {
#   description = "AWS Glue catalog ID"
#   value       = local.glue_catalog_id
# }

# Snowflake Integration Information
output "snowflake_integration_info" {
  description = "Information needed for Snowflake integration"
  value = {
    glue_database_name = aws_glue_catalog_database.iceberg_database.name
    glue_catalog_id    = local.glue_catalog_id
    s3_bucket_name     = aws_s3_bucket.iceberg_data.bucket
    iam_role_arn       = aws_iam_role.snowflake_role.arn
    external_id        = var.snowflake_external_id
  }
}

# Table Count Summary
output "table_creation_summary" {
  description = "Summary of created tables"
  value = {
    total_tables_created = var.enable_glue_tables ? 4 : 0
    tables = var.enable_glue_tables ? [
      "call_center",
      "customer", 
      "item",
      "store_sales"
    ] : []
  }
}
