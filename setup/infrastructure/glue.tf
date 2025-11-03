# ========================================
# AWS GLUE CATALOG SETUP
# ========================================
# This creates the Glue database for Iceberg performance testing
# Individual table definitions are in glue_tables_detailed.tf
# ========================================

# Glue Catalog Database for Iceberg performance testing
resource "aws_glue_catalog_database" "iceberg_database" {
  name        = var.glue_database_name
  description = var.glue_database_description
  catalog_id  = local.glue_catalog_id

  tags = merge(local.common_tags, {
    Name        = "Iceberg Performance Test Database"
    Description = "AWS Glue database for Iceberg performance testing tables"
  })
}




# ========================================
# DETAILED GLUE TABLES
# ========================================
# Individual table definitions with proper column schemas
# See glue_tables_detailed.tf for all table definitions

# Note: All detailed table definitions are now in glue_tables_detailed.tf
# This provides better maintainability and clearer column definitions
# for each table type with appropriate data types and relationships
