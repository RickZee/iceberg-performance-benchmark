# ========================================
# GLUE-MANAGED ICEBERG TABLES FOR TPC-DS
# ========================================
# This file creates Glue-managed Iceberg tables for TPC-DS data
# These tables will be managed by AWS Glue and accessible from Snowflake
# ========================================

# TPC-DS Core Tables - Glue Iceberg Format

# Call Center Iceberg Table
resource "aws_glue_catalog_table" "call_center_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "call_center"
  database_name = aws_glue_catalog_database.iceberg_database.name
  catalog_id    = local.glue_catalog_id

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"                    = "ICEBERG"
    "write.format.default"          = "parquet"
    "write.parquet.compression-codec" = "snappy"
    "write.target-file-size-bytes"  = "134217728"
    "write.distribution-mode"       = "hash"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/call_center/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "call_center_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "cc_call_center_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "cc_call_center_id"
      type    = "string"
      comment = "Call center identifier"
    }
    columns {
      name    = "cc_rec_start_date"
      type    = "date"
      comment = "Record start date"
    }
    columns {
      name    = "cc_rec_end_date"
      type    = "date"
      comment = "Record end date"
    }
    columns {
      name    = "cc_closed_date_sk"
      type    = "int"
      comment = "Closed date SK"
    }
    columns {
      name    = "cc_open_date_sk"
      type    = "int"
      comment = "Open date SK"
    }
    columns {
      name    = "cc_name"
      type    = "string"
      comment = "Call center name"
    }
    columns {
      name    = "cc_class"
      type    = "string"
      comment = "Call center class"
    }
    columns {
      name    = "cc_employees"
      type    = "int"
      comment = "Number of employees"
    }
    columns {
      name    = "cc_sq_ft"
      type    = "int"
      comment = "Square footage"
    }
    columns {
      name    = "cc_hours"
      type    = "string"
      comment = "Operating hours"
    }
    columns {
      name    = "cc_manager"
      type    = "string"
      comment = "Manager name"
    }
    columns {
      name    = "cc_mkt_id"
      type    = "int"
      comment = "Market ID"
    }
    columns {
      name    = "cc_mkt_class"
      type    = "string"
      comment = "Market class"
    }
    columns {
      name    = "cc_mkt_desc"
      type    = "string"
      comment = "Market description"
    }
    columns {
      name    = "cc_market_manager"
      type    = "string"
      comment = "Market manager"
    }
    columns {
      name    = "cc_division"
      type    = "int"
      comment = "Division"
    }
    columns {
      name    = "cc_division_name"
      type    = "string"
      comment = "Division name"
    }
    columns {
      name    = "cc_company"
      type    = "int"
      comment = "Company"
    }
    columns {
      name    = "cc_company_name"
      type    = "string"
      comment = "Company name"
    }
    columns {
      name    = "cc_street_number"
      type    = "string"
      comment = "Street number"
    }
    columns {
      name    = "cc_street_name"
      type    = "string"
      comment = "Street name"
    }
    columns {
      name    = "cc_street_type"
      type    = "string"
      comment = "Street type"
    }
    columns {
      name    = "cc_suite_number"
      type    = "string"
      comment = "Suite number"
    }
    columns {
      name    = "cc_city"
      type    = "string"
      comment = "City"
    }
    columns {
      name    = "cc_county"
      type    = "string"
      comment = "County"
    }
    columns {
      name    = "cc_state"
      type    = "string"
      comment = "State"
    }
    columns {
      name    = "cc_zip"
      type    = "string"
      comment = "ZIP code"
    }
    columns {
      name    = "cc_country"
      type    = "string"
      comment = "Country"
    }
    columns {
      name    = "cc_gmt_offset"
      type    = "double"
      comment = "GMT offset"
    }
    columns {
      name    = "cc_tax_percentage"
      type    = "double"
      comment = "Tax percentage"
    }
  }

  # Note: Glue catalog tables don't support tags directly
}

# Customer Iceberg Table
resource "aws_glue_catalog_table" "customer_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "customer"
  database_name = aws_glue_catalog_database.iceberg_database.name
  catalog_id    = local.glue_catalog_id

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"                    = "ICEBERG"
    "write.format.default"          = "parquet"
    "write.parquet.compression-codec" = "snappy"
    "write.target-file-size-bytes"  = "134217728"
    "write.distribution-mode"       = "hash"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/customer/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "customer_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "c_customer_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "c_customer_id"
      type    = "string"
      comment = "Customer identifier"
    }
    columns {
      name    = "c_current_cdemo_sk"
      type    = "int"
      comment = "Current customer demographics SK"
    }
    columns {
      name    = "c_current_hdemo_sk"
      type    = "int"
      comment = "Current household demographics SK"
    }
    columns {
      name    = "c_current_addr_sk"
      type    = "int"
      comment = "Current address SK"
    }
    columns {
      name    = "c_first_shipto_date_sk"
      type    = "int"
      comment = "First ship-to date SK"
    }
    columns {
      name    = "c_first_sales_date_sk"
      type    = "int"
      comment = "First sales date SK"
    }
    columns {
      name    = "c_salutation"
      type    = "string"
      comment = "Salutation"
    }
    columns {
      name    = "c_first_name"
      type    = "string"
      comment = "First name"
    }
    columns {
      name    = "c_last_name"
      type    = "string"
      comment = "Last name"
    }
    columns {
      name    = "c_preferred_cust_flag"
      type    = "string"
      comment = "Preferred customer flag"
    }
    columns {
      name    = "c_birth_day"
      type    = "int"
      comment = "Birth day"
    }
    columns {
      name    = "c_birth_month"
      type    = "int"
      comment = "Birth month"
    }
    columns {
      name    = "c_birth_year"
      type    = "int"
      comment = "Birth year"
    }
    columns {
      name    = "c_birth_country"
      type    = "string"
      comment = "Birth country"
    }
    columns {
      name    = "c_login"
      type    = "string"
      comment = "Login"
    }
    columns {
      name    = "c_email_address"
      type    = "string"
      comment = "Email address"
    }
    columns {
      name    = "c_last_review_date"
      type    = "string"
      comment = "Last review date"
    }
  }

  # Note: Glue catalog tables don't support tags directly
}

# Item Iceberg Table
resource "aws_glue_catalog_table" "item_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "item"
  database_name = aws_glue_catalog_database.iceberg_database.name
  catalog_id    = local.glue_catalog_id

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"                    = "ICEBERG"
    "write.format.default"          = "parquet"
    "write.parquet.compression-codec" = "snappy"
    "write.target-file-size-bytes"  = "134217728"
    "write.distribution-mode"       = "hash"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/item/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "item_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "i_item_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "i_item_id"
      type    = "string"
      comment = "Item identifier"
    }
    columns {
      name    = "i_rec_start_date"
      type    = "date"
      comment = "Record start date"
    }
    columns {
      name    = "i_rec_end_date"
      type    = "date"
      comment = "Record end date"
    }
    columns {
      name    = "i_item_desc"
      type    = "string"
      comment = "Item description"
    }
    columns {
      name    = "i_current_price"
      type    = "decimal(7,2)"
      comment = "Current price"
    }
    columns {
      name    = "i_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Wholesale cost"
    }
    columns {
      name    = "i_brand_id"
      type    = "int"
      comment = "Brand ID"
    }
    columns {
      name    = "i_brand"
      type    = "string"
      comment = "Brand"
    }
    columns {
      name    = "i_class_id"
      type    = "int"
      comment = "Class ID"
    }
    columns {
      name    = "i_class"
      type    = "string"
      comment = "Class"
    }
    columns {
      name    = "i_category_id"
      type    = "int"
      comment = "Category ID"
    }
    columns {
      name    = "i_category"
      type    = "string"
      comment = "Category"
    }
    columns {
      name    = "i_manufact_id"
      type    = "int"
      comment = "Manufacturer ID"
    }
    columns {
      name    = "i_manufact"
      type    = "string"
      comment = "Manufacturer"
    }
    columns {
      name    = "i_size"
      type    = "string"
      comment = "Size"
    }
    columns {
      name    = "i_formulation"
      type    = "string"
      comment = "Formulation"
    }
    columns {
      name    = "i_color"
      type    = "string"
      comment = "Color"
    }
    columns {
      name    = "i_units"
      type    = "string"
      comment = "Units"
    }
    columns {
      name    = "i_container"
      type    = "string"
      comment = "Container"
    }
    columns {
      name    = "i_manager_id"
      type    = "int"
      comment = "Manager ID"
    }
    columns {
      name    = "i_product_name"
      type    = "string"
      comment = "Product name"
    }
  }

  # Note: Glue catalog tables don't support tags directly
}

# Store Sales Iceberg Table (Fact Table)
resource "aws_glue_catalog_table" "store_sales_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "store_sales"
  database_name = aws_glue_catalog_database.iceberg_database.name
  catalog_id    = local.glue_catalog_id

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"                    = "ICEBERG"
    "write.format.default"          = "parquet"
    "write.parquet.compression-codec" = "snappy"
    "write.target-file-size-bytes"  = "134217728"
    "write.distribution-mode"       = "hash"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/store_sales/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "store_sales_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "ss_sold_date_sk"
      type    = "int"
      comment = "Sold date SK"
    }
    columns {
      name    = "ss_sold_time_sk"
      type    = "int"
      comment = "Sold time SK"
    }
    columns {
      name    = "ss_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "ss_customer_sk"
      type    = "int"
      comment = "Customer SK"
    }
    columns {
      name    = "ss_cdemo_sk"
      type    = "int"
      comment = "Customer demographics SK"
    }
    columns {
      name    = "ss_hdemo_sk"
      type    = "int"
      comment = "Household demographics SK"
    }
    columns {
      name    = "ss_addr_sk"
      type    = "int"
      comment = "Address SK"
    }
    columns {
      name    = "ss_store_sk"
      type    = "int"
      comment = "Store SK"
    }
    columns {
      name    = "ss_promo_sk"
      type    = "int"
      comment = "Promotion SK"
    }
    columns {
      name    = "ss_ticket_number"
      type    = "bigint"
      comment = "Ticket number"
    }
    columns {
      name    = "ss_quantity"
      type    = "int"
      comment = "Quantity"
    }
    columns {
      name    = "ss_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Wholesale cost"
    }
    columns {
      name    = "ss_list_price"
      type    = "decimal(7,2)"
      comment = "List price"
    }
    columns {
      name    = "ss_sales_price"
      type    = "decimal(7,2)"
      comment = "Sales price"
    }
    columns {
      name    = "ss_ext_discount_amt"
      type    = "decimal(7,2)"
      comment = "Extended discount amount"
    }
    columns {
      name    = "ss_ext_sales_price"
      type    = "decimal(7,2)"
      comment = "Extended sales price"
    }
    columns {
      name    = "ss_ext_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Extended wholesale cost"
    }
    columns {
      name    = "ss_ext_list_price"
      type    = "decimal(7,2)"
      comment = "Extended list price"
    }
    columns {
      name    = "ss_ext_tax"
      type    = "decimal(7,2)"
      comment = "Extended tax"
    }
    columns {
      name    = "ss_coupon_amt"
      type    = "decimal(7,2)"
      comment = "Coupon amount"
    }
    columns {
      name    = "ss_net_paid"
      type    = "decimal(7,2)"
      comment = "Net paid"
    }
    columns {
      name    = "ss_net_paid_inc_tax"
      type    = "decimal(7,2)"
      comment = "Net paid including tax"
    }
    columns {
      name    = "ss_net_profit"
      type    = "decimal(7,2)"
      comment = "Net profit"
    }
  }

  # Note: Glue catalog tables don't support tags directly
}