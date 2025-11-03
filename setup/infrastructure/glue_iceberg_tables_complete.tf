# ========================================
# COMPLETE TPC-DS GLUE ICEBERG TABLES
# ========================================
# This file creates ALL TPC-DS Glue-managed Iceberg tables
# to match the create_iceberg_glue.sql file
# ========================================

# Additional TPC-DS Dimension Tables

# Catalog Page Iceberg Table
resource "aws_glue_catalog_table" "catalog_page_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "catalog_page"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/catalog_page/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "catalog_page_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "cp_catalog_page_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "cp_catalog_page_id"
      type    = "string"
      comment = "Catalog page identifier"
    }
    columns {
      name    = "cp_start_date_sk"
      type    = "int"
      comment = "Start date SK"
    }
    columns {
      name    = "cp_end_date_sk"
      type    = "int"
      comment = "End date SK"
    }
    columns {
      name    = "cp_department"
      type    = "string"
      comment = "Department"
    }
    columns {
      name    = "cp_catalog_number"
      type    = "int"
      comment = "Catalog number"
    }
    columns {
      name    = "cp_catalog_page_number"
      type    = "int"
      comment = "Catalog page number"
    }
    columns {
      name    = "cp_description"
      type    = "string"
      comment = "Description"
    }
    columns {
      name    = "cp_type"
      type    = "string"
      comment = "Type"
    }
  }
}

# Customer Address Iceberg Table
resource "aws_glue_catalog_table" "customer_address_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "customer_address"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/customer_address/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "customer_address_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "ca_address_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "ca_address_id"
      type    = "string"
      comment = "Address identifier"
    }
    columns {
      name    = "ca_street_number"
      type    = "string"
      comment = "Street number"
    }
    columns {
      name    = "ca_street_name"
      type    = "string"
      comment = "Street name"
    }
    columns {
      name    = "ca_street_type"
      type    = "string"
      comment = "Street type"
    }
    columns {
      name    = "ca_suite_number"
      type    = "string"
      comment = "Suite number"
    }
    columns {
      name    = "ca_city"
      type    = "string"
      comment = "City"
    }
    columns {
      name    = "ca_county"
      type    = "string"
      comment = "County"
    }
    columns {
      name    = "ca_state"
      type    = "string"
      comment = "State"
    }
    columns {
      name    = "ca_zip"
      type    = "string"
      comment = "ZIP code"
    }
    columns {
      name    = "ca_country"
      type    = "string"
      comment = "Country"
    }
    columns {
      name    = "ca_gmt_offset"
      type    = "double"
      comment = "GMT offset"
    }
    columns {
      name    = "ca_location_type"
      type    = "string"
      comment = "Location type"
    }
  }
}

# Customer Demographics Iceberg Table
resource "aws_glue_catalog_table" "customer_demographics_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "customer_demographics"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/customer_demographics/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "customer_demographics_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "cd_demo_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "cd_gender"
      type    = "string"
      comment = "Gender"
    }
    columns {
      name    = "cd_marital_status"
      type    = "string"
      comment = "Marital status"
    }
    columns {
      name    = "cd_education_status"
      type    = "string"
      comment = "Education status"
    }
    columns {
      name    = "cd_purchase_estimate"
      type    = "int"
      comment = "Purchase estimate"
    }
    columns {
      name    = "cd_credit_rating"
      type    = "string"
      comment = "Credit rating"
    }
    columns {
      name    = "cd_dep_count"
      type    = "int"
      comment = "Dependent count"
    }
    columns {
      name    = "cd_dep_employed_count"
      type    = "int"
      comment = "Dependent employed count"
    }
    columns {
      name    = "cd_dep_college_count"
      type    = "int"
      comment = "Dependent college count"
    }
  }
}

# Date Dimension Iceberg Table
resource "aws_glue_catalog_table" "date_dim_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "date_dim"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/date_dim/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "date_dim_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "d_date_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "d_date_id"
      type    = "string"
      comment = "Date identifier"
    }
    columns {
      name    = "d_date"
      type    = "date"
      comment = "Date"
    }
    columns {
      name    = "d_month_seq"
      type    = "int"
      comment = "Month sequence"
    }
    columns {
      name    = "d_week_seq"
      type    = "int"
      comment = "Week sequence"
    }
    columns {
      name    = "d_quarter_seq"
      type    = "int"
      comment = "Quarter sequence"
    }
    columns {
      name    = "d_year"
      type    = "int"
      comment = "Year"
    }
    columns {
      name    = "d_dow"
      type    = "int"
      comment = "Day of week"
    }
    columns {
      name    = "d_moy"
      type    = "int"
      comment = "Month of year"
    }
    columns {
      name    = "d_dom"
      type    = "int"
      comment = "Day of month"
    }
    columns {
      name    = "d_qoy"
      type    = "int"
      comment = "Quarter of year"
    }
    columns {
      name    = "d_fy_year"
      type    = "int"
      comment = "Fiscal year"
    }
    columns {
      name    = "d_fy_quarter_seq"
      type    = "int"
      comment = "Fiscal quarter sequence"
    }
    columns {
      name    = "d_fy_week_seq"
      type    = "int"
      comment = "Fiscal week sequence"
    }
    columns {
      name    = "d_day_name"
      type    = "string"
      comment = "Day name"
    }
    columns {
      name    = "d_quarter_name"
      type    = "string"
      comment = "Quarter name"
    }
    columns {
      name    = "d_holiday"
      type    = "string"
      comment = "Holiday"
    }
    columns {
      name    = "d_weekend"
      type    = "string"
      comment = "Weekend"
    }
    columns {
      name    = "d_following_holiday"
      type    = "string"
      comment = "Following holiday"
    }
    columns {
      name    = "d_first_dom"
      type    = "int"
      comment = "First day of month"
    }
    columns {
      name    = "d_last_dom"
      type    = "int"
      comment = "Last day of month"
    }
    columns {
      name    = "d_same_day_ly"
      type    = "int"
      comment = "Same day last year"
    }
    columns {
      name    = "d_same_day_lq"
      type    = "int"
      comment = "Same day last quarter"
    }
    columns {
      name    = "d_current_day"
      type    = "string"
      comment = "Current day"
    }
    columns {
      name    = "d_current_week"
      type    = "string"
      comment = "Current week"
    }
    columns {
      name    = "d_current_month"
      type    = "string"
      comment = "Current month"
    }
    columns {
      name    = "d_current_quarter"
      type    = "string"
      comment = "Current quarter"
    }
    columns {
      name    = "d_current_year"
      type    = "string"
      comment = "Current year"
    }
  }
}

# Household Demographics Iceberg Table
resource "aws_glue_catalog_table" "household_demographics_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "household_demographics"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/household_demographics/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "household_demographics_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "hd_demo_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "hd_income_band_sk"
      type    = "int"
      comment = "Income band SK"
    }
    columns {
      name    = "hd_buy_potential"
      type    = "string"
      comment = "Buy potential"
    }
    columns {
      name    = "hd_dep_count"
      type    = "int"
      comment = "Dependent count"
    }
    columns {
      name    = "hd_vehicle_count"
      type    = "int"
      comment = "Vehicle count"
    }
  }
}

# Income Band Iceberg Table
resource "aws_glue_catalog_table" "income_band_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "income_band"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/income_band/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "income_band_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "ib_income_band_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "ib_lower_bound"
      type    = "int"
      comment = "Lower bound"
    }
    columns {
      name    = "ib_upper_bound"
      type    = "int"
      comment = "Upper bound"
    }
  }
}

# Promotion Iceberg Table
resource "aws_glue_catalog_table" "promotion_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "promotion"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/promotion/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "promotion_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "p_promo_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "p_promo_id"
      type    = "string"
      comment = "Promotion identifier"
    }
    columns {
      name    = "p_start_date_sk"
      type    = "int"
      comment = "Start date SK"
    }
    columns {
      name    = "p_end_date_sk"
      type    = "int"
      comment = "End date SK"
    }
    columns {
      name    = "p_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "p_cost"
      type    = "decimal(15,2)"
      comment = "Cost"
    }
    columns {
      name    = "p_response_target"
      type    = "int"
      comment = "Response target"
    }
    columns {
      name    = "p_promo_name"
      type    = "string"
      comment = "Promotion name"
    }
    columns {
      name    = "p_channel_dmail"
      type    = "string"
      comment = "Channel direct mail"
    }
    columns {
      name    = "p_channel_email"
      type    = "string"
      comment = "Channel email"
    }
    columns {
      name    = "p_channel_catalog"
      type    = "string"
      comment = "Channel catalog"
    }
    columns {
      name    = "p_channel_tv"
      type    = "string"
      comment = "Channel TV"
    }
    columns {
      name    = "p_channel_radio"
      type    = "string"
      comment = "Channel radio"
    }
    columns {
      name    = "p_channel_press"
      type    = "string"
      comment = "Channel press"
    }
    columns {
      name    = "p_channel_event"
      type    = "string"
      comment = "Channel event"
    }
    columns {
      name    = "p_channel_demo"
      type    = "string"
      comment = "Channel demo"
    }
    columns {
      name    = "p_channel_details"
      type    = "string"
      comment = "Channel details"
    }
    columns {
      name    = "p_purpose"
      type    = "string"
      comment = "Purpose"
    }
    columns {
      name    = "p_discount_active"
      type    = "string"
      comment = "Discount active"
    }
  }
}

# Reason Iceberg Table
resource "aws_glue_catalog_table" "reason_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "reason"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/reason/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "reason_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "r_reason_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "r_reason_id"
      type    = "string"
      comment = "Reason identifier"
    }
    columns {
      name    = "r_reason_desc"
      type    = "string"
      comment = "Reason description"
    }
  }
}

# Ship Mode Iceberg Table
resource "aws_glue_catalog_table" "ship_mode_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "ship_mode"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/ship_mode/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ship_mode_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "sm_ship_mode_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "sm_ship_mode_id"
      type    = "string"
      comment = "Ship mode identifier"
    }
    columns {
      name    = "sm_type"
      type    = "string"
      comment = "Type"
    }
    columns {
      name    = "sm_code"
      type    = "string"
      comment = "Code"
    }
    columns {
      name    = "sm_carrier"
      type    = "string"
      comment = "Carrier"
    }
    columns {
      name    = "sm_contract"
      type    = "string"
      comment = "Contract"
    }
  }
}

# Store Iceberg Table
resource "aws_glue_catalog_table" "store_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "store"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/store/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "store_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "s_store_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "s_store_id"
      type    = "string"
      comment = "Store identifier"
    }
    columns {
      name    = "s_rec_start_date"
      type    = "date"
      comment = "Record start date"
    }
    columns {
      name    = "s_rec_end_date"
      type    = "date"
      comment = "Record end date"
    }
    columns {
      name    = "s_closed_date_sk"
      type    = "int"
      comment = "Closed date SK"
    }
    columns {
      name    = "s_store_name"
      type    = "string"
      comment = "Store name"
    }
    columns {
      name    = "s_number_employees"
      type    = "int"
      comment = "Number of employees"
    }
    columns {
      name    = "s_floor_space"
      type    = "int"
      comment = "Floor space"
    }
    columns {
      name    = "s_hours"
      type    = "string"
      comment = "Hours"
    }
    columns {
      name    = "s_manager"
      type    = "string"
      comment = "Manager"
    }
    columns {
      name    = "s_market_id"
      type    = "int"
      comment = "Market ID"
    }
    columns {
      name    = "s_geography_class"
      type    = "string"
      comment = "Geography class"
    }
    columns {
      name    = "s_market_desc"
      type    = "string"
      comment = "Market description"
    }
    columns {
      name    = "s_market_manager"
      type    = "string"
      comment = "Market manager"
    }
    columns {
      name    = "s_division_id"
      type    = "int"
      comment = "Division ID"
    }
    columns {
      name    = "s_division_name"
      type    = "string"
      comment = "Division name"
    }
    columns {
      name    = "s_company_id"
      type    = "int"
      comment = "Company ID"
    }
    columns {
      name    = "s_company_name"
      type    = "string"
      comment = "Company name"
    }
    columns {
      name    = "s_street_number"
      type    = "string"
      comment = "Street number"
    }
    columns {
      name    = "s_street_name"
      type    = "string"
      comment = "Street name"
    }
    columns {
      name    = "s_street_type"
      type    = "string"
      comment = "Street type"
    }
    columns {
      name    = "s_suite_number"
      type    = "string"
      comment = "Suite number"
    }
    columns {
      name    = "s_city"
      type    = "string"
      comment = "City"
    }
    columns {
      name    = "s_county"
      type    = "string"
      comment = "County"
    }
    columns {
      name    = "s_state"
      type    = "string"
      comment = "State"
    }
    columns {
      name    = "s_zip"
      type    = "string"
      comment = "ZIP code"
    }
    columns {
      name    = "s_country"
      type    = "string"
      comment = "Country"
    }
    columns {
      name    = "s_gmt_offset"
      type    = "double"
      comment = "GMT offset"
    }
    columns {
      name    = "s_tax_precentage"
      type    = "double"
      comment = "Tax percentage"
    }
  }
}

# Time Dimension Iceberg Table
resource "aws_glue_catalog_table" "time_dim_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "time_dim"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/time_dim/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "time_dim_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "t_time_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "t_time_id"
      type    = "string"
      comment = "Time identifier"
    }
    columns {
      name    = "t_time"
      type    = "int"
      comment = "Time"
    }
    columns {
      name    = "t_hour"
      type    = "int"
      comment = "Hour"
    }
    columns {
      name    = "t_minute"
      type    = "int"
      comment = "Minute"
    }
    columns {
      name    = "t_second"
      type    = "int"
      comment = "Second"
    }
    columns {
      name    = "t_am_pm"
      type    = "string"
      comment = "AM/PM"
    }
    columns {
      name    = "t_shift"
      type    = "string"
      comment = "Shift"
    }
    columns {
      name    = "t_sub_shift"
      type    = "string"
      comment = "Sub shift"
    }
    columns {
      name    = "t_meal_time"
      type    = "string"
      comment = "Meal time"
    }
  }
}

# Warehouse Iceberg Table
resource "aws_glue_catalog_table" "warehouse_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "warehouse"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/warehouse/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "warehouse_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "w_warehouse_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "w_warehouse_id"
      type    = "string"
      comment = "Warehouse identifier"
    }
    columns {
      name    = "w_warehouse_name"
      type    = "string"
      comment = "Warehouse name"
    }
    columns {
      name    = "w_warehouse_sq_ft"
      type    = "int"
      comment = "Warehouse square feet"
    }
    columns {
      name    = "w_street_number"
      type    = "string"
      comment = "Street number"
    }
    columns {
      name    = "w_street_name"
      type    = "string"
      comment = "Street name"
    }
    columns {
      name    = "w_street_type"
      type    = "string"
      comment = "Street type"
    }
    columns {
      name    = "w_suite_number"
      type    = "string"
      comment = "Suite number"
    }
    columns {
      name    = "w_city"
      type    = "string"
      comment = "City"
    }
    columns {
      name    = "w_county"
      type    = "string"
      comment = "County"
    }
    columns {
      name    = "w_state"
      type    = "string"
      comment = "State"
    }
    columns {
      name    = "w_zip"
      type    = "string"
      comment = "ZIP code"
    }
    columns {
      name    = "w_country"
      type    = "string"
      comment = "Country"
    }
    columns {
      name    = "w_gmt_offset"
      type    = "double"
      comment = "GMT offset"
    }
  }
}

# Web Page Iceberg Table
resource "aws_glue_catalog_table" "web_page_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "web_page"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/web_page/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "web_page_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "wp_web_page_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "wp_web_page_id"
      type    = "string"
      comment = "Web page identifier"
    }
    columns {
      name    = "wp_rec_start_date"
      type    = "date"
      comment = "Record start date"
    }
    columns {
      name    = "wp_rec_end_date"
      type    = "date"
      comment = "Record end date"
    }
    columns {
      name    = "wp_creation_date_sk"
      type    = "int"
      comment = "Creation date SK"
    }
    columns {
      name    = "wp_access_date_sk"
      type    = "int"
      comment = "Access date SK"
    }
    columns {
      name    = "wp_autogen_flag"
      type    = "string"
      comment = "Auto generated flag"
    }
    columns {
      name    = "wp_customer_sk"
      type    = "int"
      comment = "Customer SK"
    }
    columns {
      name    = "wp_url"
      type    = "string"
      comment = "URL"
    }
    columns {
      name    = "wp_type"
      type    = "string"
      comment = "Type"
    }
    columns {
      name    = "wp_char_count"
      type    = "int"
      comment = "Character count"
    }
    columns {
      name    = "wp_link_count"
      type    = "int"
      comment = "Link count"
    }
    columns {
      name    = "wp_image_count"
      type    = "int"
      comment = "Image count"
    }
    columns {
      name    = "wp_max_ad_count"
      type    = "int"
      comment = "Max ad count"
    }
  }
}

# Web Site Iceberg Table
resource "aws_glue_catalog_table" "web_site_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "web_site"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/web_site/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "web_site_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "web_site_sk"
      type    = "int"
      comment = "Primary key"
    }
    columns {
      name    = "web_site_id"
      type    = "string"
      comment = "Web site identifier"
    }
    columns {
      name    = "web_rec_start_date"
      type    = "date"
      comment = "Record start date"
    }
    columns {
      name    = "web_rec_end_date"
      type    = "date"
      comment = "Record end date"
    }
    columns {
      name    = "web_name"
      type    = "string"
      comment = "Web name"
    }
    columns {
      name    = "web_open_date_sk"
      type    = "int"
      comment = "Open date SK"
    }
    columns {
      name    = "web_close_date_sk"
      type    = "int"
      comment = "Close date SK"
    }
    columns {
      name    = "web_class"
      type    = "string"
      comment = "Web class"
    }
    columns {
      name    = "web_manager"
      type    = "string"
      comment = "Web manager"
    }
    columns {
      name    = "web_mkt_id"
      type    = "int"
      comment = "Market ID"
    }
    columns {
      name    = "web_mkt_class"
      type    = "string"
      comment = "Market class"
    }
    columns {
      name    = "web_mkt_desc"
      type    = "string"
      comment = "Market description"
    }
    columns {
      name    = "web_market_manager"
      type    = "string"
      comment = "Market manager"
    }
    columns {
      name    = "web_company_id"
      type    = "int"
      comment = "Company ID"
    }
    columns {
      name    = "web_company_name"
      type    = "string"
      comment = "Company name"
    }
    columns {
      name    = "web_street_number"
      type    = "string"
      comment = "Street number"
    }
    columns {
      name    = "web_street_name"
      type    = "string"
      comment = "Street name"
    }
    columns {
      name    = "web_street_type"
      type    = "string"
      comment = "Street type"
    }
    columns {
      name    = "web_suite_number"
      type    = "string"
      comment = "Suite number"
    }
    columns {
      name    = "web_city"
      type    = "string"
      comment = "City"
    }
    columns {
      name    = "web_county"
      type    = "string"
      comment = "County"
    }
    columns {
      name    = "web_state"
      type    = "string"
      comment = "State"
    }
    columns {
      name    = "web_zip"
      type    = "string"
      comment = "ZIP code"
    }
    columns {
      name    = "web_country"
      type    = "string"
      comment = "Country"
    }
    columns {
      name    = "web_gmt_offset"
      type    = "double"
      comment = "GMT offset"
    }
    columns {
      name    = "web_tax_percentage"
      type    = "double"
      comment = "Tax percentage"
    }
  }
}

# ========================================
# FACT TABLES - ADDITIONAL TPC-DS TABLES
# ========================================

# Catalog Returns Iceberg Table
resource "aws_glue_catalog_table" "catalog_returns_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "catalog_returns"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/catalog_returns/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "catalog_returns_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "cr_returned_date_sk"
      type    = "int"
      comment = "Returned date SK"
    }
    columns {
      name    = "cr_returned_time_sk"
      type    = "int"
      comment = "Returned time SK"
    }
    columns {
      name    = "cr_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "cr_refunded_customer_sk"
      type    = "int"
      comment = "Refunded customer SK"
    }
    columns {
      name    = "cr_refunded_cdemo_sk"
      type    = "int"
      comment = "Refunded customer demographics SK"
    }
    columns {
      name    = "cr_refunded_hdemo_sk"
      type    = "int"
      comment = "Refunded household demographics SK"
    }
    columns {
      name    = "cr_refunded_addr_sk"
      type    = "int"
      comment = "Refunded address SK"
    }
    columns {
      name    = "cr_returning_customer_sk"
      type    = "int"
      comment = "Returning customer SK"
    }
    columns {
      name    = "cr_returning_cdemo_sk"
      type    = "int"
      comment = "Returning customer demographics SK"
    }
    columns {
      name    = "cr_returning_hdemo_sk"
      type    = "int"
      comment = "Returning household demographics SK"
    }
    columns {
      name    = "cr_returning_addr_sk"
      type    = "int"
      comment = "Returning address SK"
    }
    columns {
      name    = "cr_call_center_sk"
      type    = "int"
      comment = "Call center SK"
    }
    columns {
      name    = "cr_catalog_page_sk"
      type    = "int"
      comment = "Catalog page SK"
    }
    columns {
      name    = "cr_ship_mode_sk"
      type    = "int"
      comment = "Ship mode SK"
    }
    columns {
      name    = "cr_warehouse_sk"
      type    = "int"
      comment = "Warehouse SK"
    }
    columns {
      name    = "cr_reason_sk"
      type    = "int"
      comment = "Reason SK"
    }
    columns {
      name    = "cr_order_number"
      type    = "bigint"
      comment = "Order number"
    }
    columns {
      name    = "cr_return_quantity"
      type    = "int"
      comment = "Return quantity"
    }
    columns {
      name    = "cr_return_amount"
      type    = "decimal(7,2)"
      comment = "Return amount"
    }
    columns {
      name    = "cr_return_tax"
      type    = "decimal(7,2)"
      comment = "Return tax"
    }
    columns {
      name    = "cr_return_amt_inc_tax"
      type    = "decimal(7,2)"
      comment = "Return amount including tax"
    }
    columns {
      name    = "cr_fee"
      type    = "decimal(7,2)"
      comment = "Fee"
    }
    columns {
      name    = "cr_return_ship_cost"
      type    = "decimal(7,2)"
      comment = "Return ship cost"
    }
    columns {
      name    = "cr_refunded_cash"
      type    = "decimal(7,2)"
      comment = "Refunded cash"
    }
    columns {
      name    = "cr_reversed_charge"
      type    = "decimal(7,2)"
      comment = "Reversed charge"
    }
    columns {
      name    = "cr_store_credit"
      type    = "decimal(7,2)"
      comment = "Store credit"
    }
    columns {
      name    = "cr_net_loss"
      type    = "decimal(7,2)"
      comment = "Net loss"
    }
  }
}

# Catalog Sales Iceberg Table
resource "aws_glue_catalog_table" "catalog_sales_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "catalog_sales"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/catalog_sales/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "catalog_sales_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "cs_sold_date_sk"
      type    = "int"
      comment = "Sold date SK"
    }
    columns {
      name    = "cs_sold_time_sk"
      type    = "int"
      comment = "Sold time SK"
    }
    columns {
      name    = "cs_ship_date_sk"
      type    = "int"
      comment = "Ship date SK"
    }
    columns {
      name    = "cs_bill_customer_sk"
      type    = "int"
      comment = "Bill customer SK"
    }
    columns {
      name    = "cs_bill_cdemo_sk"
      type    = "int"
      comment = "Bill customer demographics SK"
    }
    columns {
      name    = "cs_bill_hdemo_sk"
      type    = "int"
      comment = "Bill household demographics SK"
    }
    columns {
      name    = "cs_bill_addr_sk"
      type    = "int"
      comment = "Bill address SK"
    }
    columns {
      name    = "cs_ship_customer_sk"
      type    = "int"
      comment = "Ship customer SK"
    }
    columns {
      name    = "cs_ship_cdemo_sk"
      type    = "int"
      comment = "Ship customer demographics SK"
    }
    columns {
      name    = "cs_ship_hdemo_sk"
      type    = "int"
      comment = "Ship household demographics SK"
    }
    columns {
      name    = "cs_ship_addr_sk"
      type    = "int"
      comment = "Ship address SK"
    }
    columns {
      name    = "cs_call_center_sk"
      type    = "int"
      comment = "Call center SK"
    }
    columns {
      name    = "cs_catalog_page_sk"
      type    = "int"
      comment = "Catalog page SK"
    }
    columns {
      name    = "cs_ship_mode_sk"
      type    = "int"
      comment = "Ship mode SK"
    }
    columns {
      name    = "cs_warehouse_sk"
      type    = "int"
      comment = "Warehouse SK"
    }
    columns {
      name    = "cs_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "cs_promo_sk"
      type    = "int"
      comment = "Promotion SK"
    }
    columns {
      name    = "cs_order_number"
      type    = "bigint"
      comment = "Order number"
    }
    columns {
      name    = "cs_quantity"
      type    = "int"
      comment = "Quantity"
    }
    columns {
      name    = "cs_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Wholesale cost"
    }
    columns {
      name    = "cs_list_price"
      type    = "decimal(7,2)"
      comment = "List price"
    }
    columns {
      name    = "cs_sales_price"
      type    = "decimal(7,2)"
      comment = "Sales price"
    }
    columns {
      name    = "cs_ext_discount_amt"
      type    = "decimal(7,2)"
      comment = "Extended discount amount"
    }
    columns {
      name    = "cs_ext_sales_price"
      type    = "decimal(7,2)"
      comment = "Extended sales price"
    }
    columns {
      name    = "cs_ext_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Extended wholesale cost"
    }
    columns {
      name    = "cs_ext_list_price"
      type    = "decimal(7,2)"
      comment = "Extended list price"
    }
    columns {
      name    = "cs_ext_tax"
      type    = "decimal(7,2)"
      comment = "Extended tax"
    }
    columns {
      name    = "cs_coupon_amt"
      type    = "decimal(7,2)"
      comment = "Coupon amount"
    }
    columns {
      name    = "cs_ext_ship_cost"
      type    = "decimal(7,2)"
      comment = "Extended ship cost"
    }
    columns {
      name    = "cs_net_paid"
      type    = "decimal(7,2)"
      comment = "Net paid"
    }
    columns {
      name    = "cs_net_paid_inc_tax"
      type    = "decimal(7,2)"
      comment = "Net paid including tax"
    }
    columns {
      name    = "cs_net_paid_inc_ship"
      type    = "decimal(7,2)"
      comment = "Net paid including ship"
    }
    columns {
      name    = "cs_net_paid_inc_ship_tax"
      type    = "decimal(7,2)"
      comment = "Net paid including ship and tax"
    }
    columns {
      name    = "cs_net_profit"
      type    = "decimal(7,2)"
      comment = "Net profit"
    }
  }
}

# Inventory Iceberg Table
resource "aws_glue_catalog_table" "inventory_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "inventory"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/inventory/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "inventory_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "inv_date_sk"
      type    = "int"
      comment = "Date SK"
    }
    columns {
      name    = "inv_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "inv_warehouse_sk"
      type    = "int"
      comment = "Warehouse SK"
    }
    columns {
      name    = "inv_quantity_on_hand"
      type    = "int"
      comment = "Quantity on hand"
    }
  }
}

# Store Returns Iceberg Table
resource "aws_glue_catalog_table" "store_returns_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "store_returns"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/store_returns/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "store_returns_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "sr_returned_date_sk"
      type    = "int"
      comment = "Returned date SK"
    }
    columns {
      name    = "sr_return_time_sk"
      type    = "int"
      comment = "Return time SK"
    }
    columns {
      name    = "sr_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "sr_customer_sk"
      type    = "int"
      comment = "Customer SK"
    }
    columns {
      name    = "sr_cdemo_sk"
      type    = "int"
      comment = "Customer demographics SK"
    }
    columns {
      name    = "sr_hdemo_sk"
      type    = "int"
      comment = "Household demographics SK"
    }
    columns {
      name    = "sr_addr_sk"
      type    = "int"
      comment = "Address SK"
    }
    columns {
      name    = "sr_store_sk"
      type    = "int"
      comment = "Store SK"
    }
    columns {
      name    = "sr_reason_sk"
      type    = "int"
      comment = "Reason SK"
    }
    columns {
      name    = "sr_ticket_number"
      type    = "bigint"
      comment = "Ticket number"
    }
    columns {
      name    = "sr_return_quantity"
      type    = "int"
      comment = "Return quantity"
    }
    columns {
      name    = "sr_return_amt"
      type    = "decimal(7,2)"
      comment = "Return amount"
    }
    columns {
      name    = "sr_return_tax"
      type    = "decimal(7,2)"
      comment = "Return tax"
    }
    columns {
      name    = "sr_return_amt_inc_tax"
      type    = "decimal(7,2)"
      comment = "Return amount including tax"
    }
    columns {
      name    = "sr_fee"
      type    = "decimal(7,2)"
      comment = "Fee"
    }
    columns {
      name    = "sr_return_ship_cost"
      type    = "decimal(7,2)"
      comment = "Return ship cost"
    }
    columns {
      name    = "sr_refunded_cash"
      type    = "decimal(7,2)"
      comment = "Refunded cash"
    }
    columns {
      name    = "sr_reversed_charge"
      type    = "decimal(7,2)"
      comment = "Reversed charge"
    }
    columns {
      name    = "sr_store_credit"
      type    = "decimal(7,2)"
      comment = "Store credit"
    }
    columns {
      name    = "sr_net_loss"
      type    = "decimal(7,2)"
      comment = "Net loss"
    }
  }
}

# Web Returns Iceberg Table
resource "aws_glue_catalog_table" "web_returns_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "web_returns"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/web_returns/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "web_returns_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "wr_returned_date_sk"
      type    = "int"
      comment = "Returned date SK"
    }
    columns {
      name    = "wr_returned_time_sk"
      type    = "int"
      comment = "Returned time SK"
    }
    columns {
      name    = "wr_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "wr_refunded_customer_sk"
      type    = "int"
      comment = "Refunded customer SK"
    }
    columns {
      name    = "wr_refunded_cdemo_sk"
      type    = "int"
      comment = "Refunded customer demographics SK"
    }
    columns {
      name    = "wr_refunded_hdemo_sk"
      type    = "int"
      comment = "Refunded household demographics SK"
    }
    columns {
      name    = "wr_refunded_addr_sk"
      type    = "int"
      comment = "Refunded address SK"
    }
    columns {
      name    = "wr_returning_customer_sk"
      type    = "int"
      comment = "Returning customer SK"
    }
    columns {
      name    = "wr_returning_cdemo_sk"
      type    = "int"
      comment = "Returning customer demographics SK"
    }
    columns {
      name    = "wr_returning_hdemo_sk"
      type    = "int"
      comment = "Returning household demographics SK"
    }
    columns {
      name    = "wr_returning_addr_sk"
      type    = "int"
      comment = "Returning address SK"
    }
    columns {
      name    = "wr_web_page_sk"
      type    = "int"
      comment = "Web page SK"
    }
    columns {
      name    = "wr_reason_sk"
      type    = "int"
      comment = "Reason SK"
    }
    columns {
      name    = "wr_order_number"
      type    = "bigint"
      comment = "Order number"
    }
    columns {
      name    = "wr_return_quantity"
      type    = "int"
      comment = "Return quantity"
    }
    columns {
      name    = "wr_return_amt"
      type    = "decimal(7,2)"
      comment = "Return amount"
    }
    columns {
      name    = "wr_return_tax"
      type    = "decimal(7,2)"
      comment = "Return tax"
    }
    columns {
      name    = "wr_return_amt_inc_tax"
      type    = "decimal(7,2)"
      comment = "Return amount including tax"
    }
    columns {
      name    = "wr_fee"
      type    = "decimal(7,2)"
      comment = "Fee"
    }
    columns {
      name    = "wr_return_ship_cost"
      type    = "decimal(7,2)"
      comment = "Return ship cost"
    }
    columns {
      name    = "wr_refunded_cash"
      type    = "decimal(7,2)"
      comment = "Refunded cash"
    }
    columns {
      name    = "wr_reversed_charge"
      type    = "decimal(7,2)"
      comment = "Reversed charge"
    }
    columns {
      name    = "wr_account_credit"
      type    = "decimal(7,2)"
      comment = "Account credit"
    }
    columns {
      name    = "wr_net_loss"
      type    = "decimal(7,2)"
      comment = "Net loss"
    }
  }
}

# Web Sales Iceberg Table
resource "aws_glue_catalog_table" "web_sales_iceberg" {
  count         = var.enable_glue_tables ? 1 : 0
  name          = "web_sales"
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
    location      = "s3://${aws_s3_bucket.iceberg_data.bucket}/${var.iceberg_table_prefix}/web_sales/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "web_sales_serde"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name    = "ws_sold_date_sk"
      type    = "int"
      comment = "Sold date SK"
    }
    columns {
      name    = "ws_sold_time_sk"
      type    = "int"
      comment = "Sold time SK"
    }
    columns {
      name    = "ws_ship_date_sk"
      type    = "int"
      comment = "Ship date SK"
    }
    columns {
      name    = "ws_item_sk"
      type    = "int"
      comment = "Item SK"
    }
    columns {
      name    = "ws_bill_customer_sk"
      type    = "int"
      comment = "Bill customer SK"
    }
    columns {
      name    = "ws_bill_cdemo_sk"
      type    = "int"
      comment = "Bill customer demographics SK"
    }
    columns {
      name    = "ws_bill_hdemo_sk"
      type    = "int"
      comment = "Bill household demographics SK"
    }
    columns {
      name    = "ws_bill_addr_sk"
      type    = "int"
      comment = "Bill address SK"
    }
    columns {
      name    = "ws_ship_customer_sk"
      type    = "int"
      comment = "Ship customer SK"
    }
    columns {
      name    = "ws_ship_cdemo_sk"
      type    = "int"
      comment = "Ship customer demographics SK"
    }
    columns {
      name    = "ws_ship_hdemo_sk"
      type    = "int"
      comment = "Ship household demographics SK"
    }
    columns {
      name    = "ws_ship_addr_sk"
      type    = "int"
      comment = "Ship address SK"
    }
    columns {
      name    = "ws_web_page_sk"
      type    = "int"
      comment = "Web page SK"
    }
    columns {
      name    = "ws_web_site_sk"
      type    = "int"
      comment = "Web site SK"
    }
    columns {
      name    = "ws_ship_mode_sk"
      type    = "int"
      comment = "Ship mode SK"
    }
    columns {
      name    = "ws_warehouse_sk"
      type    = "int"
      comment = "Warehouse SK"
    }
    columns {
      name    = "ws_promo_sk"
      type    = "int"
      comment = "Promotion SK"
    }
    columns {
      name    = "ws_order_number"
      type    = "bigint"
      comment = "Order number"
    }
    columns {
      name    = "ws_quantity"
      type    = "int"
      comment = "Quantity"
    }
    columns {
      name    = "ws_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Wholesale cost"
    }
    columns {
      name    = "ws_list_price"
      type    = "decimal(7,2)"
      comment = "List price"
    }
    columns {
      name    = "ws_sales_price"
      type    = "decimal(7,2)"
      comment = "Sales price"
    }
    columns {
      name    = "ws_ext_discount_amt"
      type    = "decimal(7,2)"
      comment = "Extended discount amount"
    }
    columns {
      name    = "ws_ext_sales_price"
      type    = "decimal(7,2)"
      comment = "Extended sales price"
    }
    columns {
      name    = "ws_ext_wholesale_cost"
      type    = "decimal(7,2)"
      comment = "Extended wholesale cost"
    }
    columns {
      name    = "ws_ext_list_price"
      type    = "decimal(7,2)"
      comment = "Extended list price"
    }
    columns {
      name    = "ws_ext_tax"
      type    = "decimal(7,2)"
      comment = "Extended tax"
    }
    columns {
      name    = "ws_coupon_amt"
      type    = "decimal(7,2)"
      comment = "Coupon amount"
    }
    columns {
      name    = "ws_ext_ship_cost"
      type    = "decimal(7,2)"
      comment = "Extended ship cost"
    }
    columns {
      name    = "ws_net_paid"
      type    = "decimal(7,2)"
      comment = "Net paid"
    }
    columns {
      name    = "ws_net_paid_inc_tax"
      type    = "decimal(7,2)"
      comment = "Net paid including tax"
    }
    columns {
      name    = "ws_net_paid_inc_ship"
      type    = "decimal(7,2)"
      comment = "Net paid including ship"
    }
    columns {
      name    = "ws_net_paid_inc_ship_tax"
      type    = "decimal(7,2)"
      comment = "Net paid including ship and tax"
    }
    columns {
      name    = "ws_net_profit"
      type    = "decimal(7,2)"
      comment = "Net profit"
    }
  }
}
