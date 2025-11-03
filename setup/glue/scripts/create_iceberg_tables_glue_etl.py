#!/usr/bin/env python3
"""
AWS Glue ETL Job to Create Iceberg Tables with Metadata
This script creates Glue-managed Iceberg tables using AWS Glue's native Iceberg support
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'GLUE_DATABASE',
    'S3_WAREHOUSE_PATH',
    'AWS_REGION'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 70)
print("CREATING ICEBERG TABLES WITH AWS GLUE ETL")
print("=" * 70)

# Configuration
glue_database = args['GLUE_DATABASE']
s3_warehouse_path = args['S3_WAREHOUSE_PATH']
aws_region = args.get('AWS_REGION', 'us-east-1')

print(f"Glue Database: {glue_database}")
print(f"S3 Warehouse Path: {s3_warehouse_path}")
print(f"AWS Region: {aws_region}")

# Configure Iceberg catalog for Glue
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", s3_warehouse_path)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Table schemas (TPC-DS dimension tables - simplified for initial test)
table_schemas = {
    'call_center': StructType([
        StructField("cc_call_center_sk", IntegerType(), False),
        StructField("cc_call_center_id", StringType(), False),
        StructField("cc_rec_start_date", DateType(), True),
        StructField("cc_rec_end_date", DateType(), True),
        StructField("cc_closed_date_sk", IntegerType(), True),
        StructField("cc_open_date_sk", IntegerType(), True),
        StructField("cc_name", StringType(), True),
        StructField("cc_class", StringType(), True),
        StructField("cc_employees", IntegerType(), True),
        StructField("cc_sq_ft", IntegerType(), True),
        StructField("cc_hours", StringType(), True),
        StructField("cc_manager", StringType(), True),
        StructField("cc_mkt_id", IntegerType(), True),
        StructField("cc_mkt_class", StringType(), True),
        StructField("cc_mkt_desc", StringType(), True),
        StructField("cc_market_manager", StringType(), True),
        StructField("cc_division", IntegerType(), True),
        StructField("cc_division_name", StringType(), True),
        StructField("cc_company", IntegerType(), True),
        StructField("cc_company_name", StringType(), True),
        StructField("cc_street_number", StringType(), True),
        StructField("cc_street_name", StringType(), True),
        StructField("cc_street_type", StringType(), True),
        StructField("cc_suite_number", StringType(), True),
        StructField("cc_city", StringType(), True),
        StructField("cc_county", StringType(), True),
        StructField("cc_state", StringType(), True),
        StructField("cc_zip", StringType(), True),
        StructField("cc_country", StringType(), True),
        StructField("cc_gmt_offset", DecimalType(5, 2), True),
        StructField("cc_tax_percentage", DecimalType(5, 2), True),
    ]),
    'customer': StructType([
        StructField("c_customer_sk", IntegerType(), False),
        StructField("c_customer_id", StringType(), False),
        StructField("c_current_cdemo_sk", IntegerType(), True),
        StructField("c_current_hdemo_sk", IntegerType(), True),
        StructField("c_current_addr_sk", IntegerType(), True),
        StructField("c_first_shipto_date_sk", IntegerType(), True),
        StructField("c_first_sales_date_sk", IntegerType(), True),
        StructField("c_salutation", StringType(), True),
        StructField("c_first_name", StringType(), True),
        StructField("c_last_name", StringType(), True),
        StructField("c_preferred_cust_flag", StringType(), True),
        StructField("c_birth_day", IntegerType(), True),
        StructField("c_birth_month", IntegerType(), True),
        StructField("c_birth_year", IntegerType(), True),
        StructField("c_birth_country", StringType(), True),
        StructField("c_login", StringType(), True),
        StructField("c_email_address", StringType(), True),
        StructField("c_last_review_date", StringType(), True),
    ]),
    'item': StructType([
        StructField("i_item_sk", IntegerType(), False),
        StructField("i_item_id", StringType(), False),
        StructField("i_rec_start_date", DateType(), True),
        StructField("i_rec_end_date", DateType(), True),
        StructField("i_item_desc", StringType(), True),
        StructField("i_current_price", DecimalType(7, 2), True),
        StructField("i_wholesale_cost", DecimalType(7, 2), True),
        StructField("i_brand_id", IntegerType(), True),
        StructField("i_brand", StringType(), True),
        StructField("i_class_id", IntegerType(), True),
        StructField("i_class", StringType(), True),
        StructField("i_category_id", IntegerType(), True),
        StructField("i_category", StringType(), True),
        StructField("i_manufact_id", IntegerType(), True),
        StructField("i_manufact", StringType(), True),
        StructField("i_size", StringType(), True),
        StructField("i_formulation", StringType(), True),
        StructField("i_color", StringType(), True),
        StructField("i_units", StringType(), True),
        StructField("i_container", StringType(), True),
        StructField("i_manager_id", IntegerType(), True),
        StructField("i_product_name", StringType(), True),
    ])
}

def create_iceberg_table(table_name, schema):
    """Create an Iceberg table using Glue catalog"""
    print(f"\nCreating Iceberg table: {table_name}")
    
    try:
        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], schema)
        
        # Full table name in Glue catalog
        full_table_name = f"glue_catalog.{glue_database}.{table_name}"
        
        print(f"  Table name: {full_table_name}")
        print(f"  Schema: {len(schema.fields)} columns")
        
        # Create Iceberg table with properties
        writer = empty_df.writeTo(full_table_name)
        writer = writer.tableProperty("write.format.default", "parquet")
        writer = writer.tableProperty("write.parquet.compression-codec", "snappy")
        writer = writer.tableProperty("write.target-file-size-bytes", "134217728")
        writer.create()
        
        # Get metadata location
        try:
            metadata_result = spark.sql(
                f"SHOW TBLPROPERTIES {full_table_name} ('metadata_location')"
            ).collect()
            if metadata_result:
                metadata_location = metadata_result[0][0]
                print(f"  ✅ Metadata location: {metadata_location}")
        except Exception as e:
            print(f"  ⚠️  Could not retrieve metadata location: {e}")
        
        print(f"  ✅ Successfully created Iceberg table: {table_name}")
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "TableExistsException" in error_msg:
            print(f"  ⚠️  Table already exists: {table_name}")
            return True
        else:
            print(f"  ❌ Failed to create table {table_name}: {e}")
            import traceback
            traceback.print_exc()
            return False

# Create tables
print(f"\nCreating {len(table_schemas)} Iceberg tables...")
print("=" * 70)

success_count = 0
failed_tables = []

for table_name, schema in table_schemas.items():
    print(f"\n[{success_count + len(failed_tables) + 1}/{len(table_schemas)}] Processing: {table_name}")
    
    if create_iceberg_table(table_name, schema):
        success_count += 1
    else:
        failed_tables.append(table_name)

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Successfully created: {success_count}/{len(table_schemas)} tables")

if failed_tables:
    print(f"❌ Failed tables: {len(failed_tables)}")
    for table in failed_tables:
        print(f"   - {table}")
    
if success_count == len(table_schemas):
    print("\n🎉 All Iceberg tables created successfully!")
    print("\nNext steps:")
    print("1. Verify tables in AWS Glue Console")
    print("2. Check S3 for metadata files")
    print("3. Test from Snowflake:")
    print(f"   SELECT * FROM AWS_GLUE_CATALOG.{glue_database}.call_center LIMIT 1;")

# Commit job
job.commit()

print("\n" + "=" * 70)

