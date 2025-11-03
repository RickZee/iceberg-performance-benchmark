#!/usr/bin/env python3
"""
Generate complete AWS Glue ETL job script for creating all TPC-DS Iceberg tables
This script reads the table schemas and generates a complete Glue ETL job
"""

import yaml
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
config_dir = project_root / "setup" / "glue" / "config"
output_file = project_root / "setup" / "glue" / "scripts" / "create_all_iceberg_tables_glue_etl.py"

def load_table_schemas():
    """Load table schemas from YAML"""
    with open(config_dir / "tpcds_table_schemas.yaml", 'r') as f:
        return yaml.safe_load(f)

def python_type_to_spark_type(col_type):
    """Convert Python type string to Spark type"""
    type_mapping = {
        "string": "StringType()",
        "int": "IntegerType()",
        "long": "LongType()",
        "double": "DoubleType()",
        "date": "DateType()",
    }
    
    if col_type in type_mapping:
        return type_mapping[col_type]
    elif col_type.startswith("decimal"):
        # Parse decimal(7,2)
        params = col_type.split("(")[1].split(")")[0].split(",")
        precision = params[0]
        scale = params[1] if len(params) > 1 else "2"
        return f"DecimalType({precision}, {scale})"
    else:
        return "StringType()"

def generate_schema_code(table_name, table_def):
    """Generate Spark StructType code for a table"""
    fields = []
    for col_def in table_def['columns']:
        col_name = col_def['name']
        col_type = python_type_to_spark_type(col_def['type'])
        nullable = "True" if col_def.get('nullable', True) else "False"
        fields.append(f'        StructField("{col_name}", {col_type}, {nullable})')
    
    schema_code = f"    '{table_name}': StructType([\n"
    schema_code += ",\n".join(fields)
    schema_code += "\n    ]),"
    return schema_code

def generate_glue_etl_script():
    """Generate complete Glue ETL job script"""
    schemas = load_table_schemas()
    
    script = '''#!/usr/bin/env python3
"""
AWS Glue ETL Job to Create All TPC-DS Iceberg Tables with Metadata
This script creates all 24 Glue-managed Iceberg tables using AWS Glue's native Iceberg support
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
print("CREATING ALL TPC-DS ICEBERG TABLES WITH AWS GLUE ETL")
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

# Table schemas for all 24 TPC-DS tables
table_schemas = {
'''
    
    # Generate schema code for all tables
    for table_name, table_def in schemas['tables'].items():
        schema_code = generate_schema_code(table_name, table_def)
        script += schema_code + "\n"
    
    script += '''}

def create_iceberg_table(table_name, schema):
    """Create an Iceberg table using Glue catalog"""
    print(f"\\nCreating Iceberg table: {table_name}")
    
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

# Create all tables
print(f"\\nCreating {len(table_schemas)} Iceberg tables...")
print("=" * 70)

success_count = 0
failed_tables = []

for table_name, schema in table_schemas.items():
    print(f"\\n[{success_count + len(failed_tables) + 1}/{len(table_schemas)}] Processing: {table_name}")
    
    if create_iceberg_table(table_name, schema):
        success_count += 1
    else:
        failed_tables.append(table_name)

# Summary
print("\\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Successfully created: {success_count}/{len(table_schemas)} tables")

if failed_tables:
    print(f"❌ Failed tables: {len(failed_tables)}")
    for table in failed_tables:
        print(f"   - {table}")
    
if success_count == len(table_schemas):
    print("\\n🎉 All Iceberg tables created successfully!")
    print("\\nNext steps:")
    print("1. Verify tables in AWS Glue Console")
    print("2. Check S3 for metadata files")
    print("3. Test from Snowflake:")
    print(f"   SELECT * FROM AWS_GLUE_CATALOG.{glue_database}.call_center LIMIT 1;")

# Commit job
job.commit()

print("\\n" + "=" * 70)
'''
    
    return script

if __name__ == "__main__":
    script_content = generate_glue_etl_script()
    with open(output_file, 'w') as f:
        f.write(script_content)
    print(f"✅ Generated Glue ETL job script: {output_file}")
    print(f"   Contains schemas for all TPC-DS tables")

