#!/usr/bin/env python3
"""
Extract table schemas from Terraform files and generate complete Glue ETL script
This script reads the Terraform Glue table definitions and generates a complete Glue ETL job
"""

import re
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
terraform_dir = project_root / "setup" / "infrastructure"
output_file = project_root / "setup" / "glue" / "scripts" / "create_all_iceberg_tables_glue_etl.py"

def extract_table_schemas_from_terraform():
    """Extract all table schemas from Terraform files"""
    tables = {}
    
    # Read both Terraform files
    for tf_file in ["glue_iceberg_tables.tf", "glue_iceberg_tables_complete.tf"]:
        tf_path = terraform_dir / tf_file
        if not tf_path.exists():
            print(f"Warning: {tf_file} not found")
            continue
            
        with open(tf_path, 'r') as f:
            lines = f.readlines()
        
        # Parse Terraform file line by line
        i = 0
        while i < len(lines):
            line = lines[i]
            
            # Look for table resource definition
            if 'resource "aws_glue_catalog_table"' in line:
                # Extract resource name
                resource_match = re.search(r'"([a-z_]+)_iceberg"', line)
                if not resource_match:
                    i += 1
                    continue
                
                # Find table name
                table_name = None
                columns = []
                i += 1
                
                # Read until we find the table name and columns
                while i < len(lines):
                    line = lines[i]
                    
                    # Check for table name
                    name_match = re.search(r'name\s+=\s+"([^"]+)"', line)
                    if name_match:
                        table_name = name_match.group(1)
                    
                    # Check for columns block start
                    if 'columns {' in line:
                        # Parse columns
                        i += 1
                        while i < len(lines):
                            col_line = lines[i]
                            
                            # Check if we've exited the columns block
                            if col_line.strip() == '}' and 'columns' not in col_line:
                                # Check if this closes the columns block
                                if i + 1 < len(lines) and lines[i+1].strip() == '}':
                                    break
                            
                            # Look for column definition
                            col_name_match = re.search(r'name\s+=\s+"([^"]+)"', col_line)
                            if col_name_match:
                                col_name = col_name_match.group(1)
                                
                                # Get type from next few lines
                                col_type = None
                                for j in range(i, min(i+5, len(lines))):
                                    type_match = re.search(r'type\s+=\s+"([^"]+)"', lines[j])
                                    if type_match:
                                        col_type = type_match.group(1)
                                        break
                                
                                if col_name and col_type:
                                    columns.append({
                                        'name': col_name,
                                        'type': col_type,
                                        'nullable': True
                                    })
                            
                            # Break if we hit the end of columns block
                            if col_line.strip() == '}' and i > 0 and 'columns' not in lines[i-1]:
                                # Check context
                                if 'columns' in ''.join(lines[max(0, i-10):i]):
                                    i += 1
                                    break
                            
                            i += 1
                    
                    # Break if we've found name and columns
                    if table_name and columns:
                        break
                    
                    # Break if we've moved to next resource
                    if i < len(lines) and 'resource "aws_glue_catalog_table"' in lines[i]:
                        break
                    
                    i += 1
                
                if table_name and columns:
                    tables[table_name] = columns
            
            i += 1
    
    return tables

def map_terraform_type_to_spark(tf_type):
    """Map Terraform/Hive type to Spark type string"""
    type_mapping = {
        'string': 'StringType()',
        'int': 'IntegerType()',
        'bigint': 'LongType()',
        'double': 'DoubleType()',
        'float': 'FloatType()',
        'boolean': 'BooleanType()',
        'date': 'DateType()',
        'timestamp': 'TimestampType()',
    }
    
    if tf_type in type_mapping:
        return type_mapping[tf_type]
    elif tf_type.startswith('decimal'):
        # Parse decimal(7,2)
        match = re.match(r'decimal\((\d+),(\d+)\)', tf_type)
        if match:
            precision, scale = match.groups()
            return f'DecimalType({precision}, {scale})'
        return 'DecimalType(10, 2)'
    else:
        return 'StringType()'

def generate_spark_schema_code(table_name, columns):
    """Generate Spark StructType code for a table"""
    fields = []
    for col in columns:
        col_name = col['name']
        col_type = map_terraform_type_to_spark(col['type'])
        nullable = "True" if col.get('nullable', True) else "False"
        fields.append(f'        StructField("{col_name}", {col_type}, {nullable})')
    
    schema_code = f"    '{table_name}': StructType([\n"
    schema_code += ",\n".join(fields)
    schema_code += "\n    ]),"
    return schema_code

def generate_glue_etl_script(tables):
    """Generate complete Glue ETL job script"""
    
    script = '''#!/usr/bin/env python3
"""
AWS Glue ETL Job to Create All TPC-DS Iceberg Tables with Metadata
This script creates all 24 Glue-managed Iceberg tables using AWS Glue's native Iceberg support
Generated from Terraform table definitions
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
# Generated from Terraform definitions
table_schemas = {
'''
    
    # Generate schema code for all tables
    for table_name in sorted(tables.keys()):
        columns = tables[table_name]
        schema_code = generate_spark_schema_code(table_name, columns)
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
        if "already exists" in error_msg.lower() or "TableExistsException" in error_msg or "TABLE_ALREADY_EXISTS" in error_msg:
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

for table_name, schema in sorted(table_schemas.items()):
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

def main():
    print("Extracting table schemas from Terraform files...")
    tables = extract_table_schemas_from_terraform()
    
    print(f"Found {len(tables)} tables:")
    for table_name in sorted(tables.keys()):
        print(f"  - {table_name} ({len(tables[table_name])} columns)")
    
    if not tables:
        print("❌ No tables found in Terraform files!")
        print("   Trying alternative approach...")
        # Fallback: use a simpler manual approach
        return 1
    
    print("\nGenerating Glue ETL job script...")
    script_content = generate_glue_etl_script(tables)
    
    with open(output_file, 'w') as f:
        f.write(script_content)
    
    print(f"✅ Generated Glue ETL job script: {output_file}")
    print(f"   Contains schemas for {len(tables)} tables")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
