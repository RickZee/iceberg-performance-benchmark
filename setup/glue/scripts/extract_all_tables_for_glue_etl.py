#!/usr/bin/env python3
"""
Extract all 24 TPC-DS table schemas from Terraform and generate complete Glue ETL script
This properly extracts all columns from both Terraform files
"""

import re
import sys
from pathlib import Path
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, DecimalType, LongType, BooleanType, TimestampType

project_root = Path(__file__).parent.parent.parent.parent
terraform_dir = project_root / "setup" / "infrastructure"
output_file = project_root / "setup" / "glue" / "scripts" / "create_all_iceberg_tables_glue_etl.py"

def map_terraform_type_to_spark_type(tf_type):
    """Map Terraform/Hive type to Spark type"""
    type_mapping = {
        'string': 'StringType()',
        'int': 'IntegerType()',
        'bigint': 'LongType()',
        'double': 'DoubleType()',
        'float': 'DoubleType()',  # Use DoubleType for float
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

def extract_all_tables_from_terraform():
    """Extract all table schemas from Terraform files"""
    tables = {}
    
    # Read both Terraform files
    for tf_file in ["glue_iceberg_tables.tf", "glue_iceberg_tables_complete.tf"]:
        tf_path = terraform_dir / tf_file
        if not tf_path.exists():
            print(f"Warning: {tf_file} not found")
            continue
            
        with open(tf_path, 'r') as f:
            content = f.read()
        
        # Find all table resource definitions
        # Pattern: resource "aws_glue_catalog_table" "table_name_iceberg"
        resource_pattern = r'resource\s+"aws_glue_catalog_table"\s+"([a-z_]+)_iceberg"'
        
        for resource_match in re.finditer(resource_pattern, content):
            resource_name = resource_match.group(1)
            start_pos = resource_match.start()
            
            # Find the name = "table_name" line
            name_pattern = rf'name\s+=\s+"([^"]+)"'
            name_match = re.search(name_pattern, content[start_pos:start_pos+500])
            if not name_match:
                continue
            
            table_name = name_match.group(1)
            
            # Extract columns from storage_descriptor
            columns = []
            # Find the storage_descriptor block
            storage_start = content.find('storage_descriptor {', start_pos)
            if storage_start == -1:
                continue
            
            # Find columns block within storage_descriptor
            columns_start = content.find('columns {', storage_start)
            if columns_start == -1:
                continue
            
            # Extract all column definitions
            # Pattern: columns { name = "col_name" type = "col_type" ... }
            col_pattern = r'columns\s+\{\s*name\s+=\s+"([^"]+)"[^}]*type\s+=\s+"([^"]+)"'
            
            for col_match in re.finditer(col_pattern, content[columns_start:columns_start+5000]):
                col_name = col_match.group(1)
                col_type = col_match.group(2)
                
                # Check if nullable (default True for most columns)
                nullable = True
                # Primary keys are typically not nullable
                if col_name.endswith('_sk') and 'Primary key' in content[col_match.start()+columns_start:col_match.end()+columns_start+200]:
                    nullable = False
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': nullable
                })
            
            if columns and table_name:
                tables[table_name] = columns
    
    return tables

def generate_spark_schema_code(table_name, columns):
    """Generate Spark StructType code for a table"""
    fields = []
    for col in columns:
        col_name = col['name']
        spark_type = map_terraform_type_to_spark_type(col['type'])
        nullable = "True" if col.get('nullable', True) else "False"
        fields.append(f'        StructField("{col_name}", {spark_type}, {nullable})')
    
    schema_code = f"    '{table_name}': StructType([\n"
    schema_code += ",\n".join(fields)
    schema_code += "\n    ]),"
    return schema_code

def generate_complete_glue_etl_script(tables):
    """Generate the complete Glue ETL script"""
    
    # Sort tables for consistent output
    sorted_tables = sorted(tables.items())
    
    # Generate schema code
    schema_code = "# Table schemas for all 24 TPC-DS tables\n"
    schema_code += "table_schemas = {\n"
    for table_name, columns in sorted_tables:
        schema_code += generate_spark_schema_code(table_name, columns) + "\n"
    schema_code += "}\n"
    
    # Read the base script template
    base_script = '''#!/usr/bin/env python3
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
# Glue 4.0 has native Iceberg support, but we still need to configure the catalog
# Note: In Glue, we can set catalog configurations after SparkContext is created
# The spark.sql.extensions must be set via job arguments (--conf) in the job definition
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", s3_warehouse_path)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Verify Iceberg extensions are available (they should be if configured via --conf)
print("Iceberg catalog configuration:")
print(f"  Catalog: glue_catalog")
print(f"  Warehouse: {s3_warehouse_path}")

'''
    
    # Generate the function
    function_code = '''
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
        
        # First, try to drop the table if it exists (from Terraform, lacks Iceberg metadata)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            print(f"  Dropped existing table (if any)")
        except Exception as drop_error:
            print(f"  ⚠️  Could not drop table (may not exist): {drop_error}")
        
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
            print(f"  ⚠️  Table already exists, dropping and recreating with Iceberg metadata...")
            try:
                # Drop existing table (created by Terraform, lacks Iceberg metadata)
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                print(f"  Dropped existing table")
                # Recreate with proper Iceberg metadata
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
                except Exception as e2:
                    print(f"  ⚠️  Could not retrieve metadata location: {e2}")
                
                print(f"  ✅ Successfully recreated table with Iceberg metadata: {table_name}")
                return True
            except Exception as e2:
                print(f"  ❌ Failed to recreate table {table_name}: {e2}")
                import traceback
                traceback.print_exc()
                return False
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
    
    return base_script + schema_code + function_code

def main():
    """Main function"""
    print("Extracting table schemas from Terraform files...")
    tables = extract_all_tables_from_terraform()
    
    print(f"\nFound {len(tables)} tables:")
    for i, table_name in enumerate(sorted(tables.keys()), 1):
        col_count = len(tables[table_name])
        print(f"  {i}. {table_name} ({col_count} columns)")
    
    if len(tables) != 24:
        print(f"\n⚠️  Warning: Expected 24 tables, found {len(tables)}")
    
    print("\nGenerating Glue ETL job script...")
    script_content = generate_complete_glue_etl_script(tables)
    
    with open(output_file, 'w') as f:
        f.write(script_content)
    
    print(f"✅ Generated Glue ETL job script: {output_file}")
    print(f"   Contains schemas for {len(tables)} tables")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

