#!/usr/bin/env python3
"""
Fix Glue table schemas by dropping and recreating tables with correct schemas
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
import re

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "fix_glue_schemas.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration files"""
    config_dir = Path(__file__).parent.parent / "config"
    
    def substitute_env_vars(content: str) -> str:
        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))
        return re.sub(r'\$\{(\w+)\}', replace_var, content)
    
    with open(config_dir / "spark_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        spark_config = yaml.safe_load(content)
    
    with open(config_dir / "glue_catalog_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        glue_config = yaml.safe_load(content)
    
    with open(config_dir / "tpcds_table_schemas.yaml", 'r') as f:
        table_schemas = yaml.safe_load(f)
    
    return spark_config, glue_config, table_schemas

def format_table_name(catalog_name, database_name, table_name):
    """Format table name with proper quoting for hyphenated identifiers
    
    Based on simple-glue-setup approach: hyphenated names must be quoted with backticks
    """
    return f"{catalog_name}.`{database_name}`.`{table_name}`"

def create_spark_session(spark_config, project_root):
    """Create Spark session with Iceberg support"""
    # Get AWS config from environment variables
    aws_config = {
        's3_iceberg_glue_path': os.getenv('AWS_S3_ICEBERG_GLUE_PATH', 's3://iceberg-performance-test/iceberg-snowflake-perf/iceberg-snowflake-perf/iceberg_glue_format/'),
        'region': os.getenv('AWS_REGION', 'us-east-1')
    }
    
    builder = SparkSession.builder \
        .appName("Fix Glue Schemas") \
        .master('local[*]') \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", aws_config['s3_iceberg_glue_path']) \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.aws.region", aws_config['region'])
    
    return builder.getOrCreate()

def drop_table(spark, table_name, glue_config):
    """Drop a Glue table"""
    logger = logging.getLogger(__name__)
    catalog_name = "glue_catalog"
    database_name = glue_config['glue']['database_name']
    full_table_name = format_table_name(catalog_name, database_name, table_name)
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        logger.info(f"✅ Dropped table: {table_name}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to drop table {table_name}: {e}")
        return False

def create_table_from_parquet(spark, table_name, table_def, glue_config, data_dir):
    """Create table from Parquet file schema"""
    logger = logging.getLogger(__name__)
    
    try:
        parquet_path = data_dir / "parquet" / f"{table_name}.parquet"
        if not parquet_path.exists():
            logger.warning(f"Parquet file not found: {parquet_path}")
            return False
        
        # Read Parquet to get schema
        df = spark.read.parquet(str(parquet_path))
        
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        # Get table properties
        table_properties = glue_config['glue'].get('table_properties', {})
        
        # Create table using DataFrame schema
        writer = df.writeTo(full_table_name)
        
        # Add table properties
        for key, value in table_properties.items():
            writer = writer.tableProperty(key, value)
        
        # Create table (empty, will load data separately)
        writer.create()
        
        logger.info(f"✅ Created table: {table_name} with correct schema")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🔧 Starting Glue schema fix...")
    
    try:
        # Load configurations
        spark_config, glue_config, table_schemas = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        spark = create_spark_session(spark_config, project_root)
        logger.info("✅ Spark session created")
        
        # Get data directory
        data_dir = project_root / "data" / "tpcds_data_sf0.01"
        
        # Get list of all tables
        all_tables = list(table_schemas['tables'].keys())
        logger.info(f"Processing {len(all_tables)} tables...")
        
        # Drop all tables
        logger.info("\n1. Dropping existing tables...")
        dropped = 0
        for table_name in all_tables:
            if drop_table(spark, table_name, glue_config):
                dropped += 1
        
        logger.info(f"Dropped {dropped}/{len(all_tables)} tables")
        
        # Recreate tables with correct schemas
        logger.info("\n2. Recreating tables with correct schemas...")
        created = 0
        for table_name in all_tables:
            if create_table_from_parquet(spark, table_name, table_schemas['tables'][table_name], glue_config, data_dir):
                created += 1
        
        logger.info(f"Created {created}/{len(all_tables)} tables")
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("SCHEMA FIX SUMMARY")
        logger.info("="*60)
        logger.info(f"Dropped: {dropped}/{len(all_tables)} tables")
        logger.info(f"Recreated: {created}/{len(all_tables)} tables")
        
        if created == len(all_tables):
            logger.info("\n🎉 All tables recreated successfully!")
            logger.info("Next step: Run load_tpcds_data.py to load data")
            return 0
        else:
            logger.warning(f"\n⚠️  {len(all_tables) - created} tables failed to recreate")
            return 1
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    exit(main())

