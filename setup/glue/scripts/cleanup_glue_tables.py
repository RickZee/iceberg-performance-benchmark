#!/usr/bin/env python3
"""
Cleanup Glue-managed Iceberg tables
This script removes tables from both Glue catalog and Snowflake
"""

import os
import sys
import yaml
import logging
import boto3
from pathlib import Path
from pyspark.sql import SparkSession
import snowflake.connector

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
            logging.FileHandler(log_dir / "cleanup_glue_tables.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration files with environment variable substitution"""
    import re
    
    config_dir = Path(__file__).parent.parent / "config"
    
    def substitute_env_vars(content: str) -> str:
        """Substitute ${VAR_NAME} with environment variable values"""
        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))  # Return original if not found
    
        return re.sub(r'\$\{(\w+)\}', replace_var, content)
    
    # Load and substitute environment variables
    with open(config_dir / "spark_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        spark_config = yaml.safe_load(content)
    
    with open(config_dir / "glue_catalog_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        glue_config = yaml.safe_load(content)
    
    return spark_config, glue_config

def cleanup_glue_catalog(glue_config):
    """Remove tables from AWS Glue catalog"""
    logger = logging.getLogger(__name__)
    
    try:
        glue = boto3.client('glue', region_name=glue_config['glue']['region'])
        database_name = glue_config['glue']['database_name']
        
        logger.info(f"Cleaning up Glue database: {database_name}")
        
        # Get tables from Glue
        response = glue.get_tables(DatabaseName=database_name)
        tables = response['TableList']
        
        logger.info(f"Found {len(tables)} tables to remove")
        
        for table in tables:
            table_name = table['Name']
            try:
                glue.delete_table(DatabaseName=database_name, Name=table_name)
                logger.info(f"✅ Removed table: {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to remove table {table_name}: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up Glue catalog: {e}")
        return False

def cleanup_spark_tables(spark, glue_config):
    """Remove tables through Spark"""
    logger = logging.getLogger(__name__)
    
    try:
        catalog_name = glue_config['glue']['catalog_name']
        database_name = glue_config['glue']['database_name']
        
        logger.info(f"Cleaning up Spark tables in {catalog_name}.{database_name}")
        
        # List tables
        tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name}")
        table_list = [row.tableName for row in tables.collect()]
        
        logger.info(f"Found {len(table_list)} tables to remove")
        
        for table_name in table_list:
            try:
                spark.sql(f"DROP TABLE {catalog_name}.{database_name}.{table_name}")
                logger.info(f"✅ Removed table: {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to remove table {table_name}: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up Spark tables: {e}")
        return False

def cleanup_snowflake_tables(glue_config):
    """Remove tables from Snowflake"""
    logger = logging.getLogger(__name__)
    
    try:
        # Load Snowflake configuration from environment
        from lib.env import get_snowflake_config, get_snowflake_schemas
        
        snowflake_config = get_snowflake_config()
        schemas = get_snowflake_schemas()
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            role=snowflake_config['role'],
            private_key_file=snowflake_config['private_key_file'],
            private_key_passphrase=snowflake_config['private_key_passphrase']
        )
        
        logger.info("✅ Connected to Snowflake successfully")
        
        cur = conn.cursor()
        
        # Check if schema exists and get tables
        try:
            iceberg_glue_schema = schemas['iceberg_glue'].upper()
            cur.execute(f"USE SCHEMA {iceberg_glue_schema}")
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            
            logger.info(f"Found {len(tables)} tables to remove from Snowflake")
            
            for table in tables:
                table_name = table[1]
                try:
                    cur.execute(f"DROP TABLE {table_name}")
                    logger.info(f"✅ Removed Snowflake table: {table_name}")
                except Exception as e:
                    logger.error(f"❌ Failed to remove Snowflake table {table_name}: {e}")
        
        except Exception as e:
            logger.warning(f"Schema or tables not found in Snowflake: {e}")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up Snowflake tables: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🧹 Starting Glue table cleanup...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded successfully")
        
        # Load AWS configuration from environment
        from lib.env import get_aws_config
        
        aws_config = get_aws_config()
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("Glue Table Cleanup") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", aws_config['s3_iceberg_glue_path']) \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.hadoop.fs.s3a.aws.region", aws_config['region']) \
            .getOrCreate()
        
        logger.info("✅ Spark session created successfully")
        
        # Cleanup Glue catalog
        logger.info("\n1. Cleaning up AWS Glue catalog...")
        glue_ok = cleanup_glue_catalog(glue_config)
        
        # Cleanup Spark tables
        logger.info("\n2. Cleaning up Spark tables...")
        spark_ok = cleanup_spark_tables(spark, glue_config)
        
        # Cleanup Snowflake tables
        logger.info("\n3. Cleaning up Snowflake tables...")
        snowflake_ok = cleanup_snowflake_tables(glue_config)
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("CLEANUP SUMMARY")
        logger.info("="*60)
        logger.info(f"Glue Catalog: {'✅ COMPLETE' if glue_ok else '❌ FAILED'}")
        logger.info(f"Spark Tables: {'✅ COMPLETE' if spark_ok else '❌ FAILED'}")
        logger.info(f"Snowflake Tables: {'✅ COMPLETE' if snowflake_ok else '❌ FAILED'}")
        
        if glue_ok and spark_ok and snowflake_ok:
            logger.info("\n🎉 Cleanup completed successfully!")
            return 0
        else:
            logger.warning("\n⚠️  Some cleanup operations failed. Check the logs for details.")
            return 1
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    exit(main())
