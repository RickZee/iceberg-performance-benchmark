#!/usr/bin/env python3
"""
Recreate all Glue Iceberg tables from Parquet file schemas
This script reads schemas from Parquet files and recreates all tables with proper backtick quoting
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
import re

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "recreate_all_glue_tables.log"),
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
    
    return spark_config, glue_config

def format_table_name(catalog_name, database_name, table_name):
    """Format table name with proper quoting for hyphenated identifiers"""
    return f"{catalog_name}.`{database_name}`.`{table_name}`"

def create_spark_session(spark_config):
    """Create Spark session with Iceberg extensions"""
    logger = logging.getLogger(__name__)
    
    # Build Spark configuration
    spark_conf = {
        "spark.app.name": "Recreate All Glue Tables",
        "spark.master": spark_config['spark']['master']
    }
    
    # Add all Spark configs
    spark_conf.update(spark_config['spark']['config'])
    
    # Add JAR files
    jar_files = []
    for jar_path in spark_config['jars']:
        full_jar_path = project_root / jar_path
        if full_jar_path.exists():
            jar_files.append(str(full_jar_path))
        else:
            logger.warning(f"JAR file not found: {full_jar_path}")
    
    if jar_files:
        spark_conf["spark.jars"] = ",".join(jar_files)
    
    logger.info(f"Creating Spark session...")
    
    # Create Spark session
    spark = SparkSession.builder
    
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

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
        logger.warning(f"⚠️  Could not drop table {table_name}: {e}")
        return False

def create_table_from_parquet(spark, table_name, glue_config, data_dir):
    """Create table from Parquet file schema"""
    logger = logging.getLogger(__name__)
    
    try:
        parquet_path = data_dir / "parquet" / f"{table_name}.parquet"
        if not parquet_path.exists():
            logger.warning(f"Parquet file not found: {parquet_path}")
            return False
        
        # Read Parquet to get schema
        logger.info(f"Reading schema from: {parquet_path}")
        df = spark.read.parquet(str(parquet_path))
        schema = df.schema
        
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        # Get table properties
        table_properties = glue_config['glue'].get('table_properties', {})
        
        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], schema)
        
        # Create table using DataFrame schema
        writer = empty_df.writeTo(full_table_name)
        
        # Add table properties
        for key, value in table_properties.items():
            writer = writer.tableProperty(key, value)
        
        # Create table
        writer.create()
        
        # Get and log metadata location
        try:
            metadata_result = spark.sql(
                f"SHOW TBLPROPERTIES {full_table_name} ('metadata_location')"
            ).collect()
            if metadata_result:
                metadata_location = metadata_result[0][0]
                logger.info(f"  Metadata location: {metadata_location}")
        except Exception as e:
            logger.warning(f"  Could not retrieve metadata location: {e}")
        
        logger.info(f"✅ Created table: {table_name} ({len(schema.fields)} columns)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🚀 Starting recreation of all Glue Iceberg tables...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created")
        
        # Get data directory
        data_dir = project_root / "data" / "tpcds_data_sf0.01"
        
        if not data_dir.exists():
            logger.error(f"Data directory does not exist: {data_dir}")
            return 1
        
        # All 24 TPC-DS tables
        tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        logger.info(f"Processing {len(tpcds_tables)} tables...")
        
        # Step 1: Drop all existing tables
        logger.info("\n" + "="*60)
        logger.info("STEP 1: Dropping existing tables...")
        logger.info("="*60)
        dropped = 0
        for table_name in tpcds_tables:
            if drop_table(spark, table_name, glue_config):
                dropped += 1
        
        logger.info(f"Dropped {dropped}/{len(tpcds_tables)} tables")
        
        # Step 2: Recreate tables with schemas from Parquet files
        logger.info("\n" + "="*60)
        logger.info("STEP 2: Recreating tables with correct schemas...")
        logger.info("="*60)
        created = 0
        for table_name in tpcds_tables:
            logger.info(f"Creating table: {table_name}")
            if create_table_from_parquet(spark, table_name, glue_config, data_dir):
                created += 1
        
        logger.info(f"\nCreated {created}/{len(tpcds_tables)} tables")
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("RECREATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Dropped: {dropped}/{len(tpcds_tables)} tables")
        logger.info(f"Recreated: {created}/{len(tpcds_tables)} tables")
        
        if created == len(tpcds_tables):
            logger.info("\n🎉 All tables recreated successfully!")
            logger.info("Next step: Run load_tpcds_data.py to load data")
            return 0
        else:
            logger.warning(f"\n⚠️  {len(tpcds_tables) - created} tables failed to recreate")
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

