#!/usr/bin/env python3
"""
Load TPC-DS data into Glue-managed Iceberg tables
This script loads existing Parquet data into the created Glue tables
"""

import os
import sys
import yaml
import logging
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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
            logging.FileHandler(log_dir / "load_tpcds_data.log"),
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

def create_spark_session(spark_config):
    """Create Spark session with Iceberg extensions"""
    logger = logging.getLogger(__name__)
    
    # Build Spark configuration
    spark_conf = {
        "spark.app.name": "TPC-DS Data Loader",
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
    
    logger.info(f"Creating Spark session with config: {spark_conf}")
    
    # Create Spark session
    spark = SparkSession.builder
    
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def load_parquet_data(spark, data_dir, table_name):
    """Load Parquet data for a specific table"""
    logger = logging.getLogger(__name__)
    
    parquet_path = data_dir / "parquet" / f"{table_name}.parquet"
    
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}")
        return None
    
    try:
        logger.info(f"Loading Parquet data from: {parquet_path}")
        df = spark.read.parquet(str(parquet_path))
        logger.info(f"Loaded {df.count()} rows for table {table_name}")
        return df
    except Exception as e:
        logger.error(f"Error loading Parquet data for {table_name}: {e}")
        return None

def load_data_to_glue_table(spark, df, table_name, glue_config):
    """Load data into Glue Iceberg table"""
    logger = logging.getLogger(__name__)
    
    try:
        catalog_name = glue_config['glue']['catalog_name']
        database_name = glue_config['glue']['database_name']
        full_table_name = f"{catalog_name}.{database_name}.{table_name}"
        
        logger.info(f"Loading data into table: {full_table_name}")
        
        # Create table if missing, else append
        try:
            df.writeTo(full_table_name).create()
            logger.info(f"Created table: {full_table_name}")
        except Exception as create_err:
            create_msg = str(create_err)
            if "already exists" in create_msg.lower() or "TableExistsException" in create_msg or "TABLE_ALREADY_EXISTS" in create_msg:
                logger.info(f"Table exists, appending to: {full_table_name}")
                df.writeTo(full_table_name).append()
            else:
                raise
        
        # Verify data was loaded
        loaded_count = spark.table(full_table_name).count()
        logger.info(f"✅ Successfully loaded {loaded_count} rows into {full_table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to load data into {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🚀 Starting TPC-DS data loading into Glue tables...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded successfully")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created successfully")
        
        # Load data directory from environment
        from lib.env import get_tpcds_data_dir
        
        data_dir = get_tpcds_data_dir()
        logger.info(f"Using data directory: {data_dir}")
        
        if not data_dir.exists():
            logger.error(f"Data directory does not exist: {data_dir}")
            return 1
        
        # TPC-DS table names
        tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        # Load data for each table
        success_count = 0
        total_tables = len(tpcds_tables)
        
        logger.info(f"Loading data for {total_tables} tables...")
        
        for table_name in tpcds_tables:
            logger.info(f"Processing table: {table_name}")
            
            # Load Parquet data
            df = load_parquet_data(spark, data_dir, table_name)
            if df is None:
                logger.warning(f"Skipping {table_name} - no data found")
                continue
            
            # Load data into Glue table
            if load_data_to_glue_table(spark, df, table_name, glue_config):
                success_count += 1
            else:
                logger.error(f"Failed to load data for table: {table_name}")
        
        # Summary
        logger.info(f"✅ Data loading completed!")
        logger.info(f"Successfully loaded: {success_count}/{total_tables} tables")
        
        if success_count == total_tables:
            logger.info("🎉 All data loaded successfully!")
        else:
            logger.warning(f"⚠️  {total_tables - success_count} tables failed to load")
        
        # Show final table counts
        logger.info("📊 Final table row counts:")
        catalog_name = glue_config['glue']['catalog_name']
        database_name = glue_config['glue']['database_name']
        
        for table_name in tpcds_tables:
            try:
                count = spark.table(f"{catalog_name}.{database_name}.{table_name}").count()
                logger.info(f"  {table_name}: {count:,} rows")
            except Exception as e:
                logger.error(f"  {table_name}: Error getting count - {e}")
        
        spark.stop()
        return 0 if success_count == total_tables else 1
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
