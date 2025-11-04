#!/usr/bin/env python3
"""
Clear data from Glue Iceberg tables using Spark
This script drops and recreates tables to clear existing data
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession

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
            logging.FileHandler(log_dir / "clear_glue_data.log"),
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
            return os.getenv(var_name, match.group(0))
        
        return re.sub(r'\$\{(\w+)\}', replace_var, content)
    
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
        "spark.app.name": "Clear Glue Table Data",
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
    
    logger.info(f"Creating Spark session with warehouse: {spark_config['spark']['config'].get('spark.sql.catalog.glue_catalog.warehouse', 'N/A')}")
    
    # Create Spark session
    spark = SparkSession.builder
    
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def format_table_name(catalog_name, database_name, table_name):
    """Format table name with proper quoting for hyphenated identifiers
    
    Based on simple-glue-setup approach: hyphenated names must be quoted with backticks
    """
    return f"{catalog_name}.`{database_name}`.`{table_name}`"

def clear_table_data(spark, table_name, glue_config):
    """Clear data from a Glue table by dropping and recreating it"""
    logger = logging.getLogger(__name__)
    
    try:
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        # Get table schema first
        try:
            existing_table = spark.table(full_table_name)
            schema = existing_table.schema
            
            # Drop the table
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            logger.info(f"Dropped table: {table_name}")
            
            # Recreate empty table with same schema
            empty_df = spark.createDataFrame([], schema)
            writer = empty_df.writeTo(full_table_name)
            
            # Add table properties
            table_properties = glue_config['glue']['table_properties']
            for key, value in table_properties.items():
                writer = writer.tableProperty(key, value)
            
            writer.create()
            logger.info(f"✅ Recreated empty table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error clearing {table_name}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Failed to clear {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🧹 Starting Glue table data clearing...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created")
        
        # All TPC-DS tables
        tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        logger.info(f"Clearing data from {len(tpcds_tables)} tables...")
        
        cleared_count = 0
        for table_name in tpcds_tables:
            if clear_table_data(spark, table_name, glue_config):
                cleared_count += 1
        
        logger.info(f"\n✅ Cleared {cleared_count}/{len(tpcds_tables)} tables")
        
        spark.stop()
        return 0 if cleared_count == len(tpcds_tables) else 1
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())

