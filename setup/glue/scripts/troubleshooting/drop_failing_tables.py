#!/usr/bin/env python3
"""
Drop failing Glue tables so they can be recreated with correct schemas
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
            logging.FileHandler(log_dir / "drop_failing_tables.log"),
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

def create_spark_session(spark_config):
    """Create Spark session with Iceberg support"""
    from setup.glue.scripts.create.load_tpcds_data import create_spark_session as create_session
    return create_session(spark_config)

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🗑️  Dropping failing Glue tables...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created")
        
        # All TPC-DS tables
        all_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        # Working tables (keep these)
        working_tables = ['catalog_sales', 'store_sales', 'web_sales']
        
        # Tables to drop (all except working ones)
        tables_to_drop = [t for t in all_tables if t not in working_tables]
        
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        
        logger.info(f"Dropping {len(tables_to_drop)} tables (keeping {len(working_tables)} working tables)...")
        
        dropped_count = 0
        for table_name in tables_to_drop:
            full_table_name = f"{catalog_name}.{database_name}.{table_name}"
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"✅ Dropped table: {table_name}")
                dropped_count += 1
            except Exception as e:
                logger.warning(f"⚠️  Could not drop {table_name}: {e}")
        
        logger.info(f"\n✅ Dropped {dropped_count}/{len(tables_to_drop)} tables")
        logger.info("Next step: Run load_tpcds_data.py to recreate tables with correct schemas")
        
        return 0
        
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

