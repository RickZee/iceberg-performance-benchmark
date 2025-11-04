#!/usr/bin/env python3
"""
Verify row counts for all Glue-managed Iceberg tables
This script checks that all tables are accessible and returns row counts
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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
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
        "spark.app.name": "Verify Glue Table Counts",
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
    
    if jar_files:
        spark_conf["spark.jars"] = ",".join(jar_files)
    
    # Create Spark session
    spark = SparkSession.builder
    
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def verify_table_count(spark, table_name, glue_config):
    """Verify row count for a table"""
    logger = logging.getLogger(__name__)
    
    try:
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        # Get row count
        count = spark.table(full_table_name).count()
        
        # Get column count
        df = spark.table(full_table_name)
        column_count = len(df.columns)
        
        return {
            'table': table_name,
            'row_count': count,
            'column_count': column_count,
            'status': 'success'
        }
        
    except Exception as e:
        logger.error(f"Error verifying {table_name}: {e}")
        return {
            'table': table_name,
            'row_count': 0,
            'column_count': 0,
            'status': 'error',
            'error': str(e)
        }

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🔍 Verifying row counts for all Glue-managed Iceberg tables...")
    
    try:
        # Load configurations
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created")
        
        # All 24 TPC-DS tables
        tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        logger.info(f"\n{'='*80}")
        logger.info(f"Verifying {len(tpcds_tables)} tables...")
        logger.info(f"{'='*80}\n")
        
        # Verify each table
        results = []
        total_rows = 0
        
        for table_name in tpcds_tables:
            result = verify_table_count(spark, table_name, glue_config)
            results.append(result)
            
            if result['status'] == 'success':
                total_rows += result['row_count']
                logger.info(f"✅ {table_name:25} | Rows: {result['row_count']:>8,} | Columns: {result['column_count']:>3}")
            else:
                logger.error(f"❌ {table_name:25} | ERROR: {result.get('error', 'Unknown error')}")
        
        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("VERIFICATION SUMMARY")
        logger.info(f"{'='*80}")
        
        successful = sum(1 for r in results if r['status'] == 'success')
        failed = len(results) - successful
        
        logger.info(f"Total tables: {len(tpcds_tables)}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"📊 Total rows across all tables: {total_rows:,}")
        
        if failed == 0:
            logger.info("\n🎉 All tables verified successfully!")
            
            # Detailed breakdown
            logger.info(f"\n{'='*80}")
            logger.info("DETAILED BREAKDOWN")
            logger.info(f"{'='*80}")
            
            dimension_tables = [
                'call_center', 'catalog_page', 'customer', 'customer_address',
                'customer_demographics', 'date_dim', 'household_demographics',
                'income_band', 'item', 'promotion', 'reason', 'ship_mode',
                'store', 'time_dim', 'warehouse', 'web_page', 'web_site'
            ]
            
            fact_tables = [
                'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
                'store_sales', 'web_returns', 'web_sales'
            ]
            
            dim_rows = sum(r['row_count'] for r in results if r['table'] in dimension_tables)
            fact_rows = sum(r['row_count'] for r in results if r['table'] in fact_tables)
            
            logger.info(f"Dimension tables (17): {dim_rows:,} rows")
            logger.info(f"Fact tables (7): {fact_rows:,} rows")
            logger.info(f"Grand total: {total_rows:,} rows")
            
            return 0
        else:
            logger.warning(f"\n⚠️  {failed} tables failed verification")
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

