#!/usr/bin/env python3
"""
Drop failing Glue tables using AWS Glue API
"""

import os
import sys
import boto3
import logging
from pathlib import Path

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
            logging.FileHandler(log_dir / "drop_failing_tables_glue.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🗑️  Dropping failing Glue tables using AWS Glue API...")
    
    try:
        # Get AWS config
        region = os.getenv('AWS_REGION', 'us-east-1')
        database_name = os.getenv('AWS_GLUE_DATABASE', 'iceberg_performance_test')
        
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
        
        # Create Glue client
        glue = boto3.client('glue', region_name=region)
        
        logger.info(f"Dropping {len(tables_to_drop)} tables from database '{database_name}' (keeping {len(working_tables)} working tables)...")
        
        dropped_count = 0
        for table_name in tables_to_drop:
            try:
                glue.delete_table(DatabaseName=database_name, Name=table_name)
                logger.info(f"✅ Dropped table: {table_name}")
                dropped_count += 1
            except glue.exceptions.EntityNotFoundException:
                logger.info(f"ℹ️  Table {table_name} does not exist (skipping)")
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

if __name__ == "__main__":
    exit(main())

