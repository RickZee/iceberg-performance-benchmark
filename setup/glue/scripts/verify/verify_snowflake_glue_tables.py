#!/usr/bin/env python3
"""
Verify Glue-managed Iceberg tables in Snowflake
This script queries Snowflake directly to verify row counts
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import snowflake.connector
from lib.env import get_snowflake_config, get_snowflake_schemas

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

def verify_table_count(sf_conn, table_name):
    """Verify row count for a table in Snowflake"""
    logger = logging.getLogger(__name__)
    
    try:
        cursor = sf_conn.cursor()
        schemas = get_snowflake_schemas()
        schema_name = schemas.get('iceberg_glue', 'tpcds_iceberg_glue_format')
        
        # Get row count
        count_sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
        cursor.execute(count_sql)
        row_count = cursor.fetchone()[0]
        
        # Get column count
        desc_sql = f"DESC TABLE {schema_name}.{table_name};"
        cursor.execute(desc_sql)
        columns = cursor.fetchall()
        column_count = len(columns)
        
        cursor.close()
        
        return {
            'success': True,
            'row_count': row_count,
            'column_count': column_count
        }
        
    except Exception as e:
        logger.error(f"Error verifying {table_name}: {e}")
        return {
            'success': False,
            'error': str(e),
            'row_count': 0,
            'column_count': 0
        }

def refresh_table(sf_conn, table_name):
    """Refresh Iceberg table metadata in Snowflake"""
    logger = logging.getLogger(__name__)
    
    try:
        cursor = sf_conn.cursor()
        schemas = get_snowflake_schemas()
        schema_name = schemas.get('iceberg_glue', 'tpcds_iceberg_glue_format')
        
        refresh_sql = f"ALTER ICEBERG TABLE {schema_name}.{table_name} REFRESH;"
        cursor.execute(refresh_sql)
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Error refreshing {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🔍 Verifying Glue-managed Iceberg tables in Snowflake...")
    
    # All 24 TPC-DS tables
    tpcds_tables = [
        'call_center', 'catalog_page', 'customer', 'customer_address',
        'customer_demographics', 'date_dim', 'household_demographics',
        'income_band', 'item', 'promotion', 'reason', 'ship_mode',
        'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
        'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
        'store_sales', 'web_returns', 'web_sales'
    ]
    
    try:
        # Connect to Snowflake
        sf_config = get_snowflake_config()
        schemas = get_snowflake_schemas()
        schema_name = schemas.get('iceberg_glue', 'tpcds_iceberg_glue_format')
        
        sf_conn = snowflake.connector.connect(
            user=sf_config['user'],
            account=sf_config['account'],
            warehouse=sf_config['warehouse'],
            database=sf_config['database'],
            role=sf_config['role'],
            private_key_file=sf_config['private_key_file'],
            private_key_passphrase=sf_config['private_key_passphrase'] if sf_config['private_key_passphrase'] else None
        )
        
        cursor = sf_conn.cursor()
        cursor.execute(f"USE DATABASE {sf_config['database']}")
        cursor.execute(f"USE SCHEMA {schema_name}")
        cursor.close()
        
        logger.info(f"✅ Connected to Snowflake (schema: {schema_name})")
        
        # Refresh all tables first
        logger.info(f"\n{'='*80}")
        logger.info("STEP 1: Refreshing table metadata...")
        logger.info(f"{'='*80}")
        
        refreshed = 0
        for table_name in tpcds_tables:
            if refresh_table(sf_conn, table_name):
                refreshed += 1
                logger.info(f"  ✅ Refreshed: {table_name}")
            else:
                logger.error(f"  ❌ Failed to refresh: {table_name}")
        
        logger.info(f"Refreshed {refreshed}/{len(tpcds_tables)} tables")
        
        # Verify tables
        logger.info(f"\n{'='*80}")
        logger.info("STEP 2: Verifying row counts in Snowflake...")
        logger.info(f"{'='*80}\n")
        
        results = []
        total_rows = 0
        
        for table_name in tpcds_tables:
            result = verify_table_count(sf_conn, table_name)
            result['table'] = table_name
            results.append(result)
            
            if result['success']:
                total_rows += result['row_count']
                logger.info(f"✅ {table_name:25} | Rows: {result['row_count']:>8,} | Columns: {result['column_count']:>3}")
            else:
                logger.error(f"❌ {table_name:25} | ERROR: {result.get('error', 'Unknown')}")
        
        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("VERIFICATION SUMMARY")
        logger.info(f"{'='*80}")
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        logger.info(f"Total tables: {len(tpcds_tables)}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"📊 Total rows: {total_rows:,}")
        
        if successful == len(tpcds_tables) and total_rows > 0:
            logger.info("\n🎉 All tables verified successfully with data!")
            return 0
        elif successful == len(tpcds_tables) and total_rows == 0:
            logger.warning("\n⚠️  All tables accessible but showing 0 rows")
            logger.warning("   This may indicate:")
            logger.warning("   1. External volume bucket mismatch")
            logger.warning("   2. Metadata files not accessible")
            logger.warning("   3. Catalog sync delay")
            return 1
        else:
            logger.warning(f"\n⚠️  {failed} tables failed verification")
            return 1
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if 'sf_conn' in locals():
            sf_conn.close()

if __name__ == "__main__":
    exit(main())

