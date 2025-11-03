#!/usr/bin/env python3
"""
Clear TPC-DS Data Script
Clears all TPC-DS data from all schemas, including external tables and S3 data
"""

import snowflake.connector
import sys
import logging
import boto3
import os
from typing import Dict, List, Any
from urllib.parse import urlparse

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from lib.output import output_manager

# Set up logging to results folder
log_config = output_manager.get_logger_config("tpcds_data_cleaner.log")
logging.basicConfig(**log_config)
logger = logging.getLogger(__name__)

class TPCDSDataCleaner:
    """Clears all TPC-DS data from Snowflake tables"""
    
    def __init__(self):
        """Initialize with connection parameters"""
        # Load Snowflake configuration from environment
        from lib.env import get_snowflake_config, get_snowflake_schemas
        
        snowflake_config = get_snowflake_config()
        self.schemas = get_snowflake_schemas()
        
        self.conn_params = {
            'user': snowflake_config['user'],
            'account': snowflake_config['account'],
            'private_key_file': snowflake_config['private_key_file'],
            'private_key_passphrase': snowflake_config['private_key_passphrase'],
            'warehouse': snowflake_config['warehouse'],
            'database': snowflake_config['database'],
            'role': snowflake_config['role']
        }
        self.connection = None
        self.cursor = None
        self.s3_client = None
        
        # TPC-DS schemas to clean (loaded from environment)
        self.tpcds_schemas = [
            self.schemas['native'],
            self.schemas['iceberg_sf'], 
            self.schemas['iceberg_glue'],
            self.schemas['external']
        ]
        
        # TPC-DS table names
        self.tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
    def connect(self):
        """Connect to Snowflake"""
        try:
            logger.info("🔌 Connecting to Snowflake...")
            self.connection = snowflake.connector.connect(**self.conn_params)
            self.cursor = self.connection.cursor()
            
            # Set context
            self.cursor.execute(f"USE DATABASE {self.conn_params['database']}")
            self.cursor.execute(f"USE WAREHOUSE {self.conn_params['warehouse']}")
            logger.info("✅ Connected successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def disconnect(self):
        """Disconnect from Snowflake"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("🔌 Disconnected from Snowflake")
    
    def connect_s3(self):
        """Connect to AWS S3"""
        try:
            logger.info("🔌 Connecting to AWS S3...")
            self.s3_client = boto3.client('s3')
            # Test connection
            self.s3_client.list_buckets()
            logger.info("✅ Connected to S3 successfully!")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to S3: {e}")
            return False
    
    def delete_s3_data(self, s3_path: str) -> bool:
        """Delete all files in an S3 path"""
        try:
            # Parse S3 URL
            parsed_url = urlparse(s3_path)
            bucket_name = parsed_url.netloc
            prefix = parsed_url.path.lstrip('/')
            
            logger.info(f"🗑️  Deleting S3 data from s3://{bucket_name}/{prefix}")
            
            # List all objects with the prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            objects_to_delete = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})
            
            if not objects_to_delete:
                logger.info(f"   📋 No objects found in s3://{bucket_name}/{prefix}")
                return True
            
            # Delete objects in batches
            batch_size = 1000
            for i in range(0, len(objects_to_delete), batch_size):
                batch = objects_to_delete[i:i + batch_size]
                response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )
                
                deleted_count = len(response.get('Deleted', []))
                logger.info(f"   ✅ Deleted {deleted_count} objects from batch {i//batch_size + 1}")
            
            logger.info(f"   ✅ Successfully deleted {len(objects_to_delete)} objects from s3://{bucket_name}/{prefix}")
            return True
            
        except Exception as e:
            logger.error(f"   ❌ Failed to delete S3 data from {s3_path}: {e}")
            return False
    
    def get_external_table_s3_paths(self) -> Dict[str, str]:
        """Get S3 paths for TPC-DS external tables"""
        s3_paths = {}
        
        try:
            # Get external table information for TPC-DS schema
            self.cursor.execute("SHOW EXTERNAL TABLES IN SCHEMA tpcds_external_format")
            external_tables = self.cursor.fetchall()
            
            for table_info in external_tables:
                table_name = table_info[1]
                
                # Extract S3 path from the external table info
                # The S3 location is in column 9 (index 9) based on the structure
                if len(table_info) > 9 and table_info[9]:
                    s3_location = table_info[9]
                    if s3_location.startswith('s3://'):
                        s3_paths[table_name] = s3_location
                        logger.info(f"   📍 Found S3 path for {table_name}: {s3_location}")
                    else:
                        logger.warning(f"   ⚠️  No S3 path found for {table_name} (value: {s3_location})")
                else:
                    logger.warning(f"   ⚠️  Could not extract S3 path for {table_name} (table_info length: {len(table_info)})")
            
            return s3_paths
            
        except Exception as e:
            logger.error(f"Failed to get external table S3 paths: {e}")
            return {}
    
    def refresh_external_tables(self) -> Dict[str, Any]:
        """Refresh metadata for TPC-DS external tables"""
        refresh_results = {
            'total_tables': 0,
            'refreshed_tables': 0,
            'failed_refreshes': 0,
            'table_results': {}
        }
        
        try:
            logger.info("🔄 Refreshing TPC-DS external table metadata...")
            
            # Get all external tables in TPC-DS schema
            self.cursor.execute("SHOW EXTERNAL TABLES IN SCHEMA tpcds_external_format")
            external_tables = self.cursor.fetchall()
            
            for table_info in external_tables:
                table_name = table_info[1]
                refresh_results['total_tables'] += 1
                
                try:
                    # Refresh the external table metadata
                    self.cursor.execute(f"ALTER EXTERNAL TABLE tpcds_external_format.{table_name} REFRESH")
                    refresh_results['refreshed_tables'] += 1
                    refresh_results['table_results'][table_name] = 'refreshed'
                    logger.info(f"   ✅ Refreshed metadata for {table_name}")
                    
                except Exception as e:
                    refresh_results['failed_refreshes'] += 1
                    refresh_results['table_results'][table_name] = f'failed: {str(e)[:50]}'
                    logger.error(f"   ❌ Failed to refresh {table_name}: {e}")
            
            logger.info(f"🔄 Metadata refresh completed!")
            logger.info(f"   📊 Total tables: {refresh_results['total_tables']}")
            logger.info(f"   ✅ Refreshed: {refresh_results['refreshed_tables']}")
            logger.info(f"   ❌ Failed: {refresh_results['failed_refreshes']}")
            
        except Exception as e:
            logger.error(f"❌ Error during metadata refresh: {e}")
            refresh_results['error'] = str(e)
        
        return refresh_results
    
    def clear_table_data(self, schema_name: str, table_name: str) -> bool:
        """Clear data from a specific TPC-DS table"""
        full_table_name = f"{schema_name}.{table_name}"
        
        try:
            # First, try to determine if it's an external table
            try:
                self.cursor.execute(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {schema_name}")
                table_details = self.cursor.fetchall()
                
                if table_details and len(table_details[0]) > 2:
                    table_type = table_details[0][2] if len(table_details[0]) > 2 else 'TABLE'
                    
                    if 'EXTERNAL' in str(table_type).upper():
                        logger.info(f"   📋 {full_table_name} is an external table - skipping data clearing")
                        return True
                
            except Exception as e:
                logger.debug(f"Could not determine table type for {full_table_name}: {e}")
            
            # Try TRUNCATE first (works for regular tables)
            try:
                self.cursor.execute(f"TRUNCATE TABLE {full_table_name}")
                logger.info(f"   ✅ Truncated {full_table_name}")
                return True
                
            except Exception as e:
                # If TRUNCATE fails, try DELETE
                try:
                    self.cursor.execute(f"DELETE FROM {full_table_name}")
                    logger.info(f"   ✅ Deleted from {full_table_name}")
                    return True
                    
                except Exception as delete_error:
                    logger.warning(f"   ⚠️  Could not clear {full_table_name}: TRUNCATE failed ({e}), DELETE failed ({delete_error})")
                    return False
                    
        except Exception as e:
            logger.error(f"   ❌ Failed to clear {full_table_name}: {e}")
            return False
    
    def clear_tpcds_tables(self, clear_external_s3: bool = True, refresh_metadata: bool = True) -> Dict[str, Any]:
        """Clear all TPC-DS tables in all schemas"""
        results = {
            'total_schemas': 0,
            'total_tables': 0,
            'cleared_tables': 0,
            'failed_tables': 0,
            'skipped_tables': 0,
            's3_paths_deleted': 0,
            's3_deletion_failed': 0,
            'metadata_refresh_results': {},
            'schema_results': {}
        }
        
        if not self.connect():
            return results
        
        try:
            logger.info("🧹 Starting to clear TPC-DS tables...")
            
            # Handle external tables S3 deletion first
            if clear_external_s3:
                logger.info("🗑️  Clearing TPC-DS external table S3 data...")
                
                if self.connect_s3():
                    # Get S3 paths for external tables
                    s3_paths = self.get_external_table_s3_paths()
                    
                    for table_name, s3_path in s3_paths.items():
                        logger.info(f"   📁 Processing external table: {table_name}")
                        if self.delete_s3_data(s3_path):
                            results['s3_paths_deleted'] += 1
                        else:
                            results['s3_deletion_failed'] += 1
                    
                    logger.info(f"   📊 S3 deletion summary: {results['s3_paths_deleted']} successful, {results['s3_deletion_failed']} failed")
                    
                    # Refresh external table metadata after S3 deletion
                    if refresh_metadata and results['s3_paths_deleted'] > 0:
                        logger.info("🔄 Refreshing TPC-DS external table metadata after S3 deletion...")
                        refresh_results = self.refresh_external_tables()
                        results['metadata_refresh_results'] = refresh_results
                        
                else:
                    logger.warning("⚠️  Could not connect to S3, skipping external table data deletion")
            
            # Process each TPC-DS schema
            for schema_name in self.tpcds_schemas:
                logger.info(f"📁 Processing TPC-DS schema: {schema_name}")
                results['total_schemas'] += 1
                
                schema_results = {
                    'total_tables': 0,
                    'cleared_tables': 0,
                    'failed_tables': 0,
                    'skipped_tables': 0,
                    'tables': {}
                }
                
                # Check if schema exists
                try:
                    self.cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
                    existing_tables = [row[1] for row in self.cursor.fetchall()]
                except Exception as e:
                    logger.warning(f"   ⚠️  Schema {schema_name} does not exist or is not accessible: {e}")
                    results['schema_results'][schema_name] = schema_results
                    continue
                
                # Process each TPC-DS table
                for table_name in self.tpcds_tables:
                    # Construct full table name based on schema format
                    if schema_name == 'tpcds_native_format':
                        full_table_name = f"{table_name}_native"
                    elif schema_name == 'tpcds_iceberg_sf_format':
                        full_table_name = f"{table_name}_iceberg_sf"
                    elif schema_name == 'tpcds_iceberg_glue_format':
                        full_table_name = f"{table_name}_iceberg_glue"
                    elif schema_name == 'tpcds_external_format':
                        full_table_name = f"{table_name}_external"
                    else:
                        full_table_name = table_name
                    
                    # Check if table exists
                    if full_table_name not in existing_tables:
                        logger.info(f"   📋 Table {full_table_name} does not exist in {schema_name} - skipping")
                        schema_results['skipped_tables'] += 1
                        results['skipped_tables'] += 1
                        schema_results['tables'][full_table_name] = 'skipped'
                        continue
                    
                    schema_results['total_tables'] += 1
                    results['total_tables'] += 1
                    
                    success = self.clear_table_data(schema_name, full_table_name)
                    
                    if success:
                        schema_results['cleared_tables'] += 1
                        results['cleared_tables'] += 1
                        schema_results['tables'][full_table_name] = 'cleared'
                    else:
                        schema_results['failed_tables'] += 1
                        results['failed_tables'] += 1
                        schema_results['tables'][full_table_name] = 'failed'
                
                results['schema_results'][schema_name] = schema_results
                logger.info(f"   📊 Schema {schema_name}: {schema_results['cleared_tables']} cleared, {schema_results['failed_tables']} failed, {schema_results['skipped_tables']} skipped")
            
            logger.info(f"✅ TPC-DS table clearing completed!")
            logger.info(f"   📊 Total: {results['total_tables']} tables in {results['total_schemas']} schemas")
            logger.info(f"   ✅ Cleared: {results['cleared_tables']} tables")
            logger.info(f"   ❌ Failed: {results['failed_tables']} tables")
            logger.info(f"   📋 Skipped: {results['skipped_tables']} tables")
            if clear_external_s3:
                logger.info(f"   🗑️  S3 paths deleted: {results['s3_paths_deleted']}")
            if refresh_metadata and results.get('metadata_refresh_results'):
                refresh_results = results['metadata_refresh_results']
                logger.info(f"   🔄 Metadata refreshed: {refresh_results.get('refreshed_tables', 0)} tables")
            
        except Exception as e:
            logger.error(f"❌ Error during TPC-DS table clearing: {e}")
            results['error'] = str(e)
        
        finally:
            self.disconnect()
        
        return results
    
    def verify_tpcds_tables_empty(self) -> Dict[str, Any]:
        """Verify that all TPC-DS tables are empty"""
        verification_results = {
            'total_tables': 0,
            'empty_tables': 0,
            'non_empty_tables': 0,
            'table_counts': {}
        }
        
        if not self.connect():
            return verification_results
        
        try:
            logger.info("🔍 Verifying TPC-DS tables are empty...")
            
            for schema_name in self.tpcds_schemas:
                logger.info(f"📁 Checking schema: {schema_name}")
                
                # Check if schema exists
                try:
                    self.cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
                    existing_tables = [row[1] for row in self.cursor.fetchall()]
                except Exception as e:
                    logger.warning(f"   ⚠️  Schema {schema_name} does not exist: {e}")
                    continue
                
                for table_name in self.tpcds_tables:
                    # Construct full table name based on schema format
                    if schema_name == 'tpcds_native_format':
                        full_table_name = f"{table_name}_native"
                    elif schema_name == 'tpcds_iceberg_sf_format':
                        full_table_name = f"{table_name}_iceberg_sf"
                    elif schema_name == 'tpcds_iceberg_glue_format':
                        full_table_name = f"{table_name}_iceberg_glue"
                    elif schema_name == 'tpcds_external_format':
                        full_table_name = f"{table_name}_external"
                    else:
                        full_table_name = table_name
                    
                    # Check if table exists
                    if full_table_name not in existing_tables:
                        continue
                    
                    try:
                        self.cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{full_table_name}")
                        row_count = self.cursor.fetchone()[0]
                        
                        verification_results['total_tables'] += 1
                        verification_results['table_counts'][f"{schema_name}.{full_table_name}"] = row_count
                        
                        if row_count == 0:
                            verification_results['empty_tables'] += 1
                            logger.info(f"   ✅ {schema_name}.{full_table_name}: {row_count} rows")
                        else:
                            verification_results['non_empty_tables'] += 1
                            logger.warning(f"   ⚠️  {schema_name}.{full_table_name}: {row_count} rows (not empty)")
                            
                    except Exception as e:
                        logger.error(f"   ❌ Failed to check {schema_name}.{full_table_name}: {e}")
            
            logger.info(f"🔍 Verification completed!")
            logger.info(f"   📊 Total tables checked: {verification_results['total_tables']}")
            logger.info(f"   ✅ Empty tables: {verification_results['empty_tables']}")
            logger.info(f"   ⚠️  Non-empty tables: {verification_results['non_empty_tables']}")
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")
            verification_results['error'] = str(e)
        
        finally:
            self.disconnect()
        
        return verification_results

def main():
    """Main function"""
    print("🧹 TPC-DS Data Cleaner")
    print("=" * 50)
    
    # Check command line arguments
    clear_external_s3 = True
    refresh_metadata = True
    if len(sys.argv) > 1:
        if sys.argv[1] == '--no-s3':
            clear_external_s3 = False
            print("⚠️  S3 deletion disabled (--no-s3 flag)")
        elif sys.argv[1] == '--no-refresh':
            refresh_metadata = False
            print("⚠️  Metadata refresh disabled (--no-refresh flag)")
        elif sys.argv[1] == '--help':
            print("Usage: python clear_tpcds_data.py [--no-s3] [--no-refresh] [--help]")
            print("  --no-s3: Skip S3 deletion for external tables")
            print("  --no-refresh: Skip metadata refresh for external tables")
            print("  --help: Show this help message")
            return 0
    
    cleaner = TPCDSDataCleaner()
    
    # Clear all TPC-DS tables
    results = cleaner.clear_tpcds_tables(clear_external_s3=clear_external_s3, refresh_metadata=refresh_metadata)
    
    if results.get('error'):
        print(f"❌ Error occurred: {results['error']}")
        return 1
    
    # Verify tables are empty
    print("\n" + "=" * 50)
    verification = cleaner.verify_tpcds_tables_empty()
    
    if verification.get('error'):
        print(f"❌ Verification error: {verification['error']}")
        return 1
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TPC-DS CLEANUP SUMMARY")
    print("=" * 50)
    print(f"Schemas processed: {results['total_schemas']}")
    print(f"Total tables: {results['total_tables']}")
    print(f"Tables cleared: {results['cleared_tables']}")
    print(f"Tables failed: {results['failed_tables']}")
    print(f"Tables skipped: {results['skipped_tables']}")
    if clear_external_s3:
        print(f"S3 paths deleted: {results['s3_paths_deleted']}")
        print(f"S3 deletions failed: {results['s3_deletion_failed']}")
    if refresh_metadata and results.get('metadata_refresh_results'):
        refresh_results = results['metadata_refresh_results']
        print(f"Metadata refreshed: {refresh_results.get('refreshed_tables', 0)}")
        print(f"Metadata refresh failed: {refresh_results.get('failed_refreshes', 0)}")
    print(f"Empty tables verified: {verification['empty_tables']}")
    print(f"Non-empty tables: {verification['non_empty_tables']}")
    
    if verification['non_empty_tables'] > 0:
        print("\n⚠️  Non-empty TPC-DS tables:")
        for table_name, count in verification['table_counts'].items():
            if count > 0:
                print(f"   - {table_name}: {count} rows")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
