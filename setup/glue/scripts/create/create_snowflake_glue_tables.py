#!/usr/bin/env python3
"""
Create Snowflake Iceberg table references for Glue-managed tables
This script:
1. Gets metadata locations from Glue tables via Spark
2. Creates/updates Snowflake table references
3. Verifies tables are accessible from Snowflake
"""

import os
import sys
import yaml
import logging
import re
from pathlib import Path

# Add project root to path BEFORE importing lib
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
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
    
    spark_conf = {
        "spark.app.name": "Get Glue Metadata",
        "spark.master": spark_config['spark']['master']
    }
    
    spark_conf.update(spark_config['spark']['config'])
    
    jar_files = []
    for jar_path in spark_config['jars']:
        full_jar_path = project_root / jar_path
        if full_jar_path.exists():
            jar_files.append(str(full_jar_path))
    
    if jar_files:
        spark_conf["spark.jars"] = ",".join(jar_files)
    
    spark = SparkSession.builder
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def verify_glue_table_exists(spark, table_name, glue_config):
    """Verify Glue table exists and is accessible"""
    logger = logging.getLogger(__name__)
    
    try:
        catalog_name = "glue_catalog"
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        # Try to query the table to verify it exists
        df = spark.table(full_table_name)
        row_count = df.count()
        
        logger.info(f"  ✅ Table exists: {table_name} ({row_count} rows)")
        return {'success': True, 'row_count': row_count}
            
    except Exception as e:
        logger.error(f"  ❌ Error verifying Glue table {table_name}: {e}")
        return {'success': False, 'error': str(e)}

def create_snowflake_table(sf_conn, table_name, glue_config):
    """Create Snowflake Iceberg table reference using CATALOG_TABLE_NAME"""
    logger = logging.getLogger(__name__)
    
    try:
        cursor = sf_conn.cursor()
        
        # Get Snowflake schema name
        schemas = get_snowflake_schemas()
        schema_name = schemas.get('iceberg_glue', 'tpcds_iceberg_glue_format')
        
        # Get external volume and catalog names from config
        external_volume = os.getenv('SNOWFLAKE_EXTERNAL_VOLUME_GLUE', 'ICEBERG_GLUE_VOLUME')
        catalog_name = os.getenv('SNOWFLAKE_GLUE_CATALOG', 'AWS_GLUE_CATALOG')
        
        # Build CREATE TABLE SQL
        # Using CATALOG_TABLE_NAME - Snowflake will query Glue catalog directly
        # No need for METADATA_LOCATION when using CATALOG_TABLE_NAME
        create_sql = f"""
        CREATE OR REPLACE ICEBERG TABLE {schema_name}.{table_name}
        EXTERNAL_VOLUME = '{external_volume}'
        CATALOG = '{catalog_name}'
        CATALOG_TABLE_NAME = '{table_name}'
        COMMENT = 'Glue-managed Iceberg table: {table_name}';
        """
        
        logger.info(f"Creating Snowflake table: {schema_name}.{table_name}")
        cursor.execute(create_sql)
        
        # Refresh metadata to sync with Glue
        try:
            refresh_sql = f"ALTER ICEBERG TABLE {schema_name}.{table_name} REFRESH;"
            cursor.execute(refresh_sql)
        except Exception as refresh_err:
            logger.warning(f"  ⚠️  Could not refresh table {table_name}: {refresh_err}")
        
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating Snowflake table {table_name}: {e}")
        return False

def verify_snowflake_table(sf_conn, table_name):
    """Verify table exists and get row count from Snowflake"""
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
        logger.error(f"Error verifying Snowflake table {table_name}: {e}")
        return {
            'success': False,
            'error': str(e),
            'row_count': 0,
            'column_count': 0
        }

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🚀 Creating Snowflake references for Glue-managed Iceberg tables...")
    
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
        # Load configs
        spark_config, glue_config = load_config()
        logger.info("✅ Configuration loaded")
        
        # Create Spark session
        logger.info("Creating Spark session to get metadata locations...")
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created")
        
        # Verify Glue tables exist
        logger.info(f"\n{'='*80}")
        logger.info("STEP 1: Verifying Glue tables exist and are accessible...")
        logger.info(f"{'='*80}")
        
        verified_tables = []
        for table_name in tpcds_tables:
            logger.info(f"Verifying: {table_name}")
            result = verify_glue_table_exists(spark, table_name, glue_config)
            if result['success']:
                verified_tables.append(table_name)
        
        spark.stop()
        
        if len(verified_tables) != len(tpcds_tables):
            logger.warning(f"⚠️  Only {len(verified_tables)}/{len(tpcds_tables)} tables verified in Glue")
        
        # Connect to Snowflake
        logger.info(f"\n{'='*80}")
        logger.info("STEP 2: Creating Snowflake table references...")
        logger.info(f"{'='*80}")
        
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
        
        # Set schema
        cursor = sf_conn.cursor()
        cursor.execute(f"USE DATABASE {sf_config['database']}")
        cursor.execute(f"USE SCHEMA {schema_name}")
        cursor.close()
        
        logger.info(f"✅ Connected to Snowflake (schema: {schema_name})")
        
        # Create Snowflake tables
        created_count = 0
        for table_name in tpcds_tables:
            if table_name in verified_tables:
                if create_snowflake_table(sf_conn, table_name, glue_config):
                    created_count += 1
                    logger.info(f"  ✅ Created: {table_name}")
                else:
                    logger.error(f"  ❌ Failed to create: {table_name}")
            else:
                logger.warning(f"  ⚠️  Skipping {table_name} (not verified in Glue)")
        
        # Verify tables in Snowflake
        logger.info(f"\n{'='*80}")
        logger.info("STEP 3: Verifying tables in Snowflake...")
        logger.info(f"{'='*80}")
        
        verification_results = []
        total_rows = 0
        
        for table_name in tpcds_tables:
            result = verify_snowflake_table(sf_conn, table_name)
            result['table'] = table_name
            verification_results.append(result)
            
            if result['success']:
                total_rows += result['row_count']
                logger.info(f"✅ {table_name:25} | Rows: {result['row_count']:>8,} | Columns: {result['column_count']:>3}")
            else:
                logger.error(f"❌ {table_name:25} | ERROR: {result.get('error', 'Unknown')}")
        
        # Summary
        logger.info(f"\n{'='*80}")
        logger.info("SUMMARY")
        logger.info(f"{'='*80}")
        
        successful = sum(1 for r in verification_results if r['success'])
        failed = len(verification_results) - successful
        
        logger.info(f"Glue tables verified: {len(verified_tables)}/{len(tpcds_tables)}")
        logger.info(f"Snowflake tables created: {created_count}/{len(tpcds_tables)}")
        logger.info(f"Tables verified in Snowflake: {successful}/{len(tpcds_tables)}")
        logger.info(f"Total rows in Snowflake: {total_rows:,}")
        
        if successful == len(tpcds_tables):
            logger.info("\n🎉 All tables successfully integrated with Snowflake!")
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
        if 'sf_conn' in locals():
            sf_conn.close()
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    exit(main())
