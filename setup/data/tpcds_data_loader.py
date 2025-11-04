"""
TPC-DS Data Loader for Snowflake
Loads TPC-DS data into all three schemas: Native, Iceberg SF, Iceberg Glue, and External
"""

import logging
import pandas as pd
import snowflake.connector
from snowflake.connector import DictCursor
from typing import Dict, List, Any, Optional
import time
from contextlib import contextmanager
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import os
import yaml
from pathlib import Path

logger = logging.getLogger(__name__)

class TPCDSDataLoader:
    """Loads TPC-DS data into Snowflake tables across all formats"""
    
    def __init__(self, connector):
        """Initialize TPC-DS data loader with Snowflake connector"""
        self.connector = connector
        
    def load_all_tpcds_data(self, data_dir: str, formats: List[str] = None, refresh_metadata: bool = False) -> Dict[str, Any]:
        """Load all TPC-DS data into Snowflake tables"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0,
            'load_duration': 0
        }
        
        start_time = time.time()
        
        try:
            # Load data from Parquet files
            parquet_dir = os.path.join(data_dir, 'parquet')
            if not os.path.exists(parquet_dir):
                raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
            
            # Load all TPC-DS tables
            tpcds_data = self._load_parquet_files(parquet_dir)
            
            for format_type in formats:
                logger.info(f"Loading TPC-DS data for format: {format_type}")
                format_results = self._load_data_for_format(tpcds_data, format_type, refresh_metadata)
                results['loaded_tables'][format_type] = format_results['loaded_tables']
                results['failed_tables'][format_type] = format_results['failed_tables']
                results['total_rows_loaded'] += format_results['total_rows_loaded']
                
        except Exception as e:
            logger.error(f"TPC-DS data loading failed: {e}")
            results['error'] = str(e)
        
        results['load_duration'] = time.time() - start_time
        logger.info(f"TPC-DS data loading completed in {results['load_duration']:.2f} seconds")
        return results
    
    def _load_parquet_files(self, parquet_dir: str) -> Dict[str, pd.DataFrame]:
        """Load all Parquet files from directory"""
        tpcds_data = {}
        
        for parquet_file in Path(parquet_dir).glob('*.parquet'):
            table_name = parquet_file.stem
            logger.info(f"Loading Parquet file: {table_name}")
            
            try:
                df = pd.read_parquet(parquet_file)
                tpcds_data[table_name] = df
                logger.info(f"Loaded {table_name}: {len(df)} rows")
            except Exception as e:
                logger.error(f"Failed to load {table_name}: {e}")
        
        return tpcds_data
    
    def _load_data_for_format(self, data: Dict[str, pd.DataFrame], format_type: str, refresh_metadata: bool = False) -> Dict[str, Any]:
        """Load data for specific format"""
        format_results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0
        }
        
        try:
            # Handle different formats with different insertion methods
            if format_type in ['iceberg_glue', 'external']:
                # For iceberg_glue and external formats, use S3-based insertion methods
                format_results = self._load_data_alternative_method(data, format_type, refresh_metadata)
            else:
                # For native and iceberg_sf formats, use standard Snowflake insertion
                format_results = self._load_data_snowflake_method(data, format_type)
        
        except Exception as e:
            logger.error(f"Failed to load data for format {format_type}: {e}")
            format_results['error'] = str(e)
        
        return format_results
    
    def _load_data_snowflake_method(self, data: Dict[str, pd.DataFrame], format_type: str) -> Dict[str, Any]:
        """Load data using standard Snowflake insertion method"""
        format_results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0
        }
        
        try:
            # Get connection for this format
            connection = self.connector.get_connection(format_type)
            
            # Load each table
            for table_name, df in data.items():
                try:
                    logger.info(f"Loading {len(df)} rows into {table_name}")
                    
                    # Create table if it doesn't exist
                    self._create_tpcds_table_if_not_exists(connection, table_name, format_type, df)
                    
                    # Load data
                    rows_loaded = self._load_dataframe_to_table(connection, df, table_name, format_type)
                    
                    format_results['loaded_tables'][table_name] = {
                        'rows_loaded': rows_loaded,
                        'status': 'success'
                    }
                    format_results['total_rows_loaded'] += rows_loaded
                    
                    logger.info(f"Successfully loaded {rows_loaded} rows into {table_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to load {table_name}: {e}")
                    format_results['failed_tables'][table_name] = {
                        'error': str(e),
                        'status': 'failed'
                    }
        
        except Exception as e:
            logger.error(f"Failed to load data for format {format_type}: {e}")
            format_results['error'] = str(e)
        
        return format_results
    
    def _load_data_alternative_method(self, data: Dict[str, pd.DataFrame], format_type: str, refresh_metadata: bool = False) -> Dict[str, Any]:
        """Load data using alternative methods for iceberg_glue and external formats"""
        format_results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0
        }
        
        try:
            if format_type == 'iceberg_glue':
                # For iceberg_glue: Write data to S3 as Parquet files for Glue-managed tables
                format_results = self._load_data_to_iceberg_glue(data)
            elif format_type == 'external':
                # For external: Write data to S3 as Parquet files for external tables
                format_results = self._load_data_to_external(data, refresh_metadata)
            else:
                raise ValueError(f"Unsupported format for alternative method: {format_type}")
        
        except Exception as e:
            logger.error(f"Failed to load data for format {format_type}: {e}")
            format_results['error'] = str(e)
        
        return format_results
    
    def _load_data_to_iceberg_glue(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Load data to Iceberg Glue tables by writing Parquet files to S3"""
        format_results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0
        }
        
        try:
            # Load AWS configuration
            aws_config = self._load_aws_config()
            
            # Initialize S3 client - use default credential provider if credentials are empty
            s3_kwargs = {'region_name': aws_config['region']}
            access_key = aws_config.get('credentials', {}).get('access_key_id', '')
            if access_key and access_key.strip():
                s3_kwargs['aws_access_key_id'] = access_key
                s3_kwargs['aws_secret_access_key'] = aws_config['credentials']['secret_access_key']
                if aws_config['credentials'].get('session_token'):
                    s3_kwargs['aws_session_token'] = aws_config['credentials']['session_token']
            # If no credentials provided, boto3 will use default credential provider chain
            
            s3_client = boto3.client('s3', **s3_kwargs)
            
            # Get S3 configuration
            bucket_name = aws_config['s3']['bucket']
            
            for table_name, df in data.items():
                try:
                    logger.info(f"Writing {len(df)} rows to S3 for {table_name}_iceberg_glue")
                    
                    # Convert DataFrame to Parquet
                    parquet_data = self._dataframe_to_parquet(df)
                    
                    # Upload to S3
                    prefix = aws_config.get('s3_prefix', 'iceberg-performance-test')
                    s3_key = f"{prefix}/iceberg_glue_format/{table_name}_iceberg_glue/data.parquet"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=parquet_data,
                        ContentType='application/octet-stream'
                    )
                    
                    format_results['loaded_tables'][table_name] = {
                        'rows_loaded': len(df),
                        'status': 'success',
                        's3_location': f"s3://{bucket_name}/{s3_key}"
                    }
                    format_results['total_rows_loaded'] += len(df)
                    
                    logger.info(f"Successfully wrote {len(df)} rows to S3 for {table_name}_iceberg_glue")
                    
                except Exception as e:
                    logger.error(f"Failed to write {table_name}_iceberg_glue to S3: {e}")
                    format_results['failed_tables'][table_name] = {
                        'error': str(e),
                        'status': 'failed'
                    }
        
        except Exception as e:
            logger.error(f"Failed to load data to Iceberg Glue: {e}")
            format_results['error'] = str(e)
        
        return format_results
    
    def _load_data_to_external(self, data: Dict[str, pd.DataFrame], refresh_metadata: bool = False) -> Dict[str, Any]:
        """Load data to external tables by writing Parquet files to S3"""
        format_results = {
            'loaded_tables': {},
            'failed_tables': {},
            'total_rows_loaded': 0
        }
        
        try:
            # Load AWS configuration
            aws_config = self._load_aws_config()
            
            # Initialize S3 client - use default credential provider if credentials are empty
            s3_kwargs = {'region_name': aws_config['region']}
            access_key = aws_config.get('credentials', {}).get('access_key_id', '')
            if access_key and access_key.strip():
                s3_kwargs['aws_access_key_id'] = access_key
                s3_kwargs['aws_secret_access_key'] = aws_config['credentials']['secret_access_key']
                if aws_config['credentials'].get('session_token'):
                    s3_kwargs['aws_session_token'] = aws_config['credentials']['session_token']
            # If no credentials provided, boto3 will use default credential provider chain
            
            s3_client = boto3.client('s3', **s3_kwargs)
            
            # Get S3 configuration - use the bucket from stage (iceberg-snowflake-perf)
            # The stage is configured to use iceberg-snowflake-perf, not the config bucket
            # We need to use the same bucket as the stage
            bucket_name = 'iceberg-snowflake-perf'  # Match the stage bucket
            
            for table_name, df in data.items():
                try:
                    logger.info(f"Writing {len(df)} rows to S3 for {table_name}_external")
                    
                    # Convert DataFrame to Parquet
                    parquet_data = self._dataframe_to_parquet(df)
                    
                    # Upload to S3 - use exact stage URL path (tcp-ds-data/ matches the stage URL)
                    # Stage URL is s3://iceberg-snowflake-perf/tcp-ds-data/, external table path is tpcds_external_format/{table_name}_external/
                    # Files should be directly in the directory, not in a subdirectory
                    # Use table name as filename (Snowflake will read all .parquet files in the directory)
                    s3_key = f"tcp-ds-data/tpcds_external_format/{table_name}_external/{table_name}.parquet"
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=parquet_data,
                        ContentType='application/octet-stream'
                    )
                    
                    format_results['loaded_tables'][table_name] = {
                        'rows_loaded': len(df),
                        'status': 'success',
                        's3_location': f"s3://{bucket_name}/{s3_key}"
                    }
                    format_results['total_rows_loaded'] += len(df)
                    
                    logger.info(f"Successfully wrote {len(df)} rows to S3 for {table_name}_external")
                    
                except Exception as e:
                    logger.error(f"Failed to write {table_name}_external to S3: {e}")
                    format_results['failed_tables'][table_name] = {
                        'error': str(e),
                        'status': 'failed'
                    }
            
            # Refresh external table metadata if requested
            if refresh_metadata and format_results['loaded_tables']:
                logger.info("🔄 Refreshing external table metadata...")
                refresh_results = self.refresh_external_tables()
                format_results['metadata_refresh'] = refresh_results
        
        except Exception as e:
            logger.error(f"Failed to load data to external tables: {e}")
            format_results['error'] = str(e)
        
        return format_results
    
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
            
            with self.connector.get_connection('external') as connection:
                with connection.cursor() as cursor:
                    # Get all external tables in TPC-DS schema
                    cursor.execute("SHOW EXTERNAL TABLES IN SCHEMA tpcds_external_format")
                    external_tables = cursor.fetchall()
                    
                    for table_info in external_tables:
                        table_name = table_info[1]
                        refresh_results['total_tables'] += 1
                        
                        try:
                            # Refresh the external table metadata
                            cursor.execute(f"ALTER EXTERNAL TABLE tpcds_external_format.{table_name} REFRESH")
                            refresh_results['refreshed_tables'] += 1
                            refresh_results['table_results'][table_name] = 'refreshed'
                            logger.info(f"   ✅ Refreshed metadata for {table_name}")
                            
                        except Exception as e:
                            refresh_results['failed_refreshes'] += 1
                            refresh_results['table_results'][table_name] = f'failed: {str(e)}'
                            logger.error(f"   ❌ Failed to refresh {table_name}: {e}")
            
            logger.info(f"External table metadata refresh completed: {refresh_results['refreshed_tables']}/{refresh_results['total_tables']} tables refreshed")
            
        except Exception as e:
            logger.error(f"Failed to refresh external table metadata: {e}")
            refresh_results['error'] = str(e)
        
        return refresh_results
    
    def _dataframe_to_parquet(self, df: pd.DataFrame) -> bytes:
        """Convert DataFrame to Parquet format in memory"""
        try:
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Write to Parquet in memory
            buffer = BytesIO()
            pq.write_table(table, buffer)
            buffer.seek(0)
            
            return buffer.getvalue()
        
        except Exception as e:
            logger.error(f"Failed to convert DataFrame to Parquet: {e}")
            raise
    
    def _load_aws_config(self) -> Dict[str, Any]:
        """Load AWS configuration from config file"""
        import boto3
        try:
            config_path = 'config/aws_config.yaml'
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Replace environment variables in config
            import os
            def replace_env_vars(obj):
                if isinstance(obj, dict):
                    return {k: replace_env_vars(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [replace_env_vars(item) for item in obj]
                elif isinstance(obj, str) and obj.startswith('${') and obj.endswith('}'):
                    env_var = obj[2:-1]
                    value = os.getenv(env_var, '')
                    # Return empty string if env var not set, so we can use default provider
                    return value if value else ''
                else:
                    return obj
            
            config = replace_env_vars(config)
            
            # Use default credential provider chain if credentials not explicitly set or are empty
            access_key = config.get('credentials', {}).get('access_key_id', '')
            if not access_key or access_key.strip() == '' or access_key.startswith('${'):
                # Use credentials from default provider chain
                session = boto3.Session()
                credentials = session.get_credentials()
                if credentials:
                    config['credentials'] = {
                        'access_key_id': credentials.access_key,
                        'secret_access_key': credentials.secret_key,
                        'session_token': credentials.token if hasattr(credentials, 'token') else None
                    }
            
            return config
        
        except Exception as e:
            logger.error(f"Failed to load AWS configuration: {e}")
            # Return default configuration using boto3 credential provider
            session = boto3.Session()
            credentials = session.get_credentials()
            return {
                'region': os.getenv('AWS_REGION', session.region_name or 'us-east-1'),
                'credentials': {
                    'access_key_id': credentials.access_key if credentials else '',
                    'secret_access_key': credentials.secret_key if credentials else '',
                    'session_token': credentials.token if credentials and hasattr(credentials, 'token') else None
                },
                's3': {
                    'bucket': os.getenv('AWS_S3_BUCKET', 'your-bucket-name'),
                    'prefix': os.getenv('AWS_S3_PREFIX', 'iceberg-performance-test')
                }
            }
    
    def _create_tpcds_table_if_not_exists(self, connection, table_name: str, format_type: str, df: pd.DataFrame):
        """Create TPC-DS table if it doesn't exist"""
        try:
            with connection.cursor() as cursor:
                # Set database and schema context
                import os
                database = os.getenv('SNOWFLAKE_DATABASE', 'tpcds_performance_test')
                cursor.execute(f"USE DATABASE {database}")
                
                # Map format types to their corresponding schema names
                schema_mapping = {
                    'native': 'tpcds_native_format',
                    'iceberg_sf': 'tpcds_iceberg_sf_format', 
                    'iceberg_glue': 'tpcds_iceberg_glue_format',
                    'external': 'tpcds_external_format'
                }
                
                schema_name = schema_mapping.get(format_type, f"tpcds_{format_type}_format")
                cursor.execute(f"USE SCHEMA {schema_name}")
                
                # Check if table exists
                check_query = f"SHOW TABLES LIKE '{table_name}'"
                cursor.execute(check_query)
                existing_tables = cursor.fetchall()
                
                if not existing_tables:
                    # Create table based on DataFrame schema
                    create_sql = self._generate_tpcds_create_table_sql(table_name, format_type, df)
                    logger.info(f"Creating TPC-DS table: {table_name}")
                    cursor.execute(create_sql)
                    logger.info(f"Table {table_name} created successfully")
                else:
                    logger.info(f"Table {table_name} already exists")
                    
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise
    
    def _generate_tpcds_create_table_sql(self, table_name: str, format_type: str, df: pd.DataFrame) -> str:
        """Generate CREATE TABLE SQL for TPC-DS tables"""
        columns = []
        
        for col_name, dtype in df.dtypes.items():
            if dtype == 'object':
                col_type = 'STRING'
            elif dtype == 'int64':
                col_type = 'NUMBER'
            elif dtype == 'float64':
                col_type = 'FLOAT'
            elif dtype == 'bool':
                col_type = 'BOOLEAN'
            elif 'datetime' in str(dtype):
                col_type = 'TIMESTAMP_NTZ'
            else:
                col_type = 'STRING'
            
            columns.append(f"{col_name} {col_type}")
        
        # Add primary key constraint for TPC-DS tables
        primary_key_columns = self._get_tpcds_primary_key(table_name)
        if primary_key_columns:
            columns.append(f"PRIMARY KEY ({', '.join(primary_key_columns)})")
        
        if format_type == 'external':
            # Create external table that reads Parquet files directly
            create_sql = f"""
            CREATE OR REPLACE EXTERNAL TABLE {table_name} (
                {', '.join(columns)}
            )
            LOCATION = @tpcds_external_stage/tpcds_external_format/{table_name}/
            FILE_FORMAT = (TYPE = 'PARQUET')
            AUTO_REFRESH = TRUE
            """
        elif format_type == 'iceberg_sf':
            # Create Iceberg Snowflake-managed table
            create_sql = f"""
            CREATE OR REPLACE ICEBERG TABLE {table_name} (
                {', '.join(columns)}
            )
            EXTERNAL_VOLUME = 'iceberg_volume'
            CATALOG = 'SNOWFLAKE'
            BASE_LOCATION = 'iceberg_sf/{table_name}'
            """
        elif format_type == 'iceberg_glue':
            # Create Iceberg AWS Glue-managed table
            create_sql = f"""
            CREATE OR REPLACE ICEBERG TABLE {table_name} (
                {', '.join(columns)}
            )
            EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
            CATALOG = 'AWS_GLUE_CATALOG'
            BASE_LOCATION = 'iceberg_glue/{table_name}'
            """
        else:
            # Create native Snowflake table
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)}
            )
            """
        
        return create_sql
    
    def _get_tpcds_primary_key(self, table_name: str) -> List[str]:
        """Get primary key columns for TPC-DS tables"""
        primary_keys = {
            'call_center': ['cc_call_center_sk'],
            'catalog_page': ['cp_catalog_page_sk'],
            'customer': ['c_customer_sk'],
            'customer_address': ['ca_address_sk'],
            'customer_demographics': ['cd_demo_sk'],
            'date_dim': ['d_date_sk'],
            'household_demographics': ['hd_demo_sk'],
            'income_band': ['ib_income_band_sk'],
            'item': ['i_item_sk'],
            'promotion': ['p_promo_sk'],
            'reason': ['r_reason_sk'],
            'ship_mode': ['sm_ship_mode_sk'],
            'store': ['s_store_sk'],
            'time_dim': ['t_time_sk'],
            'warehouse': ['w_warehouse_sk'],
            'web_page': ['wp_web_page_sk'],
            'web_site': ['web_site_sk'],
            'inventory': ['inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk']
        }
        
        return primary_keys.get(table_name, [])
    
    def _load_dataframe_to_table(self, connection, df: pd.DataFrame, table_name: str, format_type: str) -> int:
        """Load DataFrame data into Snowflake table"""
        try:
            with connection.cursor() as cursor:
                # Set database and schema context
                import os
                database = os.getenv('SNOWFLAKE_DATABASE', 'tpcds_performance_test')
                cursor.execute(f"USE DATABASE {database}")
                
                # Map format types to their corresponding schema names
                schema_mapping = {
                    'native': 'tpcds_native_format',
                    'iceberg_sf': 'tpcds_iceberg_sf_format', 
                    'iceberg_glue': 'tpcds_iceberg_glue_format',
                    'external': 'tpcds_external_format'
                }
                
                schema_name = schema_mapping.get(format_type, f"tpcds_{format_type}_format")
                cursor.execute(f"USE SCHEMA {schema_name}")
                
                # Convert timestamp columns to strings and handle NaN/NaT values
                df_copy = df.copy()
                timestamp_columns = set()
                
                for col in df_copy.columns:
                    if df_copy[col].dtype == 'datetime64[ns]':
                        # Convert datetime to string, handling NaT values
                        df_copy[col] = df_copy[col].astype(str)
                        df_copy[col] = df_copy[col].replace('NaT', None)
                        timestamp_columns.add(col)
                    elif 'datetime' in str(df_copy[col].dtype):
                        df_copy[col] = df_copy[col].astype(str)
                        df_copy[col] = df_copy[col].replace('nan', None)
                        timestamp_columns.add(col)
                
                # Handle NaN values by replacing with NULL or empty string
                for col in df_copy.columns:
                    if col in timestamp_columns:
                        # Don't convert None to empty string for timestamp columns
                        continue
                    elif df_copy[col].dtype == 'object':
                        df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), '')
                    else:
                        df_copy[col] = df_copy[col].where(pd.notna(df_copy[col]), 0)
                
                # Convert DataFrame to list of tuples for bulk insert
                data_tuples = [tuple(row) for row in df_copy.values]
                
                # Generate INSERT statement
                columns = ', '.join(df_copy.columns)
                placeholders = ', '.join(['%s'] * len(df_copy.columns))
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                
                # Execute bulk insert
                cursor.executemany(insert_sql, data_tuples)
                
                # Get number of rows inserted
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                row_count = cursor.fetchone()[0]
                
                return row_count
                
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise
    
    def verify_tpcds_data_loaded(self, formats: List[str] = None) -> Dict[str, Any]:
        """Verify that TPC-DS data was loaded correctly"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        verification_results = {
            'format_verification': {},
            'table_counts': {},
            'all_verified': True
        }
        
        try:
            for format_type in formats:
                format_verification = {}
                table_counts = {}
                
                try:
                    connection = self.connector.get_connection(format_type)
                    
                    # List of TPC-DS tables
                    tpcds_tables = [
                        'call_center', 'catalog_page', 'customer', 'customer_address',
                        'customer_demographics', 'date_dim', 'household_demographics',
                        'income_band', 'item', 'promotion', 'reason', 'ship_mode',
                        'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
                        'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
                        'store_sales', 'web_returns', 'web_sales'
                    ]
                    
                    with connection.cursor() as cursor:
                        for table_name in tpcds_tables:
                            try:
                                # Check if table exists and has data
                                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                                count = cursor.fetchone()[0]
                                table_counts[table_name] = count
                                
                                if count > 0:
                                    format_verification[table_name] = 'success'
                                else:
                                    format_verification[table_name] = 'empty'
                                    verification_results['all_verified'] = False
                                    
                            except Exception as e:
                                format_verification[table_name] = f'error: {str(e)}'
                                verification_results['all_verified'] = False
                    
                    verification_results['format_verification'][format_type] = format_verification
                    verification_results['table_counts'][format_type] = table_counts
                    
                except Exception as e:
                    logger.error(f"Verification failed for format {format_type}: {e}")
                    verification_results['format_verification'][format_type] = {'error': str(e)}
                    verification_results['all_verified'] = False
        
        except Exception as e:
            logger.error(f"TPC-DS data verification failed: {e}")
            verification_results['error'] = str(e)
            verification_results['all_verified'] = False
        
        return verification_results
    
    def cleanup_tpcds_data(self, formats: List[str] = None) -> Dict[str, Any]:
        """Clean up TPC-DS data from all formats"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        cleanup_results = {
            'cleaned_formats': [],
            'failed_cleanups': [],
            'tables_cleaned': 0
        }
        
        try:
            for format_type in formats:
                try:
                    connection = self.connector.get_connection(format_type)
                    
                    # TPC-DS tables
                    tpcds_tables = [
                        'call_center', 'catalog_page', 'customer', 'customer_address',
                        'customer_demographics', 'date_dim', 'household_demographics',
                        'income_band', 'item', 'promotion', 'reason', 'ship_mode',
                        'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
                        'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
                        'store_sales', 'web_returns', 'web_sales'
                    ]
                    
                    with connection.cursor() as cursor:
                        for table_name in tpcds_tables:
                            try:
                                # Delete data instead of dropping tables
                                cursor.execute(f"DELETE FROM {table_name}")
                                cleanup_results['tables_cleaned'] += 1
                            except Exception as e:
                                logger.warning(f"Failed to delete data from {table_name}: {e}")
                    
                    cleanup_results['cleaned_formats'].append(format_type)
                    logger.info(f"Cleanup completed for format: {format_type}")
                    
                except Exception as e:
                    logger.error(f"Cleanup failed for format {format_type}: {e}")
                    cleanup_results['failed_cleanups'].append({
                        'format': format_type,
                        'error': str(e)
                    })
        
        except Exception as e:
            logger.error(f"TPC-DS data cleanup failed: {e}")
            cleanup_results['error'] = str(e)
        
        return cleanup_results


def main():
    """Main function for command-line usage"""
    import argparse
    import sys
    import os
    
    # Add the project root to the Python path
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    
    from tests.snowflake_connector import SnowflakeConnector
    
    parser = argparse.ArgumentParser(description='TPC-DS Data Loader')
    parser.add_argument('--data-dir', required=True, help='Directory containing TPC-DS Parquet data')
    parser.add_argument('--action', choices=['load', 'verify', 'cleanup'], default='load', 
                       help='Action to perform')
    parser.add_argument('--formats', nargs='+', default=['native', 'iceberg_sf', 'iceberg_glue', 'external'], 
                       help='Formats to process')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Initialize connector and data loader
        connector = SnowflakeConnector()
        data_loader = TPCDSDataLoader(connector)
        
        if args.action == 'load':
            # Load TPC-DS data
            results = data_loader.load_all_tpcds_data(args.data_dir, args.formats)
            print(f"\n📊 TPC-DS Data Loading Results:")
            print(f"Total rows loaded: {results['total_rows_loaded']:,}")
            print(f"Duration: {results['load_duration']:.2f} seconds")
            
            for format_type, format_results in results['loaded_tables'].items():
                print(f"\n{format_type}:")
                for table_name, table_info in format_results.items():
                    print(f"  {table_name}: {table_info['rows_loaded']} rows")
        
        elif args.action == 'verify':
            # Verify data
            results = data_loader.verify_tpcds_data_loaded(args.formats)
            print(f"\n🔍 TPC-DS Data Verification Results:")
            print(f"All verified: {results['all_verified']}")
            
            for format_type, table_counts in results['table_counts'].items():
                print(f"\n{format_type}:")
                for table_name, count in table_counts.items():
                    print(f"  {table_name}: {count} rows")
        
        elif args.action == 'cleanup':
            # Cleanup data
            results = data_loader.cleanup_tpcds_data(args.formats)
            print(f"\n🧹 TPC-DS Data Cleanup Results:")
            print(f"Tables cleaned: {results['tables_cleaned']}")
            print(f"Formats cleaned: {len(results['cleaned_formats'])}")
        
        print(f"\n✅ {args.action.title()} completed successfully!")
        
    except Exception as e:
        logger.error(f"TPC-DS data loader execution failed: {e}")
        exit(1)


if __name__ == '__main__':
    main()
