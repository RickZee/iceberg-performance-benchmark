"""
Snowflake Connector for TPC-DS Data Loading
Provides connection management for different table formats
"""

import logging
import snowflake.connector
from typing import Dict, Any
from lib.env import get_snowflake_config, get_snowflake_schemas

logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """Snowflake connector with format-specific connection management"""
    
    def __init__(self):
        """Initialize Snowflake connector"""
        self.config = get_snowflake_config()
        self.schemas = get_snowflake_schemas()
        self._connections = {}
        
        logger.info("SnowflakeConnector initialized")
    
    def get_connection(self, format_type: str):
        """Get or create a connection for a specific format"""
        if format_type in self._connections:
            return self._connections[format_type]
        
        # Create new connection
        try:
            conn = snowflake.connector.connect(
                user=self.config['user'],
                account=self.config['account'],
                warehouse=self.config['warehouse'],
                database=self.config['database'],
                role=self.config['role'],
                private_key_file=self.config['private_key_file'],
                private_key_passphrase=self.config['private_key_passphrase'] if self.config['private_key_passphrase'] else None
            )
            
            # Set schema based on format
            schema_name = self.schemas.get(format_type, f'tpcds_{format_type}_format')
            with conn.cursor() as cursor:
                cursor.execute(f"USE DATABASE {self.config['database']}")
                cursor.execute(f"USE SCHEMA {schema_name}")
            
            self._connections[format_type] = conn
            logger.info(f"Created connection for format: {format_type} (schema: {schema_name})")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to create connection for {format_type}: {e}")
            raise
    
    def close_all_connections(self):
        """Close all connections"""
        for format_type, conn in self._connections.items():
            try:
                conn.close()
                logger.info(f"Closed connection for {format_type}")
            except Exception as e:
                logger.error(f"Error closing connection for {format_type}: {e}")
        
        self._connections.clear()
    
    def execute_query(self, query, format_name):
        """Execute a query and return results"""
        try:
            conn = self.get_connection(format_name)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Query execution failed for {format_name}: {e}")
            raise
    
    def execute_query_with_metrics(self, query, format_name, query_number):
        """Execute query and capture both local and Snowflake native metrics"""
        import time
        
        try:
            conn = self.get_connection(format_name)
            cursor = conn.cursor()
            
            # Generate unique query tag for tracking
            query_tag = f"iceberg_performance_test_{format_name}_q{query_number}_{int(time.time())}"
            cursor.execute(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'")
            
            # Execute query and measure local time
            start_time = time.time()
            cursor.execute(query)
            results = cursor.fetchall()
            end_time = time.time()
            
            # Get query ID for metrics lookup
            query_id = cursor.sfqid
            local_execution_time = end_time - start_time
            
            cursor.close()
            
            # Wait for query to appear in query history (ACCOUNT_USAGE can have a delay)
            # Retry up to 3 times with increasing delays
            snowflake_metrics = None
            for attempt in range(3):
                time.sleep(2 + attempt)  # 2, 3, 4 seconds
                snowflake_metrics = self._get_snowflake_metrics(conn, query_id)
                if snowflake_metrics:
                    break
            
            if not snowflake_metrics:
                logger.warning(f"Could not retrieve Snowflake metrics for query {query_id} after 3 attempts")
            
            return {
                'results': results,
                'query_id': query_id,
                'query_tag': query_tag,
                'local_execution_time': local_execution_time,
                'snowflake_metrics': snowflake_metrics
            }
            
        except Exception as e:
            logger.error(f"Query execution failed for {format_name}: {e}")
            raise
    
    def get_table_row_counts(self):
        """Get row counts for all TPC-DS tables across all schemas"""
        try:
            # Use the first available connection
            conn = None
            for format_type in ['native', 'iceberg_sf', 'iceberg_glue', 'external']:
                try:
                    conn = self.get_connection(format_type)
                    break
                except:
                    continue
            
            if not conn:
                logger.error("No connection available")
                return {}
            
            cursor = conn.cursor()
            
            # Define all schemas and their table prefixes
            schemas = {
                'native': f"{self.config['database']}.{self.schemas.get('native', 'tpcds_native_format')}",
                'iceberg_sf': f"{self.config['database']}.{self.schemas.get('iceberg_sf', 'tpcds_iceberg_sf_format')}",
                'iceberg_glue': f"{self.config['database']}.{self.schemas.get('iceberg_glue', 'tpcds_iceberg_glue_format')}",
                'external': f"{self.config['database']}.{self.schemas.get('external', 'tpcds_external_format')}"
            }
            
            # TPC-DS table names
            table_names = [
                'call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
                'customer_address', 'customer_demographics', 'customer',
                'date_dim', 'household_demographics', 'income_band', 'inventory',
                'item', 'promotion', 'reason', 'ship_mode', 'store_returns',
                'store_sales', 'store', 'time_dim', 'warehouse', 'web_page',
                'web_returns', 'web_sales', 'web_site'
            ]
            
            row_counts = {}
            
            for schema_name, full_schema in schemas.items():
                row_counts[schema_name] = {}
                
                for table_name in table_names:
                    try:
                        # Check if table exists and get row count
                        query = f"""
                        SELECT COUNT(*) as row_count
                        FROM {full_schema}.{table_name}
                        """
                        cursor.execute(query)
                        result = cursor.fetchone()
                        row_counts[schema_name][table_name] = result[0] if result else 0
                        
                    except Exception as e:
                        # Table might not exist in this schema
                        logger.debug(f"Table {table_name} not found in {schema_name}: {e}")
                        row_counts[schema_name][table_name] = 0
            
            cursor.close()
            return row_counts
            
        except Exception as e:
            logger.error(f"Failed to get table row counts: {e}")
            return {}
    
    def _get_snowflake_metrics(self, conn, query_id):
        """Retrieve Snowflake native metrics from query history"""
        try:
            cursor = conn.cursor()
            
            # Try INFORMATION_SCHEMA first (immediate, session-level)
            try:
                metrics_query = f"""
                SELECT 
                    QUERY_ID,
                    WAREHOUSE_NAME,
                    WAREHOUSE_SIZE,
                    EXECUTION_STATUS,
                    START_TIME,
                    END_TIME,
                    TOTAL_ELAPSED_TIME,
                    BYTES_SCANNED,
                    ROWS_PRODUCED,
                    QUERY_TAG
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                WHERE QUERY_ID = '{query_id}'
                AND START_TIME >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
                ORDER BY START_TIME DESC
                LIMIT 1
                """
                
                cursor.execute(metrics_query)
                result = cursor.fetchone()
                
                if result:
                    # Map result to dictionary
                    columns = [desc[0] for desc in cursor.description]
                    metrics = dict(zip(columns, result))
                    
                    # Calculate additional derived metrics
                    metrics['total_elapsed_time_ms'] = metrics.get('TOTAL_ELAPSED_TIME', 0)
                    metrics['bytes_scanned_mb'] = (metrics.get('BYTES_SCANNED', 0) or 0) / (1024 * 1024)
                    metrics['cache_hit_ratio'] = 0  # INFORMATION_SCHEMA doesn't provide this
                    
                    # Convert datetime objects to strings for JSON serialization
                    if metrics.get('START_TIME') and hasattr(metrics['START_TIME'], 'isoformat'):
                        metrics['START_TIME'] = metrics['START_TIME'].isoformat()
                    if metrics.get('END_TIME') and hasattr(metrics['END_TIME'], 'isoformat'):
                        metrics['END_TIME'] = metrics['END_TIME'].isoformat()
                    
                    logger.debug(f"Found metrics via INFORMATION_SCHEMA for query {query_id}")
                    cursor.close()
                    return metrics
            except Exception as e:
                logger.debug(f"INFORMATION_SCHEMA query failed: {e}, trying ACCOUNT_USAGE...")
            
            # Fallback to ACCOUNT_USAGE (has delay but more comprehensive)
            metrics_query = f"""
            SELECT 
                QUERY_ID,
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE,
                EXECUTION_STATUS,
                START_TIME,
                END_TIME,
                TOTAL_ELAPSED_TIME,
                BYTES_SCANNED,
                ROWS_PRODUCED,
                PERCENTAGE_SCANNED_FROM_CACHE,
                QUERY_TAG
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
            WHERE QUERY_ID = '{query_id}'
            AND START_TIME >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
            ORDER BY START_TIME DESC
            LIMIT 1
            """
            
            cursor.execute(metrics_query)
            result = cursor.fetchone()
            
            if result:
                # Map result to dictionary
                columns = [desc[0] for desc in cursor.description]
                metrics = dict(zip(columns, result))
                
                # Calculate additional derived metrics
                metrics['total_elapsed_time_ms'] = metrics.get('TOTAL_ELAPSED_TIME', 0)
                metrics['bytes_scanned_mb'] = (metrics.get('BYTES_SCANNED', 0) or 0) / (1024 * 1024)
                metrics['cache_hit_ratio'] = (metrics.get('PERCENTAGE_SCANNED_FROM_CACHE', 0) or 0) / 100
                
                # Convert datetime objects to strings for JSON serialization
                if metrics.get('START_TIME') and hasattr(metrics['START_TIME'], 'isoformat'):
                    metrics['START_TIME'] = metrics['START_TIME'].isoformat()
                if metrics.get('END_TIME') and hasattr(metrics['END_TIME'], 'isoformat'):
                    metrics['END_TIME'] = metrics['END_TIME'].isoformat()
                
                cursor.close()
                return metrics
            else:
                logger.debug(f"No metrics found for query {query_id} in ACCOUNT_USAGE.QUERY_HISTORY")
                cursor.close()
                return None
                
        except Exception as e:
            logger.error(f"Failed to retrieve query metrics: {e}")
            if cursor:
                cursor.close()
            return None

