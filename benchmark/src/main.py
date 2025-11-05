#!/usr/bin/env python3
"""
TPC-DS Performance Testing Application
Comprehensive performance testing suite for TPC-DS queries across all Snowflake table formats
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import statistics
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import cost tracking modules
try:
    from benchmark.src.aws_cost_tracker import AWSCostTracker
except ImportError:
    try:
        from src.aws_cost_tracker import AWSCostTracker
    except ImportError:
        AWSCostTracker = None

# Import Snowflake connector
try:
    from tests.snowflake_connector import SnowflakeConnector
except ImportError:
    # Fallback: Create a simple SnowflakeConnector class
    import snowflake.connector
    
    class SnowflakeConnector:
        """Simple Snowflake connector for testing"""
        
        def __init__(self):
            self.connection = None
            self._connect()
        
        def _connect(self):
            """Connect to Snowflake"""
            try:
                self.connection = snowflake.connector.connect(
                    user=os.getenv('SNOWFLAKE_USER'),
                    account=os.getenv('SNOWFLAKE_ACCOUNT'),
                    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                    database=os.getenv('SNOWFLAKE_DATABASE'),
                    role=os.getenv('SNOWFLAKE_ROLE'),
                    private_key_file=os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE'),
                    private_key_passphrase=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
                )
                logger.info("Connected to Snowflake successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {e}")
                raise
        
        def execute_query(self, query, format_name):
            """Execute a query and return results"""
            try:
                cursor = self.connection.cursor()
                
                # Map format names to schema names
                schema_mapping = {
                    'native': 'tpcds_performance_test.tpcds_native_format',
                    'iceberg_sf': 'tpcds_performance_test.tpcds_iceberg_sf_format',
                    'iceberg_glue': 'tpcds_performance_test.tpcds_iceberg_glue_format',
                    'external': 'tpcds_performance_test.tpcds_external_format',
                    'new_format': 'tpcds_performance_test.tpcds_native_format'  # Use native schema for testing
                }
                
                # Set schema context before executing query
                schema_name = schema_mapping.get(format_name)
                if schema_name:
                    cursor.execute(f"USE SCHEMA {schema_name}")
                
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                return results
            except Exception as e:
                logger.error(f"Query execution failed for {format_name}: {e}")
                raise
        
        def execute_query_with_metrics(self, query, format_name, query_number):
            """Execute query and capture both local and Snowflake native metrics"""
            try:
                cursor = self.connection.cursor()
                
                # Map format names to schema names
                schema_mapping = {
                    'native': 'tpcds_performance_test.tpcds_native_format',
                    'iceberg_sf': 'tpcds_performance_test.tpcds_iceberg_sf_format',
                    'iceberg_glue': 'tpcds_performance_test.tpcds_iceberg_glue_format',
                    'external': 'tpcds_performance_test.tpcds_external_format',
                    'new_format': 'tpcds_performance_test.tpcds_native_format'
                }
                
                # Set schema context before executing query
                schema_name = schema_mapping.get(format_name)
                if schema_name:
                    cursor.execute(f"USE SCHEMA {schema_name}")
                
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
                    snowflake_metrics = self._get_snowflake_metrics(query_id)
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
                cursor = self.connection.cursor()
                
                # Define all schemas and their table prefixes
                schemas = {
                    'native': 'tpcds_performance_test.tpcds_native_format',
                    'iceberg_sf': 'tpcds_performance_test.tpcds_iceberg_sf_format', 
                    'iceberg_glue': 'tpcds_performance_test.tpcds_iceberg_glue_format',
                    'external': 'tpcds_performance_test.tpcds_external_format'
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
        
        def _get_snowflake_metrics(self, query_id):
            """Retrieve Snowflake native metrics from query history"""
            try:
                cursor = self.connection.cursor()
                
                # Try INFORMATION_SCHEMA first (immediate, session-level)
                # This shows queries from current session/warehouse immediately
                # Note: INFORMATION_SCHEMA doesn't have PERCENTAGE_SCANNED_FROM_CACHE
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
                # Use string formatting for query_id (sanitized)
                # Note: query_id is UUID format, safe for string interpolation
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
                    logger.debug(f"No metrics found for query {query_id} in ACCOUNT_USAGE.QUERY_HISTORY (may need more time to appear)")
                    cursor.close()
                    return None
                    
            except Exception as e:
                logger.error(f"Failed to retrieve query metrics: {e}")
                if cursor:
                    cursor.close()
                return None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('benchmark/logs/tpcds_perf_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TPCDSPerformanceTester:
    """Main TPC-DS Performance Testing Application"""
    
    def __init__(self, config_path: str = "benchmark/config/perf_test_config.yaml"):
        """Initialize the performance tester"""
        self.config_path = config_path
        self.config = self._load_config()
        self.connector = None
        self.results = {}
        self.start_time = None
        self.end_time = None
        
        # Initialize AWS cost tracker
        if AWSCostTracker:
            try:
                aws_config = self.config.get('aws', {})
                self.aws_cost_tracker = AWSCostTracker(aws_config)
            except Exception as e:
                logger.warning(f"Failed to initialize AWS cost tracker: {e}")
                self.aws_cost_tracker = None
        else:
            self.aws_cost_tracker = None
        
        # Create results directory
        self.results_dir = Path("results")
        self.reports_dir = Path("results/reports")
        self.results_dir.mkdir(exist_ok=True)
        self.reports_dir.mkdir(exist_ok=True)
        
        # Query directories
        self.queries_dir = Path("benchmark/queries")
        self.formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            import yaml
            with open(self.config_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except FileNotFoundError:
            logger.warning(f"Configuration file not found: {self.config_path}")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            'test_settings': {
                'max_queries_per_format': 99,
                'max_execution_time': 300,  # 5 minutes
                'retry_attempts': 3,
                'warmup_runs': 1,
                'test_runs': 3
            },
            'performance_thresholds': {
                'max_execution_time': 300,
                'memory_limit_mb': 1024,
                'error_rate_threshold': 0.05
            },
            'reporting': {
                'generate_html': True,
                'generate_pdf': False,
                'generate_csv': True,
                'include_charts': True
            }
        }
    
    def initialize_connector(self) -> bool:
        """Initialize Snowflake connector"""
        try:
            self.connector = SnowflakeConnector()
            logger.info("Snowflake connector initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Snowflake connector: {e}")
            return False
    
    def run_performance_test(self, 
                           formats: Optional[List[str]] = None,
                           query_range: Optional[Tuple[int, int]] = None,
                           test_mode: bool = False) -> Dict[str, Any]:
        """Run comprehensive performance test"""
        
        if not self.initialize_connector():
            logger.error("Cannot run performance test without database connection")
            return {}
        
        formats = formats or self.formats
        self.start_time = time.time()
        
        logger.info(f"Starting TPC-DS performance test for formats: {formats}")
        
        all_results = {}
        
        for format_name in formats:
            logger.info(f"Testing format: {format_name}")
            format_results = self._test_format(format_name, query_range, test_mode)
            all_results[format_name] = format_results
        
        self.end_time = time.time()
        self.results = all_results
        
        # Generate reports
        self._generate_reports()
        
        logger.info("Performance test completed successfully")
        return all_results
    
    def _test_format(self, format_name: str, query_range: Optional[Tuple[int, int]], test_mode: bool) -> List[Dict[str, Any]]:
        """Test all queries for a specific format"""
        format_dir = self.queries_dir / format_name
        if not format_dir.exists():
            logger.error(f"Format directory not found: {format_dir}")
            return []
        
        # Get query files
        query_files = sorted(format_dir.glob("q*.sql"))
        query_files = [f for f in query_files if not f.name.endswith('_test.sql')]
        
        # Apply query range filter
        if query_range:
            start_q, end_q = query_range
            query_files = [f for f in query_files 
                          if start_q <= int(f.stem[1:]) <= end_q]
        
        # Limit queries in test mode
        if test_mode:
            query_files = query_files[:5]
        
        logger.info(f"Testing {len(query_files)} queries for format {format_name}")
        
        format_results = []
        
        for query_file in query_files:
            query_num = int(query_file.stem[1:])
            logger.info(f"Testing query {query_num} for format {format_name}")
            
            result = self._execute_query_with_metrics(query_file, format_name, query_num)
            format_results.append(result)
            
            # Small delay between queries
            time.sleep(1)
        
        return format_results
    
    def _execute_query_with_metrics(self, query_file: Path, format_name: str, query_num: int) -> Dict[str, Any]:
        """Execute a single query and collect comprehensive metrics including Snowflake native metrics"""
        
        try:
            # Read query
            with open(query_file, 'r') as f:
                query = f.read()
            
            # Filter out comments and empty lines
            lines = query.split('\n')
            sql_lines = []
            for line in lines:
                line = line.strip()
                if line and not line.startswith('--'):
                    sql_lines.append(line)
            
            query = '\n'.join(sql_lines).strip()
            
            # Ensure query ends with semicolon
            query = query.strip()
            if not query.endswith(';'):
                query += ';'
            
            # Execute query multiple times for accurate metrics
            local_execution_times = []
            snowflake_execution_times = []
            row_counts = []
            errors = []
            snowflake_metrics_list = []
            
            warmup_runs = self.config['test_settings']['warmup_runs']
            test_runs = self.config['test_settings']['test_runs']
            
            for run in range(warmup_runs + test_runs):
                try:
                    # Use enhanced metrics collection for the last run only to avoid overhead
                    if run == warmup_runs + test_runs - 1:  # Last run
                        result_data = self.connector.execute_query_with_metrics(query, format_name, query_num)
                        local_execution_time = result_data['local_execution_time']
                        snowflake_metrics = result_data['snowflake_metrics']
                        result = result_data['results']
                        
                        if snowflake_metrics:
                            snowflake_execution_times.append(snowflake_metrics.get('total_elapsed_time_ms', 0) / 1000)  # Convert to seconds
                            snowflake_metrics_list.append(snowflake_metrics)
                    else:
                        # Regular execution for other runs
                        start_time = time.time()
                        result = self.connector.execute_query(query, format_name)
                        end_time = time.time()
                        local_execution_time = end_time - start_time
                        snowflake_metrics = None
                    
                    if run >= warmup_runs:  # Only count test runs
                        local_execution_times.append(local_execution_time)
                        row_counts.append(len(result) if result else 0)
                    
                    logger.debug(f"Query {query_num} run {run+1}: {local_execution_time:.2f}s")
                    
                except Exception as e:
                    error_msg = str(e)
                    errors.append(error_msg)
                    logger.error(f"Query {query_num} run {run+1} failed: {error_msg}")
            
            # Calculate local timing metrics
            if local_execution_times:
                avg_time = statistics.mean(local_execution_times)
                min_time = min(local_execution_times)
                max_time = max(local_execution_times)
                std_time = statistics.stdev(local_execution_times) if len(local_execution_times) > 1 else 0
                avg_rows = statistics.mean(row_counts)
            else:
                avg_time = min_time = max_time = std_time = avg_rows = 0
            
            # Calculate Snowflake native metrics
            snowflake_avg_time = 0
            snowflake_min_time = 0
            snowflake_max_time = 0
            snowflake_std_time = 0
            avg_cache_hit_ratio = 0
            avg_bytes_scanned_mb = 0
            total_bytes_scanned_mb = 0
            
            if snowflake_execution_times:
                snowflake_avg_time = statistics.mean(snowflake_execution_times)
                snowflake_min_time = min(snowflake_execution_times)
                snowflake_max_time = max(snowflake_execution_times)
                snowflake_std_time = statistics.stdev(snowflake_execution_times) if len(snowflake_execution_times) > 1 else 0
            
            if snowflake_metrics_list:
                cache_hit_ratios = [m.get('cache_hit_ratio', 0) for m in snowflake_metrics_list]
                bytes_scanned_list = [m.get('bytes_scanned_mb', 0) for m in snowflake_metrics_list]
                
                avg_cache_hit_ratio = statistics.mean(cache_hit_ratios) if cache_hit_ratios else 0
                avg_bytes_scanned_mb = statistics.mean(bytes_scanned_list) if bytes_scanned_list else 0
                total_bytes_scanned_mb = sum(bytes_scanned_list)
            
            # Extract warehouse size from Snowflake metrics
            warehouse_size = 'X-Small'  # Default
            if snowflake_metrics_list and snowflake_metrics_list[0]:
                warehouse_info = snowflake_metrics_list[0]
                warehouse_size = warehouse_info.get('WAREHOUSE_SIZE', warehouse_info.get('warehouse_size', 'X-Small'))
            
            # Determine status
            if errors:
                if len(errors) >= test_runs:
                    status = 'failed'
                else:
                    status = 'partial'
            else:
                status = 'success'
            
            # Get AWS cost data for external formats
            aws_cost_data = {}
            if self.aws_cost_tracker and format_name in ['iceberg_glue', 'external', 'iceberg_sf']:
                try:
                    # Estimate S3 requests based on bytes scanned
                    bytes_scanned = int(avg_bytes_scanned_mb * 1024 * 1024)
                    s3_requests = self.aws_cost_tracker.estimate_query_s3_requests(format_name, bytes_scanned)
                    
                    # Get format-specific S3 usage
                    s3_usage = self.aws_cost_tracker.get_format_s3_usage(format_name)
                    
                    aws_cost_data = {
                        's3_storage_gb': s3_usage.get('total_size_gb', 0),
                        's3_requests': s3_requests,
                        's3_storage_class': 'STANDARD',
                        'data_transferred_gb': avg_bytes_scanned_mb / 1024,  # Approximate
                        'same_region': True  # Assume same region
                    }
                    
                    if format_name == 'iceberg_glue':
                        glue_calls = self.aws_cost_tracker.get_glue_api_calls()
                        aws_cost_data['glue_api_calls'] = {
                            'get': glue_calls.get('GetTable', 0) + glue_calls.get('GetDatabase', 0),
                            'create_update': glue_calls.get('CreateTable', 0) + glue_calls.get('UpdateTable', 0)
                        }
                        aws_cost_data['glue_metadata_storage_tb'] = self.aws_cost_tracker.get_glue_metadata_storage()
                except Exception as e:
                    logger.warning(f"Failed to get AWS cost data: {e}")
            
            # Prepare result with warehouse size and execution time for cost tracking
            result = {
                'query_number': query_num,
                'query_file': query_file.name,
                'format': format_name,
                'status': status,
                'execution_times': local_execution_times,
                'execution_time': avg_time,  # For metrics collector
                'average_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'std_time': std_time,
                'average_rows': avg_rows,
                'result_rows': int(avg_rows),  # For metrics collector
                'errors': errors,
                'error_count': len(errors),
                'success_rate': (test_runs - len(errors)) / test_runs if test_runs > 0 else 0,
                'timestamp': datetime.now().isoformat(),
                'warehouse_size': warehouse_size,  # For cost calculation
                # AWS cost data (for external formats)
                **aws_cost_data,
                # Snowflake native metrics
                'snowflake_metrics': {
                    'execution_times_ms': [t * 1000 for t in snowflake_execution_times],  # Convert back to ms
                    'average_time_ms': snowflake_avg_time * 1000,
                    'min_time_ms': snowflake_min_time * 1000,
                    'max_time_ms': snowflake_max_time * 1000,
                    'std_time_ms': snowflake_std_time * 1000,
                    'avg_cache_hit_ratio': avg_cache_hit_ratio,
                    'avg_bytes_scanned_mb': avg_bytes_scanned_mb,
                    'total_bytes_scanned_mb': total_bytes_scanned_mb,
                    'warehouse_info': snowflake_metrics_list[0] if snowflake_metrics_list else None,
                    'warehouse_size': warehouse_size
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute query {query_num}: {e}")
            return {
                'query_number': query_num,
                'query_file': query_file.name,
                'format': format_name,
                'status': 'error',
                'execution_times': [],
                'average_time': 0,
                'min_time': 0,
                'max_time': 0,
                'std_time': 0,
                'average_rows': 0,
                'errors': [str(e)],
                'error_count': 1,
                'success_rate': 0,
                'timestamp': datetime.now().isoformat(),
                'snowflake_metrics': None
            }
    
    def _generate_reports(self):
        """Generate comprehensive performance reports"""
        logger.info("Generating performance reports...")
        
        # Generate JSON report
        self._generate_json_report()
        
        # Generate CSV report
        self._generate_csv_report()
        
        # Generate HTML report
        self._generate_html_report()
        
        # Generate analytics
        self._generate_analytics()
        
        logger.info("Reports generated successfully")
    
    def _generate_json_report(self):
        """Generate detailed JSON report"""
        report_data = {
            'test_summary': {
                'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
                'end_time': datetime.fromtimestamp(self.end_time).isoformat(),
                'total_duration': self.end_time - self.start_time,
                'formats_tested': list(self.results.keys()),
                'total_queries': sum(len(format_results) for format_results in self.results.values())
            },
            'results': self.results,
            'config': self.config
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.results_dir / f"tpcds_performance_report_{timestamp}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        logger.info(f"JSON report saved: {report_file}")
    
    def _generate_csv_report(self):
        """Generate CSV report for analysis"""
        rows = []
        
        for format_name, format_results in self.results.items():
            for result in format_results:
                rows.append({
                    'format': format_name,
                    'query_number': result['query_number'],
                    'query_file': result['query_file'],
                    'status': result['status'],
                    'average_time': result['average_time'],
                    'min_time': result['min_time'],
                    'max_time': result['max_time'],
                    'std_time': result['std_time'],
                    'average_rows': result['average_rows'],
                    'error_count': result['error_count'],
                    'success_rate': result['success_rate'],
                    'timestamp': result['timestamp']
                })
        
        df = pd.DataFrame(rows)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_file = self.results_dir / f"tpcds_performance_data_{timestamp}.csv"
        
        df.to_csv(csv_file, index=False)
        logger.info(f"CSV report saved: {csv_file}")
    
    def _generate_html_report(self):
        """Generate comprehensive HTML report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        html_file = self.reports_dir / f"tpcds_performance_report_{timestamp}.html"
        
        html_content = self._create_html_report()
        
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report saved: {html_file}")
    
    def _create_html_report(self) -> str:
        """Create HTML report content with comprehensive analytics"""
        total_duration = self.end_time - self.start_time if self.end_time and self.start_time else 0
        
        # Generate analytics
        analytics = self._analyze_performance()
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>TPC-DS Performance Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; line-height: 1.6; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; margin-bottom: 20px; }}
        .summary {{ margin: 20px 0; padding: 15px; background-color: #e8f4fd; border-radius: 5px; }}
        .analytics {{ margin: 20px 0; padding: 15px; background-color: #f8f9fa; border-radius: 5px; }}
        .format-section {{ margin: 20px 0; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
        .query-result {{ margin: 10px 0; padding: 10px; background-color: #f9f9f9; border-radius: 3px; }}
        .success {{ border-left: 5px solid #4CAF50; }}
        .failed {{ border-left: 5px solid #f44336; }}
        .partial {{ border-left: 5px solid #ff9800; }}
        .error {{ border-left: 5px solid #f44336; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .metric-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 15px 0; }}
        .metric-card {{ background-color: white; padding: 15px; border-radius: 5px; border: 1px solid #ddd; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
        .metric-label {{ font-size: 14px; color: #7f8c8d; margin-top: 5px; }}
        .comparison-table {{ margin: 20px 0; }}
        .recommendations {{ background-color: #fff3cd; padding: 15px; border-radius: 5px; border-left: 4px solid #ffc107; }}
        .snowflake-metrics {{ background-color: #d1ecf1; padding: 10px; border-radius: 3px; margin: 5px 0; }}
        h1, h2, h3 {{ color: #2c3e50; }}
        .highlight {{ background-color: #fff3cd; padding: 2px 4px; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>TPC-DS Performance Test Report</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Total Duration:</strong> {total_duration:.2f} seconds</p>
        <p><strong>Test Mode:</strong> {'Test Mode (5 queries)' if len(self.results.get('native', [])) <= 5 else 'Full Test'}</p>
    </div>
    
    <div class="summary">
        <h2>📊 Executive Summary</h2>
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value">{analytics['summary']['total_queries']}</div>
                <div class="metric-label">Total Queries</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{analytics['summary']['total_successful']}</div>
                <div class="metric-label">Successful Queries</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{analytics['summary']['overall_success_rate']:.1%}</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{analytics['summary']['overall_average_time']:.2f}s</div>
                <div class="metric-label">Avg Execution Time</div>
            </div>
        </div>
        
        <h3>Formats Tested</h3>
        <ul>
            <li><strong>Formats:</strong> {', '.join(self.results.keys())}</li>
            <li><strong>Total Queries:</strong> {sum(len(format_results) for format_results in self.results.values())}</li>
            <li><strong>Test Duration:</strong> {total_duration:.2f} seconds</li>
        </ul>
    </div>
    
    <div class="analytics">
        <h2>📈 Performance Analytics</h2>
        
        <h3>Format Comparison</h3>
        <div class="comparison-table">
            <table>
                <tr>
                    <th>Format</th>
                    <th>Queries</th>
                    <th>Success Rate</th>
                    <th>Local Avg (s)</th>
                    <th>Local Min (s)</th>
                    <th>Local Max (s)</th>
                    <th>Local Std Dev</th>
                    <th>Snowflake Avg (ms)</th>
                    <th>Snowflake Min (ms)</th>
                    <th>Snowflake Max (ms)</th>
                    <th>Snowflake Std Dev</th>
                    <th>Bytes Scanned (MB)</th>
                    <th>Cache Hit Ratio</th>
                </tr>
"""
        
        # Add format comparison data
        for format_name, format_data in analytics['format_comparison'].items():
            snowflake_metrics = format_data.get('snowflake_metrics', {})
            html += f"""
                <tr>
                    <td><strong>{format_name}</strong></td>
                    <td>{format_data['total_queries']}</td>
                    <td>{format_data['success_rate']:.1%}</td>
                    <td>{format_data['average_execution_time']:.2f}</td>
                    <td>{format_data['min_execution_time']:.2f}</td>
                    <td>{format_data['max_execution_time']:.2f}</td>
                    <td>{format_data.get('std_execution_time', 0):.2f}</td>
                    <td class="snowflake-metrics">{snowflake_metrics.get('average_execution_time_ms', 0):.1f}</td>
                    <td class="snowflake-metrics">{snowflake_metrics.get('min_execution_time_ms', 0):.1f}</td>
                    <td class="snowflake-metrics">{snowflake_metrics.get('max_execution_time_ms', 0):.1f}</td>
                    <td class="snowflake-metrics">{snowflake_metrics.get('std_execution_time_ms', 0):.1f}</td>
                    <td>{snowflake_metrics.get('avg_bytes_scanned_mb', 0):.2f}</td>
                    <td>{snowflake_metrics.get('avg_cache_hit_ratio', 0):.1%}</td>
                </tr>
"""
        
        html += """
            </table>
        </div>
        
        <h3>Table Row Count Statistics</h3>
        <div class="comparison-table">
            <table>
                <tr>
                    <th>Format</th>
                    <th>Total Rows</th>
                    <th>Avg Rows/Table</th>
                    <th>Min Rows</th>
                    <th>Max Rows</th>
                    <th>Tables with Data</th>
                    <th>Total Tables</th>
                </tr>
"""
        
        # Add row count statistics
        for format_name, format_data in analytics['format_comparison'].items():
            row_stats = format_data.get('row_count_stats', {})
            html += f"""
                <tr>
                    <td><strong>{format_name}</strong></td>
                    <td>{row_stats.get('total_rows', 0):.0f}</td>
                    <td>{row_stats.get('avg_rows_per_table', 0):.1f}</td>
                    <td>{row_stats.get('min_rows', 0):.0f}</td>
                    <td>{row_stats.get('max_rows', 0):.0f}</td>
                    <td>{row_stats.get('tables_with_data', 0)}</td>
                    <td>{row_stats.get('total_tables', 0)}</td>
                </tr>
"""
        
        html += """
            </table>
        </div>
        
        <h3>Detailed Table Row Counts by Schema</h3>
        <div class="comparison-table">
            <table>
                <tr>
                    <th>Table Name</th>
"""
        
        # Add column headers for each format
        for format_name in analytics['format_comparison'].keys():
            html += f"                    <th>{format_name}</th>\n"
        
        html += "                </tr>\n"
        
        # Get all table names from the first format
        first_format = list(analytics['table_row_counts'].keys())[0] if analytics['table_row_counts'] else None
        if first_format and analytics['table_row_counts'].get(first_format):
            table_names = list(analytics['table_row_counts'][first_format].keys())
            
            for table_name in sorted(table_names):
                html += f"                <tr>\n                    <td><strong>{table_name}</strong></td>\n"
                
                for format_name in analytics['format_comparison'].keys():
                    table_counts = analytics['table_row_counts'].get(format_name, {})
                    row_count = table_counts.get(table_name, 0)
                    html += f"                    <td>{row_count:,}</td>\n"
                
                html += "                </tr>\n"
        
        html += """
            </table>
        </div>
        
        <h3>Snowflake Native Metrics Summary</h3>
        <div class="metric-grid">
"""
        
        # Add Snowflake metrics summary
        if analytics['summary'].get('snowflake_metrics'):
            sf_metrics = analytics['summary']['snowflake_metrics']
            html += f"""
            <div class="metric-card">
                <div class="metric-value">{sf_metrics.get('overall_average_time_ms', 0):.1f}ms</div>
                <div class="metric-label">Avg Snowflake Time</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{sf_metrics.get('queries_with_snowflake_metrics', 0)}</div>
                <div class="metric-label">Queries with Metrics</div>
            </div>
"""
        
        html += """
        </div>
    </div>
"""
        
        # Add format-specific results
        for format_name, format_results in self.results.items():
            format_data = analytics['format_comparison'].get(format_name, {})
            snowflake_metrics = format_data.get('snowflake_metrics', {})
            row_stats = format_data.get('row_count_stats', {})
            
            html += f"""
    <div class="format-section">
        <h2>📊 Format: {format_name}</h2>
        <div class="metric-grid">
            <div class="metric-card">
                <div class="metric-value">{len(format_results)}</div>
                <div class="metric-label">Queries Tested</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{format_data.get('success_rate', 0):.1%}</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{format_data.get('average_execution_time', 0):.2f}s</div>
                <div class="metric-label">Avg Local Time</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{snowflake_metrics.get('average_execution_time_ms', 0):.1f}ms</div>
                <div class="metric-label">Avg Snowflake Time</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{row_stats.get('total_rows', 0):.0f}</div>
                <div class="metric-label">Total Table Rows</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{row_stats.get('tables_with_data', 0)}/{row_stats.get('total_tables', 0)}</div>
                <div class="metric-label">Tables with Data</div>
            </div>
        </div>
        
        <table>
            <tr>
                <th>Query</th>
                <th>Status</th>
                <th>Local Avg (s)</th>
                <th>Snowflake Avg (ms)</th>
                <th>Min Time (s)</th>
                <th>Max Time (s)</th>
                <th>Std Dev</th>
                <th>Rows Produced</th>
                <th>Bytes Scanned (MB)</th>
                <th>Success Rate</th>
            </tr>
"""
            
            for result in format_results:
                status_class = result['status']
                sf_metrics = result.get('snowflake_metrics', {})
                html += f"""
            <tr>
                <td>{result['query_file']}</td>
                <td class="{status_class}">{result['status']}</td>
                <td>{result['average_time']:.2f}</td>
                <td class="snowflake-metrics">{sf_metrics.get('average_time_ms', 0):.1f}</td>
                <td>{result['min_time']:.2f}</td>
                <td>{result['max_time']:.2f}</td>
                <td>{result['std_time']:.2f}</td>
                <td>{result['average_rows']:.0f}</td>
                <td>{sf_metrics.get('avg_bytes_scanned_mb', 0):.2f}</td>
                <td>{result['success_rate']:.2%}</td>
            </tr>
"""
            
            html += """
        </table>
    </div>
"""
        
        # Add recommendations section
        if analytics.get('recommendations'):
            html += """
    <div class="recommendations">
        <h2>💡 Recommendations</h2>
        <ul>
"""
            for recommendation in analytics['recommendations']:
                html += f"            <li>{recommendation}</li>\n"
            
            html += """
        </ul>
    </div>
"""
        
        # Add footer with additional insights
        html += f"""
    <div class="analytics">
        <h2>🔍 Additional Insights</h2>
        <ul>
            <li><strong>Performance Variance:</strong> Standard deviation across queries shows consistency in execution times</li>
            <li><strong>Timing Methodology:</strong>
                <ul>
                    <li><strong>Local Timing:</strong> End-to-end time including network latency, connection overhead, data transfer, and Python processing</li>
                    <li><strong>Snowflake Timing:</strong> Pure query execution time on Snowflake server (from QUERY_HISTORY)</li>
                    <li><strong>Why Local < Snowflake:</strong> This is unusual - typically local timing is higher due to network overhead. May indicate query result caching or very fast network.</li>
                </ul>
            </li>
            <li><strong>Table Row Counts:</strong> Shows actual data volume across all TPC-DS tables in each schema format</li>
            <li><strong>Cache Performance:</strong> Cache hit ratios indicate query result caching effectiveness</li>
            <li><strong>Data Scanning:</strong> Bytes scanned metrics help identify resource-intensive queries</li>
        </ul>
        
        <h3>Test Configuration</h3>
        <ul>
            <li><strong>Warmup Runs:</strong> {self.config['test_settings']['warmup_runs']}</li>
            <li><strong>Test Runs:</strong> {self.config['test_settings']['test_runs']}</li>
            <li><strong>Max Execution Time:</strong> {self.config['performance_thresholds']['max_execution_time']}s</li>
            <li><strong>Success Rate Threshold:</strong> {self.config['performance_thresholds']['success_rate_threshold']:.1%}</li>
        </ul>
    </div>
    
    <div style="margin-top: 40px; padding: 20px; background-color: #f8f9fa; border-radius: 5px; text-align: center; color: #6c757d;">
        <p><strong>Report Generated by TPC-DS Performance Test Framework</strong></p>
        <p>For detailed data, see the accompanying JSON and CSV files in the results directory.</p>
    </div>
</body>
</html>
"""
        
        return html
    
    def _generate_analytics(self):
        """Generate performance analytics and insights"""
        logger.info("Generating performance analytics...")
        
        # Create analytics data
        analytics_data = self._analyze_performance()
        
        # Save analytics
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analytics_file = self.results_dir / f"tpcds_analytics_{timestamp}.json"
        
        with open(analytics_file, 'w') as f:
            json.dump(analytics_data, f, indent=2)
        
        logger.info(f"Analytics saved: {analytics_file}")
    
    def _analyze_performance(self) -> Dict[str, Any]:
        """Analyze performance data and generate insights"""
        analytics = {
            'summary': {},
            'format_comparison': {},
            'query_performance': {},
            'recommendations': [],
            'table_row_counts': {}
        }
        
        # Get actual table row counts for all schemas
        logger.info("Collecting table row counts for all schemas...")
        table_row_counts = self.connector.get_table_row_counts()
        analytics['table_row_counts'] = table_row_counts
        
        # Format comparison
        for format_name, format_results in self.results.items():
            successful_results = [r for r in format_results if r['status'] == 'success']
            
            if successful_results:
                avg_times = [r['average_time'] for r in successful_results]
                
                # Snowflake metrics
                snowflake_metrics = [r.get('snowflake_metrics') for r in successful_results if r.get('snowflake_metrics')]
                snowflake_avg_times = [m['average_time_ms'] for m in snowflake_metrics if m and 'average_time_ms' in m]
                cache_hit_ratios = [m['avg_cache_hit_ratio'] for m in snowflake_metrics if m and 'avg_cache_hit_ratio' in m]
                bytes_scanned = [m['avg_bytes_scanned_mb'] for m in snowflake_metrics if m and 'avg_bytes_scanned_mb' in m]
                
                # Calculate row count statistics from actual table data
                schema_table_counts = table_row_counts.get(format_name, {})
                table_row_values = list(schema_table_counts.values())
                
                format_data = {
                    'total_queries': len(format_results),
                    'successful_queries': len(successful_results),
                    'success_rate': len(successful_results) / len(format_results),
                    'average_execution_time': statistics.mean(avg_times),
                    'median_execution_time': statistics.median(avg_times),
                    'min_execution_time': min(avg_times),
                    'max_execution_time': max(avg_times),
                    'std_execution_time': statistics.stdev(avg_times) if len(avg_times) > 1 else 0,
                    'row_count_stats': {
                        'total_rows': sum(table_row_values),
                        'avg_rows_per_table': statistics.mean(table_row_values) if table_row_values else 0,
                        'min_rows': min(table_row_values) if table_row_values else 0,
                        'max_rows': max(table_row_values) if table_row_values else 0,
                        'std_rows': statistics.stdev(table_row_values) if len(table_row_values) > 1 else 0,
                        'tables_with_data': sum(1 for count in table_row_values if count > 0),
                        'total_tables': len(table_row_values)
                    }
                }
                
                # Add Snowflake native metrics
                if snowflake_avg_times:
                    format_data['snowflake_metrics'] = {
                        'average_execution_time_ms': statistics.mean(snowflake_avg_times),
                        'median_execution_time_ms': statistics.median(snowflake_avg_times),
                        'min_execution_time_ms': min(snowflake_avg_times),
                        'max_execution_time_ms': max(snowflake_avg_times),
                        'std_execution_time_ms': statistics.stdev(snowflake_avg_times) if len(snowflake_avg_times) > 1 else 0,
                        'avg_cache_hit_ratio': statistics.mean(cache_hit_ratios) if cache_hit_ratios else 0,
                        'avg_bytes_scanned_mb': statistics.mean(bytes_scanned) if bytes_scanned else 0,
                        'total_bytes_scanned_mb': sum(bytes_scanned) if bytes_scanned else 0
                    }
                
                analytics['format_comparison'][format_name] = format_data
        
        # Overall summary
        all_times = []
        all_snowflake_times = []
        total_queries = 0
        total_successful = 0
        
        for format_results in self.results.values():
            total_queries += len(format_results)
            successful_results = [r for r in format_results if r['status'] == 'success']
            total_successful += len(successful_results)
            all_times.extend([r['average_time'] for r in successful_results])
            
            # Collect Snowflake metrics
            snowflake_metrics = [r.get('snowflake_metrics') for r in successful_results if r.get('snowflake_metrics')]
            snowflake_times = [m['average_time_ms'] for m in snowflake_metrics if m and 'average_time_ms' in m]
            all_snowflake_times.extend(snowflake_times)
        
        analytics['summary'] = {
            'total_queries': total_queries,
            'total_successful': total_successful,
            'overall_success_rate': total_successful / total_queries if total_queries > 0 else 0,
            'overall_average_time': statistics.mean(all_times) if all_times else 0,
            'overall_median_time': statistics.median(all_times) if all_times else 0,
            'snowflake_metrics': {
                'overall_average_time_ms': statistics.mean(all_snowflake_times) if all_snowflake_times else 0,
                'overall_median_time_ms': statistics.median(all_snowflake_times) if all_snowflake_times else 0,
                'queries_with_snowflake_metrics': len(all_snowflake_times)
            }
        }
        
        # Generate recommendations
        analytics['recommendations'] = self._generate_recommendations(analytics)
        
        return analytics
    
    def _generate_recommendations(self, analytics: Dict[str, Any]) -> List[str]:
        """Generate performance recommendations"""
        recommendations = []
        
        # Check success rates
        for format_name, format_data in analytics['format_comparison'].items():
            if format_data['success_rate'] < 0.95:
                recommendations.append(f"Format {format_name} has low success rate ({format_data['success_rate']:.2%}). Investigate failed queries.")
        
        # Check performance differences
        if len(analytics['format_comparison']) > 1:
            times = [(name, data['average_execution_time']) for name, data in analytics['format_comparison'].items()]
            times.sort(key=lambda x: x[1])
            
            fastest = times[0]
            slowest = times[-1]
            
            if slowest[1] > fastest[1] * 2:
                recommendations.append(f"Significant performance difference detected: {slowest[0]} is {slowest[1]/fastest[1]:.1f}x slower than {fastest[0]}")
        
        return recommendations

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='TPC-DS Performance Testing Application')
    parser.add_argument('--formats', nargs='+', choices=['native', 'iceberg_sf', 'iceberg_glue', 'external'],
                       help='Formats to test (default: all)')
    parser.add_argument('--query-range', nargs=2, type=int, metavar=('START', 'END'),
                       help='Query range to test (e.g., --query-range 1 10)')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode (limited queries)')
    parser.add_argument('--config', default='benchmark/config/perf_test_config.yaml',
                       help='Configuration file path')
    
    args = parser.parse_args()
    
    # Initialize tester
    tester = TPCDSPerformanceTester(args.config)
    
    # Run performance test
    results = tester.run_performance_test(
        formats=args.formats,
        query_range=tuple(args.query_range) if args.query_range else None,
        test_mode=args.test_mode
    )
    
    # Print summary
    print("\n" + "="*60)
    print("TPC-DS PERFORMANCE TEST SUMMARY")
    print("="*60)
    
    total_queries = sum(len(format_results) for format_results in results.values())
    total_successful = sum(
        sum(1 for result in format_results if result['status'] == 'success')
        for format_results in results.values()
    )
    
    print(f"Total queries tested: {total_queries}")
    print(f"Successful executions: {total_successful}")
    print(f"Success rate: {total_successful/total_queries:.2%}")
    
    for format_name, format_results in results.items():
        successful = sum(1 for r in format_results if r['status'] == 'success')
        avg_time = statistics.mean([r['average_time'] for r in format_results if r['status'] == 'success'])
        
        # Get Snowflake metrics
        snowflake_metrics = [r.get('snowflake_metrics') for r in format_results if r['status'] == 'success' and r.get('snowflake_metrics')]
        if snowflake_metrics:
            snowflake_avg_time = statistics.mean([m['average_time_ms'] for m in snowflake_metrics if 'average_time_ms' in m])
            print(f"{format_name}: {successful}/{len(format_results)} queries, local avg: {avg_time:.2f}s, snowflake avg: {snowflake_avg_time:.1f}ms")
        else:
            print(f"{format_name}: {successful}/{len(format_results)} queries, avg time: {avg_time:.2f}s")

if __name__ == "__main__":
    main()
