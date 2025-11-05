#!/usr/bin/env python3
"""
Performance Metrics Collector for TPC-DS Performance Testing
Collects, stores, and analyzes performance metrics
"""

import time
import json
import logging
import threading
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta
import statistics
import psutil
import os
from collections import defaultdict, deque
import sqlite3

# Import cost calculator
try:
    from .cost_calculator import CostCalculator
except ImportError:
    # Fallback for direct execution
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from cost_calculator import CostCalculator

logger = logging.getLogger(__name__)

class PerformanceMetricsCollector:
    """Collects and manages performance metrics"""
    
    def __init__(self, config: Dict[str, Any], results_dir: Path):
        """Initialize metrics collector"""
        self.config = config
        self.results_dir = results_dir
        self.metrics_db_path = results_dir / "performance_metrics.db"
        self.metrics_cache = defaultdict(list)
        self.system_metrics = deque(maxlen=1000)
        self.collection_active = False
        self.collection_thread = None
        
        # Initialize cost calculator
        self.cost_calculator = CostCalculator(config)
        
        # Initialize database
        self._init_database()
        
        # Start system metrics collection
        self.start_system_monitoring()
    
    def _init_database(self):
        """Initialize SQLite database for metrics storage"""
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Create metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    query_number INTEGER NOT NULL,
                    format TEXT NOT NULL,
                    execution_time REAL NOT NULL,
                    result_rows INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    system_cpu_percent REAL,
                    system_memory_mb REAL,
                    query_memory_mb REAL,
                    session_id TEXT,
                    test_run_id TEXT
                )
            ''')
            
            # Create system metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    cpu_percent REAL NOT NULL,
                    memory_percent REAL NOT NULL,
                    memory_used_mb REAL NOT NULL,
                    memory_available_mb REAL NOT NULL,
                    disk_io_read_mb REAL,
                    disk_io_write_mb REAL,
                    network_sent_mb REAL,
                    network_recv_mb REAL
                )
            ''')
            
            # Create cost metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cost_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id INTEGER NOT NULL,
                    format TEXT NOT NULL,
                    compute_credits REAL,
                    compute_cost_usd REAL,
                    storage_cost_usd REAL,
                    s3_storage_cost_usd REAL,
                    s3_request_cost_usd REAL,
                    glue_cost_usd REAL,
                    data_transfer_cost_usd REAL,
                    total_cost_usd REAL,
                    warehouse_size TEXT,
                    execution_time_seconds REAL,
                    timestamp TEXT NOT NULL,
                    FOREIGN KEY (query_id) REFERENCES query_metrics(id)
                )
            ''')
            
            # Create warehouse metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS warehouse_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    warehouse_name TEXT NOT NULL,
                    warehouse_size TEXT NOT NULL,
                    execution_time_seconds REAL,
                    credits_consumed REAL,
                    timestamp TEXT NOT NULL
                )
            ''')
            
            # Create storage metrics table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS storage_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    format TEXT NOT NULL,
                    storage_bytes INTEGER,
                    storage_cost_usd REAL,
                    timestamp TEXT NOT NULL
                )
            ''')
            
            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_query_metrics_timestamp ON query_metrics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_query_metrics_format ON query_metrics(format)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_query_metrics_query_number ON query_metrics(query_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp ON system_metrics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cost_metrics_query_id ON cost_metrics(query_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cost_metrics_format ON cost_metrics(format)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_warehouse_metrics_timestamp ON warehouse_metrics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_storage_metrics_format ON storage_metrics(format)')
            
            # Add warehouse_size and bytes_scanned columns to query_metrics if they don't exist
            try:
                cursor.execute('ALTER TABLE query_metrics ADD COLUMN warehouse_size TEXT')
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            try:
                cursor.execute('ALTER TABLE query_metrics ADD COLUMN bytes_scanned INTEGER')
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            try:
                cursor.execute('ALTER TABLE query_metrics ADD COLUMN compute_credits REAL')
            except sqlite3.OperationalError:
                pass  # Column already exists
            
            conn.commit()
            conn.close()
            
            logger.info(f"Metrics database initialized: {self.metrics_db_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize metrics database: {e}")
    
    def collect_query_metrics(self, query_result: Dict[str, Any], 
                           session_id: str = None, test_run_id: str = None) -> None:
        """Collect metrics from a query execution result"""
        
        try:
            # Get current system metrics
            system_metrics = self._get_current_system_metrics()
            
            # Extract warehouse size and execution time for cost calculation
            warehouse_size = query_result.get('warehouse_size', 'X-Small')
            execution_time = query_result.get('execution_time', 0)
            if execution_time <= 0:
                execution_time = query_result.get('average_time', 0)
            
            # Get bytes scanned from Snowflake metrics if available
            snowflake_metrics = query_result.get('snowflake_metrics', {})
            bytes_scanned = snowflake_metrics.get('avg_bytes_scanned_mb', 0) * 1024 * 1024 if snowflake_metrics else 0
            
            # Calculate compute credits
            compute_credits = self.cost_calculator.calculate_snowflake_credits(warehouse_size, execution_time)
            
            # Prepare metrics data
            metrics_data = {
                'timestamp': query_result.get('timestamp', datetime.now().isoformat()),
                'query_number': query_result.get('query_number', 0),
                'format': query_result.get('format', 'unknown'),
                'execution_time': execution_time,
                'result_rows': query_result.get('result_rows', 0),
                'status': query_result.get('status', 'unknown'),
                'error_message': query_result.get('error', ''),
                'system_cpu_percent': system_metrics['cpu_percent'],
                'system_memory_mb': system_metrics['memory_mb'],
                'query_memory_mb': query_result.get('memory_usage_mb', 0),
                'warehouse_size': warehouse_size,
                'bytes_scanned': int(bytes_scanned),
                'compute_credits': compute_credits,
                'session_id': session_id,
                'test_run_id': test_run_id
            }
            
            # Store in database
            query_id = self._store_metrics(metrics_data)
            
            # Calculate and store cost metrics
            if query_id and execution_time > 0:
                self._store_cost_metrics(query_id, query_result, metrics_data)
            
            # Store warehouse metrics
            if execution_time > 0:
                self._store_warehouse_metrics(warehouse_size, execution_time, compute_credits)
            
            # Store in cache
            self.metrics_cache[f"{metrics_data['format']}_{metrics_data['query_number']}"].append(metrics_data)
            
            logger.debug(f"Collected metrics for query {metrics_data['query_number']} ({metrics_data['format']})")
            
        except Exception as e:
            logger.error(f"Failed to collect query metrics: {e}")
    
    def collect_batch_metrics(self, query_results: List[Dict[str, Any]], 
                            session_id: str = None, test_run_id: str = None) -> None:
        """Collect metrics from multiple query results"""
        
        for result in query_results:
            self.collect_query_metrics(result, session_id, test_run_id)
    
    def start_system_monitoring(self):
        """Start background system metrics collection"""
        if self.collection_active:
            return
        
        self.collection_active = True
        self.collection_thread = threading.Thread(target=self._collect_system_metrics)
        self.collection_thread.daemon = True
        self.collection_thread.start()
        
        logger.info("System metrics collection started")
    
    def stop_system_monitoring(self):
        """Stop background system metrics collection"""
        self.collection_active = False
        if self.collection_thread:
            self.collection_thread.join(timeout=5)
        
        logger.info("System metrics collection stopped")
    
    def _collect_system_metrics(self):
        """Background thread for collecting system metrics"""
        while self.collection_active:
            try:
                metrics = self._get_current_system_metrics()
                metrics['timestamp'] = datetime.now().isoformat()
                
                # Store in memory cache
                self.system_metrics.append(metrics)
                
                # Store in database every 10 collections
                if len(self.system_metrics) % 10 == 0:
                    self._store_system_metrics_batch()
                
                time.sleep(5)  # Collect every 5 seconds
                
            except Exception as e:
                logger.error(f"Error collecting system metrics: {e}")
                time.sleep(10)  # Wait longer on error
    
    def _get_current_system_metrics(self) -> Dict[str, float]:
        """Get current system metrics"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_mb = memory.used / 1024 / 1024
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            disk_read_mb = disk_io.read_bytes / 1024 / 1024 if disk_io else 0
            disk_write_mb = disk_io.write_bytes / 1024 / 1024 if disk_io else 0
            
            # Network I/O
            network_io = psutil.net_io_counters()
            network_sent_mb = network_io.bytes_sent / 1024 / 1024 if network_io else 0
            network_recv_mb = network_io.bytes_recv / 1024 / 1024 if network_io else 0
            
            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_mb': memory_mb,
                'memory_available_mb': memory.available / 1024 / 1024,
                'disk_io_read_mb': disk_read_mb,
                'disk_io_write_mb': disk_write_mb,
                'network_sent_mb': network_sent_mb,
                'network_recv_mb': network_recv_mb
            }
            
        except Exception as e:
            logger.error(f"Error getting system metrics: {e}")
            return {
                'cpu_percent': 0,
                'memory_percent': 0,
                'memory_mb': 0,
                'memory_available_mb': 0,
                'disk_io_read_mb': 0,
                'disk_io_write_mb': 0,
                'network_sent_mb': 0,
                'network_recv_mb': 0
            }
    
    def _store_metrics(self, metrics_data: Dict[str, Any]) -> Optional[int]:
        """Store metrics in database and return query_id"""
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Check if warehouse_size, bytes_scanned, compute_credits columns exist
            cursor.execute("PRAGMA table_info(query_metrics)")
            columns = [col[1] for col in cursor.fetchall()]
            has_warehouse_size = 'warehouse_size' in columns
            has_bytes_scanned = 'bytes_scanned' in columns
            has_compute_credits = 'compute_credits' in columns
            
            # Build INSERT statement dynamically based on available columns
            base_cols = ['timestamp', 'query_number', 'format', 'execution_time', 'result_rows', 
                        'status', 'error_message', 'system_cpu_percent', 'system_memory_mb', 
                        'query_memory_mb', 'session_id', 'test_run_id']
            placeholders = ['?' for _ in base_cols]
            
            values = [
                metrics_data['timestamp'],
                metrics_data['query_number'],
                metrics_data['format'],
                metrics_data['execution_time'],
                metrics_data['result_rows'],
                metrics_data['status'],
                metrics_data['error_message'],
                metrics_data['system_cpu_percent'],
                metrics_data['system_memory_mb'],
                metrics_data['query_memory_mb'],
                metrics_data.get('session_id'),
                metrics_data.get('test_run_id')
            ]
            
            if has_warehouse_size:
                base_cols.append('warehouse_size')
                placeholders.append('?')
                values.append(metrics_data.get('warehouse_size'))
            
            if has_bytes_scanned:
                base_cols.append('bytes_scanned')
                placeholders.append('?')
                values.append(metrics_data.get('bytes_scanned', 0))
            
            if has_compute_credits:
                base_cols.append('compute_credits')
                placeholders.append('?')
                values.append(metrics_data.get('compute_credits', 0))
            
            query = f'''
                INSERT INTO query_metrics 
                ({', '.join(base_cols)})
                VALUES ({', '.join(placeholders)})
            '''
            
            cursor.execute(query, values)
            query_id = cursor.lastrowid
            
            conn.commit()
            conn.close()
            
            return query_id
            
        except Exception as e:
            logger.error(f"Failed to store metrics: {e}")
            return None
    
    def _store_cost_metrics(self, query_id: int, query_result: Dict[str, Any], metrics_data: Dict[str, Any]):
        """Store cost metrics in database"""
        try:
            # Prepare query metrics for cost calculation
            format_name = metrics_data['format']
            cost_query_metrics = {
                'warehouse_size': metrics_data.get('warehouse_size', 'X-Small'),
                'execution_time_seconds': metrics_data.get('execution_time', 0),
                'storage_bytes': query_result.get('storage_bytes', 0),
                's3_storage_gb': query_result.get('s3_storage_gb', 0),
                's3_storage_class': query_result.get('s3_storage_class', 'STANDARD'),
                's3_requests': query_result.get('s3_requests', {}),
                'glue_api_calls': query_result.get('glue_api_calls', {}),
                'glue_metadata_storage_tb': query_result.get('glue_metadata_storage_tb', 0.0),
                'data_transferred_gb': query_result.get('data_transferred_gb', 0),
                'same_region': query_result.get('same_region', True)
            }
            
            # Calculate costs
            cost_breakdown = self.cost_calculator.calculate_total_query_cost(cost_query_metrics, format_name)
            
            # Store in database
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO cost_metrics 
                (query_id, format, compute_credits, compute_cost_usd, storage_cost_usd,
                 s3_storage_cost_usd, s3_request_cost_usd, glue_cost_usd, 
                 data_transfer_cost_usd, total_cost_usd, warehouse_size,
                 execution_time_seconds, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                query_id,
                format_name,
                cost_breakdown['compute_credits'],
                cost_breakdown['compute_cost_usd'],
                cost_breakdown['storage_cost_usd'],
                cost_breakdown['s3_storage_cost_usd'],
                cost_breakdown['s3_request_cost_usd'],
                cost_breakdown['glue_cost_usd'],
                cost_breakdown['data_transfer_cost_usd'],
                cost_breakdown['total_cost_usd'],
                cost_breakdown['warehouse_size'],
                cost_breakdown['execution_time_seconds'],
                metrics_data['timestamp']
            ))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"Stored cost metrics for query_id {query_id}: ${cost_breakdown['total_cost_usd']:.6f}")
            
        except Exception as e:
            logger.error(f"Failed to store cost metrics: {e}")
    
    def _store_warehouse_metrics(self, warehouse_size: str, execution_time: float, credits: float):
        """Store warehouse metrics"""
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO warehouse_metrics 
                (warehouse_name, warehouse_size, execution_time_seconds, credits_consumed, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                'default',  # Warehouse name - could be extracted from query_result if available
                warehouse_size,
                execution_time,
                credits,
                datetime.now().isoformat()
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to store warehouse metrics: {e}")
    
    def _store_system_metrics_batch(self):
        """Store batch of system metrics in database"""
        if not self.system_metrics:
            return
        
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Get metrics to store (last 10)
            metrics_to_store = list(self.system_metrics)[-10:]
            
            for metrics in metrics_to_store:
                cursor.execute('''
                    INSERT INTO system_metrics 
                    (timestamp, cpu_percent, memory_percent, memory_used_mb, 
                     memory_available_mb, disk_io_read_mb, disk_io_write_mb, 
                     network_sent_mb, network_recv_mb)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    metrics['timestamp'],
                    metrics['cpu_percent'],
                    metrics['memory_percent'],
                    metrics['memory_mb'],
                    metrics['memory_available_mb'],
                    metrics['disk_io_read_mb'],
                    metrics['disk_io_write_mb'],
                    metrics['network_sent_mb'],
                    metrics['network_recv_mb']
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to store system metrics batch: {e}")
    
    def get_metrics_summary(self, format_name: str = None, 
                          query_number: int = None,
                          time_range: Tuple[datetime, datetime] = None) -> Dict[str, Any]:
        """Get metrics summary for analysis"""
        
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Build query
            query = "SELECT * FROM query_metrics WHERE 1=1"
            params = []
            
            if format_name:
                query += " AND format = ?"
                params.append(format_name)
            
            if query_number:
                query += " AND query_number = ?"
                params.append(query_number)
            
            if time_range:
                start_time, end_time = time_range
                query += " AND timestamp BETWEEN ? AND ?"
                params.extend([start_time.isoformat(), end_time.isoformat()])
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            # Convert to list of dictionaries
            metrics_data = [dict(zip(columns, row)) for row in rows]
            
            conn.close()
            
            # Calculate summary statistics
            if metrics_data:
                execution_times = [m['execution_time'] for m in metrics_data if m['execution_time'] > 0]
                result_rows = [m['result_rows'] for m in metrics_data]
                
                summary = {
                    'total_queries': len(metrics_data),
                    'successful_queries': len([m for m in metrics_data if m['status'] == 'success']),
                    'failed_queries': len([m for m in metrics_data if m['status'] == 'error']),
                    'success_rate': len([m for m in metrics_data if m['status'] == 'success']) / len(metrics_data),
                    'average_execution_time': statistics.mean(execution_times) if execution_times else 0,
                    'median_execution_time': statistics.median(execution_times) if execution_times else 0,
                    'min_execution_time': min(execution_times) if execution_times else 0,
                    'max_execution_time': max(execution_times) if execution_times else 0,
                    'std_execution_time': statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
                    'average_result_rows': statistics.mean(result_rows) if result_rows else 0,
                    'total_result_rows': sum(result_rows),
                    'time_range': {
                        'start': min(m['timestamp'] for m in metrics_data),
                        'end': max(m['timestamp'] for m in metrics_data)
                    }
                }
            else:
                summary = {
                    'total_queries': 0,
                    'successful_queries': 0,
                    'failed_queries': 0,
                    'success_rate': 0,
                    'average_execution_time': 0,
                    'median_execution_time': 0,
                    'min_execution_time': 0,
                    'max_execution_time': 0,
                    'std_execution_time': 0,
                    'average_result_rows': 0,
                    'total_result_rows': 0,
                    'time_range': None
                }
            
            return {
                'summary': summary,
                'raw_data': metrics_data
            }
            
        except Exception as e:
            logger.error(f"Failed to get metrics summary: {e}")
            return {'summary': {}, 'raw_data': []}
    
    def get_format_comparison(self, time_range: Tuple[datetime, datetime] = None) -> Dict[str, Any]:
        """Get performance comparison between formats"""
        
        comparison = {}
        
        for format_name in ['native', 'iceberg_sf', 'iceberg_glue', 'external']:
            format_summary = self.get_metrics_summary(format_name=format_name, time_range=time_range)
            comparison[format_name] = format_summary['summary']
        
        return comparison
    
    def get_query_performance_ranking(self, format_name: str = None) -> List[Dict[str, Any]]:
        """Get query performance ranking"""
        
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            query = '''
                SELECT query_number, format, 
                       AVG(execution_time) as avg_time,
                       MIN(execution_time) as min_time,
                       MAX(execution_time) as max_time,
                       COUNT(*) as run_count,
                       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as success_count
                FROM query_metrics 
                WHERE execution_time > 0
            '''
            params = []
            
            if format_name:
                query += " AND format = ?"
                params.append(format_name)
            
            query += " GROUP BY query_number, format ORDER BY avg_time"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            ranking = []
            for row in rows:
                query_num, fmt, avg_time, min_time, max_time, run_count, success_count = row
                ranking.append({
                    'query_number': query_num,
                    'format': fmt,
                    'average_time': avg_time,
                    'min_time': min_time,
                    'max_time': max_time,
                    'run_count': run_count,
                    'success_count': success_count,
                    'success_rate': success_count / run_count if run_count > 0 else 0
                })
            
            conn.close()
            return ranking
            
        except Exception as e:
            logger.error(f"Failed to get query performance ranking: {e}")
            return []
    
    def export_metrics(self, output_file: Path, format_type: str = 'json') -> bool:
        """Export metrics to file"""
        
        try:
            if format_type.lower() == 'json':
                # Export as JSON
                all_metrics = self.get_metrics_summary()
                
                with open(output_file, 'w') as f:
                    json.dump(all_metrics, f, indent=2, default=str)
                
            elif format_type.lower() == 'csv':
                # Export as CSV
                import pandas as pd
                
                conn = sqlite3.connect(str(self.metrics_db_path))
                df = pd.read_sql_query("SELECT * FROM query_metrics", conn)
                conn.close()
                
                df.to_csv(output_file, index=False)
            
            else:
                logger.error(f"Unsupported export format: {format_type}")
                return False
            
            logger.info(f"Metrics exported to: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
            return False
    
    def cleanup_old_metrics(self, days_to_keep: int = 30):
        """Clean up old metrics data"""
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Delete old query metrics
            cursor.execute("DELETE FROM query_metrics WHERE timestamp < ?", (cutoff_date.isoformat(),))
            query_deleted = cursor.rowcount
            
            # Delete old system metrics
            cursor.execute("DELETE FROM system_metrics WHERE timestamp < ?", (cutoff_date.isoformat(),))
            system_deleted = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            logger.info(f"Cleaned up {query_deleted} query metrics and {system_deleted} system metrics older than {days_to_keep} days")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {e}")
    
    def get_cost_summary(self, format_name: str = None, 
                        time_range: Tuple[datetime, datetime] = None) -> Dict[str, Any]:
        """Get cost summary for analysis"""
        
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Build query
            query = "SELECT * FROM cost_metrics WHERE 1=1"
            params = []
            
            if format_name:
                query += " AND format = ?"
                params.append(format_name)
            
            if time_range:
                start_time, end_time = time_range
                query += " AND timestamp BETWEEN ? AND ?"
                params.extend([start_time.isoformat(), end_time.isoformat()])
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            # Convert to list of dictionaries
            cost_data = [dict(zip(columns, row)) for row in rows]
            
            conn.close()
            
            # Calculate summary statistics
            if cost_data:
                total_cost = sum(c.get('total_cost_usd', 0) for c in cost_data)
                total_credits = sum(c.get('compute_credits', 0) for c in cost_data)
                compute_cost = sum(c.get('compute_cost_usd', 0) for c in cost_data)
                storage_cost = sum(c.get('storage_cost_usd', 0) for c in cost_data)
                s3_storage_cost = sum(c.get('s3_storage_cost_usd', 0) for c in cost_data)
                s3_request_cost = sum(c.get('s3_request_cost_usd', 0) for c in cost_data)
                glue_cost = sum(c.get('glue_cost_usd', 0) for c in cost_data)
                
                summary = {
                    'total_queries': len(cost_data),
                    'total_cost_usd': total_cost,
                    'total_credits': total_credits,
                    'avg_cost_per_query': total_cost / len(cost_data) if cost_data else 0,
                    'cost_breakdown': {
                        'compute_cost_usd': compute_cost,
                        'storage_cost_usd': storage_cost,
                        's3_storage_cost_usd': s3_storage_cost,
                        's3_request_cost_usd': s3_request_cost,
                        'glue_cost_usd': glue_cost
                    },
                    'time_range': {
                        'start': min(c['timestamp'] for c in cost_data),
                        'end': max(c['timestamp'] for c in cost_data)
                    } if cost_data else None
                }
            else:
                summary = {
                    'total_queries': 0,
                    'total_cost_usd': 0,
                    'total_credits': 0,
                    'avg_cost_per_query': 0,
                    'cost_breakdown': {},
                    'time_range': None
                }
            
            return {
                'summary': summary,
                'raw_data': cost_data
            }
            
        except Exception as e:
            logger.error(f"Failed to get cost summary: {e}")
            return {'summary': {}, 'raw_data': []}
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        
        try:
            conn = sqlite3.connect(str(self.metrics_db_path))
            cursor = conn.cursor()
            
            # Get table sizes
            cursor.execute("SELECT COUNT(*) FROM query_metrics")
            query_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM system_metrics")
            system_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM cost_metrics")
            cost_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM warehouse_metrics")
            warehouse_count = cursor.fetchone()[0]
            
            # Get database size
            cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            db_size = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'query_metrics_count': query_count,
                'system_metrics_count': system_count,
                'cost_metrics_count': cost_count,
                'warehouse_metrics_count': warehouse_count,
                'database_size_bytes': db_size,
                'database_size_mb': db_size / 1024 / 1024,
                'database_path': str(self.metrics_db_path)
            }
            
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
