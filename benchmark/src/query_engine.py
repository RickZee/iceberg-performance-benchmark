#!/usr/bin/env python3
"""
Query Execution Engine for TPC-DS Performance Testing
Handles query execution, retry logic, and performance monitoring
"""

import time
import logging
import threading
from typing import Dict, List, Any, Optional, Tuple, Callable
from pathlib import Path
from datetime import datetime
import signal
import psutil
import os

logger = logging.getLogger(__name__)

class QueryExecutionEngine:
    """Advanced query execution engine with performance monitoring"""
    
    def __init__(self, connector, config: Dict[str, Any]):
        """Initialize the query execution engine"""
        self.connector = connector
        self.config = config
        self.active_queries = {}
        self.query_history = []
        self.performance_stats = {}
        
    def execute_query_with_monitoring(self, 
                                    query_file: Path, 
                                    format_name: str, 
                                    query_num: int,
                                    callback: Optional[Callable] = None) -> Dict[str, Any]:
        """Execute query with comprehensive monitoring"""
        
        query_id = f"{format_name}_q{query_num}"
        start_time = time.time()
        
        # Read query content
        try:
            with open(query_file, 'r') as f:
                query_content = f.read()
        except Exception as e:
            return self._create_error_result(query_num, format_name, f"Failed to read query file: {e}")
        
        # Prepare query
        prepared_query = self._prepare_query(query_content)
        
        # Initialize monitoring
        monitor_data = {
            'query_id': query_id,
            'start_time': start_time,
            'format': format_name,
            'query_number': query_num,
            'query_file': query_file.name
        }
        
        self.active_queries[query_id] = monitor_data
        
        try:
            # Execute with timeout and monitoring
            result = self._execute_with_timeout(prepared_query, format_name, query_id)
            
            # Calculate final metrics
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Create result
            execution_result = {
                'query_number': query_num,
                'query_file': query_file.name,
                'format': format_name,
                'status': 'success',
                'execution_time': execution_time,
                'start_time': start_time,
                'end_time': end_time,
                'result_rows': len(result) if result else 0,
                'query_content': prepared_query,
                'monitoring_data': monitor_data,
                'timestamp': datetime.now().isoformat()
            }
            
            # Call callback if provided
            if callback:
                callback(execution_result)
            
            return execution_result
            
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            
            error_result = self._create_error_result(
                query_num, format_name, str(e), execution_time, start_time, end_time
            )
            
            if callback:
                callback(error_result)
            
            return error_result
            
        finally:
            # Clean up monitoring
            if query_id in self.active_queries:
                del self.active_queries[query_id]
    
    def execute_multiple_runs(self, 
                            query_file: Path, 
                            format_name: str, 
                            query_num: int,
                            warmup_runs: int = 1,
                            test_runs: int = 3) -> Dict[str, Any]:
        """Execute query multiple times for accurate performance measurement"""
        
        execution_times = []
        row_counts = []
        errors = []
        all_results = []
        
        logger.info(f"Executing query {query_num} for format {format_name} ({warmup_runs} warmup + {test_runs} test runs)")
        
        for run_num in range(warmup_runs + test_runs):
            run_type = "warmup" if run_num < warmup_runs else "test"
            
            try:
                result = self.execute_query_with_monitoring(query_file, format_name, query_num)
                
                if result['status'] == 'success':
                    execution_time = result['execution_time']
                    row_count = result['result_rows']
                    
                    if run_type == "test":
                        execution_times.append(execution_time)
                        row_counts.append(row_count)
                    
                    logger.debug(f"Query {query_num} {run_type} run {run_num+1}: {execution_time:.2f}s, {row_count} rows")
                else:
                    errors.append(result.get('error', 'Unknown error'))
                    logger.warning(f"Query {query_num} {run_type} run {run_num+1} failed: {result.get('error', 'Unknown error')}")
                
                all_results.append(result)
                
                # Small delay between runs
                if run_num < warmup_runs + test_runs - 1:
                    time.sleep(0.5)
                    
            except Exception as e:
                error_msg = f"Run {run_num+1} failed: {str(e)}"
                errors.append(error_msg)
                logger.error(f"Query {query_num} {run_type} run {run_num+1} error: {error_msg}")
        
        # Calculate aggregated metrics
        return self._aggregate_results(query_num, format_name, query_file.name, 
                                     execution_times, row_counts, errors, all_results)
    
    def _execute_with_timeout(self, query: str, format_name: str, query_id: str) -> Any:
        """Execute query with timeout and resource monitoring"""
        
        timeout = self.config.get('query_settings', {}).get('timeout_per_query', 600)
        
        # Start monitoring thread
        monitor_thread = threading.Thread(
            target=self._monitor_query_execution, 
            args=(query_id, timeout)
        )
        monitor_thread.daemon = True
        monitor_thread.start()
        
        try:
            # Execute query
            result = self.connector.execute_query(format_name, query)
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed for {query_id}: {e}")
            raise
    
    def _monitor_query_execution(self, query_id: str, timeout: float):
        """Monitor query execution for resource usage and timeout"""
        
        start_time = time.time()
        process = psutil.Process(os.getpid())
        
        while query_id in self.active_queries:
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            # Check timeout
            if elapsed_time > timeout:
                logger.warning(f"Query {query_id} exceeded timeout of {timeout}s")
                break
            
            # Monitor resource usage
            try:
                cpu_percent = process.cpu_percent()
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                
                # Log resource usage every 30 seconds
                if int(elapsed_time) % 30 == 0 and elapsed_time > 0:
                    logger.debug(f"Query {query_id} - Time: {elapsed_time:.1f}s, CPU: {cpu_percent:.1f}%, Memory: {memory_mb:.1f}MB")
                
                # Check memory limit
                memory_limit = self.config.get('performance_thresholds', {}).get('memory_limit_mb', 1024)
                if memory_mb > memory_limit:
                    logger.warning(f"Query {query_id} exceeded memory limit: {memory_mb:.1f}MB > {memory_limit}MB")
                
            except Exception as e:
                logger.debug(f"Resource monitoring error for {query_id}: {e}")
            
            time.sleep(1)
    
    def _prepare_query(self, query_content: str) -> str:
        """Prepare query for execution"""
        
        # Add safety limit if configured and not present
        if self.config.get('query_settings', {}).get('add_safety_limit', True):
            if 'LIMIT' not in query_content.upper():
                query_content += '\nLIMIT 1000;'
        
        return query_content
    
    def _create_error_result(self, query_num: int, format_name: str, error_msg: str, 
                           execution_time: float = 0, start_time: float = 0, end_time: float = 0) -> Dict[str, Any]:
        """Create error result structure"""
        
        return {
            'query_number': query_num,
            'format': format_name,
            'status': 'error',
            'error': error_msg,
            'execution_time': execution_time,
            'start_time': start_time,
            'end_time': end_time,
            'result_rows': 0,
            'timestamp': datetime.now().isoformat()
        }
    
    def _aggregate_results(self, query_num: int, format_name: str, query_file: str,
                         execution_times: List[float], row_counts: List[int], 
                         errors: List[str], all_results: List[Dict]) -> Dict[str, Any]:
        """Aggregate multiple execution results"""
        
        import statistics
        
        # Calculate statistics
        if execution_times:
            avg_time = statistics.mean(execution_times)
            min_time = min(execution_times)
            max_time = max(execution_times)
            std_time = statistics.stdev(execution_times) if len(execution_times) > 1 else 0
            median_time = statistics.median(execution_times)
        else:
            avg_time = min_time = max_time = std_time = median_time = 0
        
        if row_counts:
            avg_rows = statistics.mean(row_counts)
            min_rows = min(row_counts)
            max_rows = max(row_counts)
        else:
            avg_rows = min_rows = max_rows = 0
        
        # Determine status
        total_runs = len(all_results)
        successful_runs = len(execution_times)
        
        if successful_runs == 0:
            status = 'failed'
        elif successful_runs < total_runs:
            status = 'partial'
        else:
            status = 'success'
        
        return {
            'query_number': query_num,
            'query_file': query_file,
            'format': format_name,
            'status': status,
            'execution_times': execution_times,
            'row_counts': row_counts,
            'average_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'median_time': median_time,
            'std_time': std_time,
            'average_rows': avg_rows,
            'min_rows': min_rows,
            'max_rows': max_rows,
            'errors': errors,
            'error_count': len(errors),
            'success_rate': successful_runs / total_runs if total_runs > 0 else 0,
            'total_runs': total_runs,
            'successful_runs': successful_runs,
            'all_results': all_results,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_active_queries(self) -> Dict[str, Dict[str, Any]]:
        """Get currently active queries"""
        return self.active_queries.copy()
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return self.performance_stats.copy()
    
    def clear_history(self):
        """Clear query execution history"""
        self.query_history.clear()
        self.performance_stats.clear()

class QueryRetryManager:
    """Manages query retry logic with exponential backoff"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize retry manager"""
        self.config = config
        self.max_retries = config.get('test_settings', {}).get('retry_attempts', 3)
        self.base_delay = 1.0  # Base delay in seconds
        self.max_delay = 30.0  # Maximum delay in seconds
    
    def execute_with_retry(self, execution_func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return execution_func(*args, **kwargs)
                
            except Exception as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.1f}s: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_retries + 1} attempts failed")
        
        # If we get here, all retries failed
        raise last_exception

class QueryValidator:
    """Validates queries before execution"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize query validator"""
        self.config = config
    
    def validate_query(self, query_content: str, format_name: str) -> Tuple[bool, List[str]]:
        """Validate query content and format compatibility"""
        
        issues = []
        
        # Basic SQL validation
        if not query_content.strip():
            issues.append("Query is empty")
            return False, issues
        
        # Check for required keywords
        query_upper = query_content.upper()
        if 'SELECT' not in query_upper:
            issues.append("Query must contain SELECT statement")
        
        if 'FROM' not in query_upper:
            issues.append("Query must contain FROM clause")
        
        # Check for potentially dangerous operations
        dangerous_ops = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
        for op in dangerous_ops:
            if op in query_upper:
                issues.append(f"Query contains potentially dangerous operation: {op}")
        
        # Check for format-specific issues
        if format_name == 'external':
            # External tables might have different syntax requirements
            if 'USE SCHEMA' in query_upper:
                issues.append("External format queries should not use USE SCHEMA")
        
        # Check query length
        max_length = self.config.get('query_settings', {}).get('max_query_length', 100000)
        if len(query_content) > max_length:
            issues.append(f"Query too long: {len(query_content)} characters (max: {max_length})")
        
        return len(issues) == 0, issues
