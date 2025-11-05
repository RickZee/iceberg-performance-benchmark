#!/usr/bin/env python3
"""
Query Optimizer for TPC-DS Performance Testing
Analyzes query execution plans and provides format-specific optimization recommendations
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class QueryOptimizer:
    """Analyzes queries and provides optimization recommendations"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize query optimizer"""
        self.config = config or {}
    
    def analyze_query(self, query_content: str, format_name: str, 
                     execution_time: float = 0,
                     bytes_scanned: int = 0) -> Dict[str, Any]:
        """
        Analyze a query and provide optimization recommendations.
        
        Args:
            query_content: SQL query content
            format_name: Table format name
            execution_time: Query execution time in seconds
            bytes_scanned: Bytes scanned by the query
            
        Returns:
            Dictionary with analysis and recommendations
        """
        analysis = {
            'format': format_name,
            'execution_time': execution_time,
            'bytes_scanned': bytes_scanned,
            'recommendations': [],
            'optimization_opportunities': []
        }
        
        # Format-specific optimizations
        if format_name == 'native':
            analysis['recommendations'].extend(self._analyze_native_optimizations(query_content, execution_time))
        elif format_name == 'iceberg_sf':
            analysis['recommendations'].extend(self._analyze_iceberg_sf_optimizations(query_content, execution_time))
        elif format_name == 'iceberg_glue':
            analysis['recommendations'].extend(self._analyze_iceberg_glue_optimizations(query_content, execution_time))
        elif format_name == 'external':
            analysis['recommendations'].extend(self._analyze_external_optimizations(query_content, execution_time))
        
        # General optimizations
        analysis['recommendations'].extend(self._analyze_general_optimizations(query_content, execution_time, bytes_scanned))
        
        # Identify optimization opportunities
        analysis['optimization_opportunities'] = self._identify_optimization_opportunities(query_content, format_name, execution_time)
        
        return analysis
    
    def _analyze_native_optimizations(self, query_content: str, execution_time: float) -> List[str]:
        """Analyze optimizations for native Snowflake tables"""
        recommendations = []
        
        query_upper = query_content.upper()
        
        # Check for clustering key usage
        if 'WHERE' in query_upper and execution_time > 1.0:
            recommendations.append("Consider using clustering keys on frequently filtered columns for native tables")
        
        # Check for result caching
        if execution_time > 0.5:
            recommendations.append("Enable result caching for repeated queries on native tables")
        
        # Check for materialized views
        if 'JOIN' in query_upper and execution_time > 2.0:
            recommendations.append("Consider creating materialized views for complex joins on native tables")
        
        return recommendations
    
    def _analyze_iceberg_sf_optimizations(self, query_content: str, execution_time: float) -> List[str]:
        """Analyze optimizations for Iceberg Snowflake-managed tables"""
        recommendations = []
        
        query_upper = query_content.upper()
        
        # Check for partition pruning
        if 'WHERE' in query_upper and execution_time > 1.0:
            recommendations.append("Ensure WHERE clauses include partition columns for Iceberg partition pruning")
        
        # Check for file size optimization
        if execution_time > 1.0:
            recommendations.append("Consider optimizing Iceberg file sizes (target 128MB) for better performance")
        
        # Check for compaction
        if execution_time > 2.0:
            recommendations.append("Consider running table compaction to merge small files in Iceberg tables")
        
        return recommendations
    
    def _analyze_iceberg_glue_optimizations(self, query_content: str, execution_time: float) -> List[str]:
        """Analyze optimizations for Iceberg Glue-managed tables"""
        recommendations = []
        
        query_upper = query_content.upper()
        
        # Check for partition pruning
        if 'WHERE' in query_upper and execution_time > 1.5:
            recommendations.append("Optimize WHERE clauses for Iceberg partition pruning on Glue-managed tables")
        
        # Check for Glue API calls
        if execution_time > 1.0:
            recommendations.append("Minimize Glue catalog API calls by caching table metadata")
        
        # Check for S3 request optimization
        if execution_time > 1.0:
            recommendations.append("Optimize S3 request patterns by using larger file sizes and better partitioning")
        
        return recommendations
    
    def _analyze_external_optimizations(self, query_content: str, execution_time: float) -> List[str]:
        """Analyze optimizations for external tables"""
        recommendations = []
        
        query_upper = query_content.upper()
        
        # Check for file format optimization
        if execution_time > 1.0:
            recommendations.append("Ensure Parquet files are properly compressed and optimized for external tables")
        
        # Check for partition pruning
        if 'WHERE' in query_upper and execution_time > 1.0:
            recommendations.append("Use partition columns in WHERE clauses for external table partition pruning")
        
        # Check for S3 optimization
        if execution_time > 1.0:
            recommendations.append("Optimize S3 request patterns and use S3 Select for filtered queries on external tables")
        
        return recommendations
    
    def _analyze_general_optimizations(self, query_content: str, execution_time: float, bytes_scanned: int) -> List[str]:
        """Analyze general query optimizations"""
        recommendations = []
        
        query_upper = query_content.upper()
        
        # Check for SELECT *
        if 'SELECT *' in query_upper:
            recommendations.append("Avoid SELECT * - specify only needed columns to reduce data scanning")
        
        # Check for large result sets
        if bytes_scanned > 100 * 1024 * 1024:  # 100 MB
            recommendations.append(f"Query scans {bytes_scanned / (1024*1024):.2f} MB - consider adding filters or reducing data scanned")
        
        # Check for missing LIMIT
        if 'LIMIT' not in query_upper and execution_time > 1.0:
            recommendations.append("Consider adding LIMIT clause if full result set is not needed")
        
        # Check for suboptimal joins
        if query_upper.count('JOIN') > 3 and execution_time > 2.0:
            recommendations.append("Consider optimizing join order or using CTEs for complex multi-join queries")
        
        # Check for window functions
        if 'OVER (' in query_upper and execution_time > 1.0:
            recommendations.append("Optimize window functions by using appropriate PARTITION BY and ORDER BY clauses")
        
        return recommendations
    
    def _identify_optimization_opportunities(self, query_content: str, format_name: str, execution_time: float) -> List[Dict[str, Any]]:
        """Identify specific optimization opportunities"""
        opportunities = []
        
        query_upper = query_content.upper()
        
        # Slow query opportunity
        if execution_time > 5.0:
            opportunities.append({
                'type': 'slow_query',
                'priority': 'high',
                'description': f'Query execution time ({execution_time:.2f}s) exceeds optimal threshold',
                'suggestion': 'Consider query optimization or format-specific tuning'
            })
        
        # Missing index/clustering opportunity
        if 'WHERE' in query_upper and execution_time > 1.0:
            opportunities.append({
                'type': 'missing_index',
                'priority': 'medium',
                'description': 'Query with WHERE clause may benefit from clustering or indexing',
                'suggestion': f'Review clustering keys for {format_name} format'
            })
        
        # Large scan opportunity
        if 'SELECT *' in query_upper:
            opportunities.append({
                'type': 'column_selection',
                'priority': 'medium',
                'description': 'Query uses SELECT * which may scan unnecessary columns',
                'suggestion': 'Specify only required columns'
            })
        
        return opportunities
    
    def get_format_specific_recommendations(self, format_name: str) -> List[str]:
        """Get format-specific general recommendations"""
        recommendations = {
            'native': [
                'Use clustering keys on frequently queried columns',
                'Enable result caching for repeated queries',
                'Consider materialized views for complex aggregations',
                'Monitor and optimize warehouse size'
            ],
            'iceberg_sf': [
                'Optimize Iceberg table partitioning',
                'Maintain optimal file sizes (128MB target)',
                'Run periodic table compaction',
                'Use partition columns in WHERE clauses'
            ],
            'iceberg_glue': [
                'Minimize Glue catalog API calls',
                'Optimize S3 request patterns',
                'Use proper Iceberg partitioning',
                'Cache table metadata when possible'
            ],
            'external': [
                'Optimize Parquet file compression',
                'Use partition columns in WHERE clauses',
                'Optimize S3 request patterns',
                'Consider S3 Select for filtered queries'
            ]
        }
        
        return recommendations.get(format_name, [])

