#!/usr/bin/env python3
"""
Data Optimizer for TPC-DS Performance Testing
Analyzes table statistics and provides clustering and partitioning recommendations
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class DataOptimizer:
    """Analyzes data organization and provides optimization recommendations"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize data optimizer"""
        self.config = config or {}
    
    def analyze_table_statistics(self, table_name: str, format_name: str,
                                row_count: int = 0,
                                table_size_bytes: int = 0,
                                column_statistics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze table statistics and provide optimization recommendations.
        
        Args:
            table_name: Table name
            format_name: Table format name
            row_count: Number of rows in table
            table_size_bytes: Table size in bytes
            column_statistics: Dictionary with column statistics
            
        Returns:
            Analysis dictionary with recommendations
        """
        analysis = {
            'table_name': table_name,
            'format': format_name,
            'row_count': row_count,
            'table_size_bytes': table_size_bytes,
            'table_size_mb': table_size_bytes / (1024 * 1024),
            'recommendations': [],
            'clustering_recommendations': [],
            'partitioning_recommendations': []
        }
        
        # Format-specific recommendations
        if format_name == 'native':
            analysis['clustering_recommendations'] = self._recommend_clustering_keys(table_name, row_count, column_statistics)
        elif format_name in ['iceberg_sf', 'iceberg_glue']:
            analysis['partitioning_recommendations'] = self._recommend_iceberg_partitioning(table_name, row_count, column_statistics)
        elif format_name == 'external':
            analysis['partitioning_recommendations'] = self._recommend_external_partitioning(table_name, row_count, column_statistics)
        
        # General recommendations
        analysis['recommendations'] = self._get_general_recommendations(row_count, table_size_bytes, format_name)
        
        return analysis
    
    def _recommend_clustering_keys(self, table_name: str, row_count: int,
                                   column_statistics: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Recommend clustering keys for native Snowflake tables"""
        recommendations = []
        
        # TPC-DS specific recommendations
        clustering_map = {
            'store_sales': ['ss_sold_date_sk', 'ss_item_sk'],
            'catalog_sales': ['cs_sold_date_sk', 'cs_item_sk'],
            'web_sales': ['ws_sold_date_sk', 'ws_item_sk'],
            'store_returns': ['sr_returned_date_sk', 'sr_item_sk'],
            'catalog_returns': ['cr_returned_date_sk', 'cr_item_sk'],
            'web_returns': ['wr_returned_date_sk', 'wr_item_sk'],
            'inventory': ['inv_date_sk', 'inv_item_sk'],
            'customer': ['c_customer_sk'],
            'item': ['i_item_sk'],
            'date_dim': ['d_date_sk']
        }
        
        if table_name in clustering_map:
            recommendations.append({
                'columns': clustering_map[table_name],
                'reason': f'Recommended clustering keys for {table_name} based on TPC-DS query patterns',
                'priority': 'high'
            })
        
        # General recommendations for large tables
        if row_count > 1000000:
            recommendations.append({
                'columns': ['date_sk', 'item_sk'],
                'reason': 'Large tables benefit from clustering on frequently joined columns',
                'priority': 'medium'
            })
        
        return recommendations
    
    def _recommend_iceberg_partitioning(self, table_name: str, row_count: int,
                                       column_statistics: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Recommend partitioning for Iceberg tables"""
        recommendations = []
        
        # Iceberg partitioning recommendations
        partitioning_map = {
            'store_sales': {
                'partition_by': ['ss_sold_date_sk'],
                'bucket_by': ['ss_item_sk'],
                'bucket_count': 16
            },
            'catalog_sales': {
                'partition_by': ['cs_sold_date_sk'],
                'bucket_by': ['cs_item_sk'],
                'bucket_count': 16
            },
            'web_sales': {
                'partition_by': ['ws_sold_date_sk'],
                'bucket_by': ['ws_item_sk'],
                'bucket_count': 16
            },
            'inventory': {
                'partition_by': ['inv_date_sk'],
                'bucket_by': ['inv_item_sk'],
                'bucket_count': 8
            }
        }
        
        if table_name in partitioning_map:
            partitioning = partitioning_map[table_name]
            recommendations.append({
                'partition_by': partitioning['partition_by'],
                'bucket_by': partitioning.get('bucket_by', []),
                'bucket_count': partitioning.get('bucket_count', 16),
                'reason': f'Recommended partitioning strategy for {table_name}',
                'priority': 'high'
            })
        elif row_count > 1000000:
            recommendations.append({
                'partition_by': ['date_sk'],
                'bucket_by': [],
                'bucket_count': 0,
                'reason': 'Large tables should use date-based partitioning',
                'priority': 'medium'
            })
        
        return recommendations
    
    def _recommend_external_partitioning(self, table_name: str, row_count: int,
                                        column_statistics: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Recommend partitioning for external tables"""
        recommendations = []
        
        # External tables use S3 path-based partitioning
        if row_count > 1000000:
            recommendations.append({
                'partition_by': ['date_sk'],
                's3_path_structure': 's3://bucket/table/date_sk=YYYYMMDD/',
                'reason': 'Use S3 path-based partitioning for external tables',
                'priority': 'high'
            })
        
        return recommendations
    
    def _get_general_recommendations(self, row_count: int, table_size_bytes: int,
                                    format_name: str) -> List[str]:
        """Get general data organization recommendations"""
        recommendations = []
        
        table_size_mb = table_size_bytes / (1024 * 1024)
        
        # File size recommendations
        if format_name in ['iceberg_sf', 'iceberg_glue']:
            if table_size_mb > 1000:
                recommendations.append("Consider running table compaction to optimize file sizes (target 128MB per file)")
        
        # Compression recommendations
        if table_size_mb > 100:
            recommendations.append("Ensure Parquet files use appropriate compression (Snappy for balanced performance)")
        
        # Partitioning recommendations
        if row_count > 10000000:
            recommendations.append("Large tables should use partitioning to improve query performance")
        
        return recommendations
    
    def recommend_compaction_strategy(self, format_name: str, 
                                     small_file_count: int = 0,
                                     avg_file_size_mb: float = 0) -> Dict[str, Any]:
        """Recommend table compaction strategy"""
        
        if format_name not in ['iceberg_sf', 'iceberg_glue']:
            return {'recommendation': 'Compaction not applicable for this format'}
        
        recommendations = {
            'format': format_name,
            'should_compact': False,
            'reason': '',
            'compaction_settings': {}
        }
        
        # Recommend compaction if there are many small files
        if small_file_count > 100:
            recommendations['should_compact'] = True
            recommendations['reason'] = f'Large number of small files ({small_file_count}) detected'
            recommendations['compaction_settings'] = {
                'target_file_size': '128MB',
                'min_file_size': '64MB',
                'max_file_size': '256MB'
            }
        elif avg_file_size_mb < 64:
            recommendations['should_compact'] = True
            recommendations['reason'] = f'Average file size ({avg_file_size_mb:.2f}MB) is below optimal threshold'
            recommendations['compaction_settings'] = {
                'target_file_size': '128MB',
                'min_file_size': '64MB'
            }
        
        return recommendations
    
    def get_format_specific_recommendations(self, format_name: str) -> List[str]:
        """Get format-specific data organization recommendations"""
        recommendations = {
            'native': [
                'Use clustering keys on frequently queried columns',
                'Monitor clustering effectiveness and recluster if needed',
                'Consider micro-partitions alignment with query patterns'
            ],
            'iceberg_sf': [
                'Use appropriate partitioning strategy (date-based for time-series)',
                'Maintain optimal file sizes (128MB target)',
                'Run periodic compaction to merge small files',
                'Use bucket partitioning for large tables'
            ],
            'iceberg_glue': [
                'Use Iceberg partitioning with Glue catalog',
                'Optimize S3 file organization',
                'Run compaction using Spark with Iceberg extensions',
                'Monitor partition pruning effectiveness'
            ],
            'external': [
                'Use S3 path-based partitioning',
                'Organize files in partition directories',
                'Optimize Parquet file compression',
                'Use partition columns in WHERE clauses'
            ]
        }
        
        return recommendations.get(format_name, [])

