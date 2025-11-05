#!/usr/bin/env python3
"""
AWS Cost Tracker for TPC-DS Performance Testing
Tracks S3 usage, Glue API calls, and calculates AWS costs
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logging.warning("boto3 not available. AWS cost tracking will be limited.")

logger = logging.getLogger(__name__)

class AWSCostTracker:
    """Tracks AWS costs for S3 and Glue services"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize AWS cost tracker"""
        self.config = config or {}
        
        # AWS configuration
        aws_config = self.config.get('aws', {}) or {}
        self.region = aws_config.get('region', 'us-east-1')
        self.s3_bucket = aws_config.get('s3_bucket') or aws_config.get('bucket')
        self.s3_prefix = aws_config.get('s3_prefix', 'iceberg-performance-test')
        
        # Initialize AWS clients if boto3 is available
        if BOTO3_AVAILABLE:
            try:
                self.s3_client = boto3.client('s3', region_name=self.region)
                self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.region)
                self.glue_client = boto3.client('glue', region_name=self.region)
                # Cost Explorer requires special permissions, make it optional
                try:
                    self.ce_client = boto3.client('ce', region_name='us-east-1')  # Cost Explorer only in us-east-1
                except Exception:
                    self.ce_client = None
                    logger.warning("Cost Explorer API not available. Cost tracking will use CloudWatch metrics.")
            except Exception as e:
                logger.warning(f"Failed to initialize AWS clients: {e}. Cost tracking will be limited.")
                self.s3_client = None
                self.cloudwatch_client = None
                self.glue_client = None
                self.ce_client = None
        else:
            self.s3_client = None
            self.cloudwatch_client = None
            self.glue_client = None
            self.ce_client = None
    
    def get_s3_storage_usage(self, bucket: Optional[str] = None, prefix: Optional[str] = None) -> Dict[str, Any]:
        """
        Get S3 storage usage for a bucket/prefix.
        
        Args:
            bucket: S3 bucket name (default: from config)
            prefix: S3 prefix (default: from config)
            
        Returns:
            Dictionary with storage usage information
        """
        bucket = bucket or self.s3_bucket
        prefix = prefix or self.s3_prefix
        
        if not BOTO3_AVAILABLE or not self.s3_client or not bucket:
            logger.warning("S3 client not available. Cannot get storage usage.")
            return {
                'bucket': bucket,
                'prefix': prefix,
                'total_size_bytes': 0,
                'total_size_gb': 0,
                'object_count': 0,
                'error': 'S3 client not available'
            }
        
        try:
            total_size = 0
            object_count = 0
            
            # List objects with pagination
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj.get('Size', 0)
                        object_count += 1
            
            return {
                'bucket': bucket,
                'prefix': prefix,
                'total_size_bytes': total_size,
                'total_size_gb': total_size / (1024 ** 3),
                'object_count': object_count,
                'timestamp': datetime.now().isoformat()
            }
            
        except ClientError as e:
            logger.error(f"Error getting S3 storage usage: {e}")
            return {
                'bucket': bucket,
                'prefix': prefix,
                'total_size_bytes': 0,
                'total_size_gb': 0,
                'object_count': 0,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error getting S3 storage usage: {e}")
            return {
                'bucket': bucket,
                'prefix': prefix,
                'total_size_bytes': 0,
                'total_size_gb': 0,
                'object_count': 0,
                'error': str(e)
            }
    
    def get_s3_request_metrics(self, bucket: Optional[str] = None, 
                               start_time: Optional[datetime] = None,
                               end_time: Optional[datetime] = None) -> Dict[str, int]:
        """
        Get S3 request metrics from CloudWatch.
        
        Args:
            bucket: S3 bucket name (default: from config)
            start_time: Start time for metrics (default: 24 hours ago)
            end_time: End time for metrics (default: now)
            
        Returns:
            Dictionary with request counts (GetRequests, PutRequests, ListRequests)
        """
        bucket = bucket or self.s3_bucket
        
        if not BOTO3_AVAILABLE or not self.cloudwatch_client or not bucket:
            logger.warning("CloudWatch client not available. Cannot get S3 request metrics.")
            return {
                'GetRequests': 0,
                'PutRequests': 0,
                'ListRequests': 0,
                'error': 'CloudWatch client not available'
            }
        
        if not start_time:
            start_time = datetime.now() - timedelta(days=1)
        if not end_time:
            end_time = datetime.now()
        
        try:
            metrics = {
                'GetRequests': 0,
                'PutRequests': 0,
                'ListRequests': 0
            }
            
            # Get metrics for each request type
            for metric_name in ['GetRequests', 'PutRequests', 'ListRequests']:
                try:
                    response = self.cloudwatch_client.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName=metric_name,
                        Dimensions=[
                            {'Name': 'BucketName', 'Value': bucket},
                            {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,  # 1 hour periods
                        Statistics=['Sum']
                    )
                    
                    # Sum up all datapoints
                    if response.get('Datapoints'):
                        metrics[metric_name] = int(sum(dp['Sum'] for dp in response['Datapoints']))
                        
                except ClientError as e:
                    logger.warning(f"Error getting {metric_name} metric: {e}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting S3 request metrics: {e}")
            return {
                'GetRequests': 0,
                'PutRequests': 0,
                'ListRequests': 0,
                'error': str(e)
            }
    
    def get_glue_api_calls(self, database_name: Optional[str] = None,
                           start_time: Optional[datetime] = None,
                           end_time: Optional[datetime] = None) -> Dict[str, int]:
        """
        Get Glue API call counts from CloudTrail or CloudWatch.
        
        Note: This is a simplified implementation. Full tracking requires CloudTrail.
        
        Args:
            database_name: Glue database name
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Dictionary with API call counts
        """
        if not BOTO3_AVAILABLE or not self.glue_client:
            logger.warning("Glue client not available. Cannot get API call counts.")
            return {
                'GetTable': 0,
                'GetDatabase': 0,
                'CreateTable': 0,
                'UpdateTable': 0,
                'error': 'Glue client not available'
            }
        
        # Note: Accurate API call tracking requires CloudTrail analysis
        # This is a placeholder that returns estimated counts
        # In production, you would query CloudTrail logs
        
        return {
            'GetTable': 0,  # Estimated - would need CloudTrail analysis
            'GetDatabase': 0,
            'CreateTable': 0,
            'UpdateTable': 0,
            'note': 'Accurate counts require CloudTrail analysis'
        }
    
    def get_glue_metadata_storage(self, database_name: Optional[str] = None) -> float:
        """
        Get Glue metadata storage size.
        
        Args:
            database_name: Glue database name
            
        Returns:
            Metadata storage size in TB (estimated)
        """
        if not BOTO3_AVAILABLE or not self.glue_client:
            logger.warning("Glue client not available. Cannot get metadata storage.")
            return 0.0
        
        # Glue metadata storage is typically very small (MB to GB range)
        # This is a simplified estimation
        # In production, you would calculate actual metadata size
        
        return 0.0  # Estimated - would need detailed analysis
    
    def get_cost_from_cost_explorer(self, service: str = 'S3',
                                   start_date: Optional[str] = None,
                                   end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get costs from AWS Cost Explorer API.
        
        Args:
            service: AWS service name (S3, Glue, etc.)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Dictionary with cost information
        """
        if not self.ce_client:
            logger.warning("Cost Explorer API not available.")
            return {
                'total_cost': 0.0,
                'error': 'Cost Explorer API not available'
            }
        
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            response = self.ce_client.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date,
                    'End': end_date
                },
                Granularity='MONTHLY',
                Metrics=['UnblendedCost'],
                Filter={
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': [service]
                    }
                }
            )
            
            total_cost = 0.0
            for result in response.get('ResultsByTime', []):
                amount = result.get('Total', {}).get('UnblendedCost', {}).get('Amount', '0')
                total_cost += float(amount)
            
            return {
                'service': service,
                'start_date': start_date,
                'end_date': end_date,
                'total_cost': total_cost,
                'currency': 'USD'
            }
            
        except ClientError as e:
            logger.error(f"Error getting cost from Cost Explorer: {e}")
            return {
                'total_cost': 0.0,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error getting cost from Cost Explorer: {e}")
            return {
                'total_cost': 0.0,
                'error': str(e)
            }
    
    def get_format_s3_usage(self, format_name: str) -> Dict[str, Any]:
        """
        Get S3 usage for a specific format.
        
        Args:
            format_name: Format name (native, iceberg_sf, iceberg_glue, external)
            
        Returns:
            S3 usage information
        """
        # Map format names to S3 prefixes
        format_prefixes = {
            'native': None,  # Native format doesn't use S3
            'iceberg_sf': f'{self.s3_prefix}/iceberg_sf_format/',
            'iceberg_glue': f'{self.s3_prefix}/iceberg_glue_format/',
            'external': f'{self.s3_prefix}/external_format/'
        }
        
        prefix = format_prefixes.get(format_name)
        
        if not prefix:
            return {
                'format': format_name,
                'total_size_bytes': 0,
                'total_size_gb': 0,
                'object_count': 0,
                'note': 'Format does not use S3 storage'
            }
        
        return self.get_s3_storage_usage(prefix=prefix)
    
    def estimate_query_s3_requests(self, format_name: str, 
                                   bytes_scanned: int = 0) -> Dict[str, int]:
        """
        Estimate S3 requests for a query based on bytes scanned.
        
        This is an estimation based on typical S3 request patterns.
        
        Args:
            format_name: Format name
            bytes_scanned: Bytes scanned by the query
            
        Returns:
            Estimated request counts
        """
        # Estimate requests based on bytes scanned
        # Typical S3 GET request retrieves 1-128 MB
        # This is a simplified estimation
        
        if format_name not in ['iceberg_glue', 'external', 'iceberg_sf']:
            return {'get': 0, 'put': 0, 'list': 0}
        
        # Estimate GET requests (assuming 64MB per request average)
        avg_bytes_per_get = 64 * 1024 * 1024  # 64 MB
        estimated_gets = max(1, int(bytes_scanned / avg_bytes_per_get))
        
        # LIST requests for metadata (typically 1-2 per query)
        estimated_lists = 1
        
        # PUT requests typically only during data loading, not queries
        estimated_puts = 0
        
        return {
            'get': estimated_gets,
            'put': estimated_puts,
            'list': estimated_lists
        }
    
    def get_comprehensive_cost_data(self, format_name: str,
                                   start_time: Optional[datetime] = None,
                                   end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get comprehensive cost data for a format.
        
        Args:
            format_name: Format name
            start_time: Start time for metrics
            end_time: End time for metrics
            
        Returns:
            Comprehensive cost data dictionary
        """
        # Get S3 storage usage
        s3_usage = self.get_format_s3_usage(format_name)
        
        # Get S3 request metrics
        s3_requests = self.get_s3_request_metrics(start_time=start_time, end_time=end_time)
        
        # Get Glue API calls (if applicable)
        glue_calls = {}
        glue_metadata = 0.0
        if format_name == 'iceberg_glue':
            glue_calls = self.get_glue_api_calls(start_time=start_time, end_time=end_time)
            glue_metadata = self.get_glue_metadata_storage()
        
        return {
            'format': format_name,
            's3_storage_gb': s3_usage.get('total_size_gb', 0),
            's3_storage_bytes': s3_usage.get('total_size_bytes', 0),
            's3_requests': {
                'get': s3_requests.get('GetRequests', 0),
                'put': s3_requests.get('PutRequests', 0),
                'list': s3_requests.get('ListRequests', 0)
            },
            'glue_api_calls': glue_calls,
            'glue_metadata_storage_tb': glue_metadata,
            'timestamp': datetime.now().isoformat()
        }

