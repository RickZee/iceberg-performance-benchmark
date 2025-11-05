#!/usr/bin/env python3
"""
Cost Calculator for TPC-DS Performance Testing
Calculates costs for Snowflake compute, storage, AWS S3, and Glue services
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class CostCalculator:
    """Calculates costs for different components of the benchmark"""
    
    # Warehouse size multipliers for credit calculation
    WAREHOUSE_SIZE_MULTIPLIERS = {
        'X-Small': 1,
        'XSMALL': 1,
        'Small': 2,
        'SMALL': 2,
        'Medium': 4,
        'MEDIUM': 4,
        'Large': 8,
        'LARGE': 8,
        'X-Large': 16,
        'XLARGE': 16,
        '2X-Large': 32,
        '2XLARGE': 32,
        '3X-Large': 64,
        '3XLARGE': 64,
        '4X-Large': 128,
        '4XLARGE': 128
    }
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize cost calculator with pricing configuration"""
        self.config = config or {}
        
        # Load pricing from config with defaults
        pricing_config = self.config.get('cost_pricing', {})
        
        # Snowflake pricing (default: $3/credit)
        self.snowflake_credit_price = pricing_config.get('snowflake_credit_price_usd', 3.0)
        self.snowflake_storage_price_per_tb = pricing_config.get('snowflake_storage_price_per_tb_usd', 31.5)  # Average of $23-40
        
        # AWS S3 pricing (default: us-east-1)
        s3_pricing = pricing_config.get('aws_s3', {})
        self.s3_storage_standard_per_gb = s3_pricing.get('storage_standard_per_gb_usd', 0.023)
        self.s3_storage_ia_per_gb = s3_pricing.get('storage_ia_per_gb_usd', 0.0125)
        self.s3_storage_glacier_per_gb = s3_pricing.get('storage_glacier_per_gb_usd', 0.004)
        self.s3_request_get_per_1k = s3_pricing.get('request_get_per_1k_usd', 0.0004)
        self.s3_request_put_per_1k = s3_pricing.get('request_put_per_1k_usd', 0.005)
        self.s3_request_list_per_1k = s3_pricing.get('request_list_per_1k_usd', 0.0005)
        
        # AWS Glue pricing
        glue_pricing = pricing_config.get('aws_glue', {})
        self.glue_metadata_storage_per_tb = glue_pricing.get('metadata_storage_per_tb_usd', 1.0)
        self.glue_api_get_per_1k = glue_pricing.get('api_get_per_1k_usd', 0.0004)
        self.glue_api_create_update_per_1k = glue_pricing.get('api_create_update_per_1k_usd', 0.001)
        
        # Data transfer pricing
        transfer_pricing = pricing_config.get('data_transfer', {})
        self.data_transfer_cross_region_per_gb = transfer_pricing.get('cross_region_per_gb_usd', 0.02)
        
        logger.info(f"Cost calculator initialized with pricing: Snowflake credit=${self.snowflake_credit_price}, Storage=${self.snowflake_storage_price_per_tb}/TB")
    
    def calculate_snowflake_credits(self, warehouse_size: str, execution_time_seconds: float) -> float:
        """
        Calculate Snowflake compute credits based on warehouse size and execution time.
        
        Formula: credits = (warehouse_size_multiplier × execution_time_seconds) / 3600
        
        Args:
            warehouse_size: Warehouse size (e.g., 'X-Small', 'Small', 'Medium', etc.)
            execution_time_seconds: Query execution time in seconds
            
        Returns:
            Credits consumed (float)
        """
        multiplier = self.WAREHOUSE_SIZE_MULTIPLIERS.get(warehouse_size, 1)
        
        if execution_time_seconds <= 0:
            return 0.0
        
        credits = (multiplier * execution_time_seconds) / 3600.0
        
        logger.debug(f"Calculated credits: {credits:.6f} (warehouse={warehouse_size}, time={execution_time_seconds:.2f}s)")
        
        return credits
    
    def calculate_snowflake_storage_cost(self, storage_bytes: int, format_name: str, months: float = 1.0) -> float:
        """
        Calculate Snowflake storage costs.
        
        Only applies to native format. Other formats use S3 storage.
        
        Args:
            storage_bytes: Storage size in bytes
            format_name: Table format name
            months: Number of months to calculate cost for
            
        Returns:
            Storage cost in USD
        """
        if format_name != 'native':
            # Native format only uses Snowflake storage
            return 0.0
        
        if storage_bytes <= 0:
            return 0.0
        
        # Convert bytes to TB
        storage_tb = storage_bytes / (1024 ** 4)
        
        # Calculate cost
        cost = storage_tb * self.snowflake_storage_price_per_tb * months
        
        logger.debug(f"Calculated Snowflake storage cost: ${cost:.4f} ({storage_tb:.4f} TB for {months} months)")
        
        return cost
    
    def calculate_s3_storage_cost(self, storage_gb: float, storage_class: str = 'STANDARD', months: float = 1.0) -> float:
        """
        Calculate AWS S3 storage costs.
        
        Args:
            storage_gb: Storage size in GB
            storage_class: S3 storage class (STANDARD, STANDARD_IA, GLACIER, etc.)
            months: Number of months to calculate cost for
            
        Returns:
            Storage cost in USD
        """
        if storage_gb <= 0:
            return 0.0
        
        # Select pricing based on storage class
        if storage_class.upper() in ['STANDARD_IA', 'STANDARD-IA', 'IA']:
            price_per_gb = self.s3_storage_ia_per_gb
        elif storage_class.upper() in ['GLACIER', 'GLACIER_IR', 'GLACIER-DEEP-ARCHIVE']:
            price_per_gb = self.s3_storage_glacier_per_gb
        else:
            price_per_gb = self.s3_storage_standard_per_gb
        
        cost = storage_gb * price_per_gb * months
        
        logger.debug(f"Calculated S3 storage cost: ${cost:.4f} ({storage_gb:.2f} GB {storage_class} for {months} months)")
        
        return cost
    
    def calculate_s3_request_cost(self, get_requests: int = 0, put_requests: int = 0, list_requests: int = 0) -> float:
        """
        Calculate AWS S3 request costs.
        
        Args:
            get_requests: Number of GET requests
            put_requests: Number of PUT requests
            list_requests: Number of LIST requests
            
        Returns:
            Request cost in USD
        """
        get_cost = (get_requests / 1000.0) * self.s3_request_get_per_1k if get_requests > 0 else 0.0
        put_cost = (put_requests / 1000.0) * self.s3_request_put_per_1k if put_requests > 0 else 0.0
        list_cost = (list_requests / 1000.0) * self.s3_request_list_per_1k if list_requests > 0 else 0.0
        
        total_cost = get_cost + put_cost + list_cost
        
        logger.debug(f"Calculated S3 request cost: ${total_cost:.6f} (GET={get_requests}, PUT={put_requests}, LIST={list_requests})")
        
        return total_cost
    
    def calculate_glue_catalog_cost(self, metadata_storage_tb: float = 0.0, 
                                   api_get_calls: int = 0, 
                                   api_create_update_calls: int = 0,
                                   months: float = 1.0) -> float:
        """
        Calculate AWS Glue catalog costs.
        
        Args:
            metadata_storage_tb: Metadata storage in TB
            api_get_calls: Number of GET API calls
            api_create_update_calls: Number of CREATE/UPDATE API calls
            months: Number of months to calculate cost for
            
        Returns:
            Glue catalog cost in USD
        """
        storage_cost = metadata_storage_tb * self.glue_metadata_storage_per_tb * months if metadata_storage_tb > 0 else 0.0
        get_cost = (api_get_calls / 1000.0) * self.glue_api_get_per_1k if api_get_calls > 0 else 0.0
        create_update_cost = (api_create_update_calls / 1000.0) * self.glue_api_create_update_per_1k if api_create_update_calls > 0 else 0.0
        
        total_cost = storage_cost + get_cost + create_update_cost
        
        logger.debug(f"Calculated Glue catalog cost: ${total_cost:.6f} (storage={metadata_storage_tb:.4f}TB, GET={api_get_calls}, CREATE/UPDATE={api_create_update_calls})")
        
        return total_cost
    
    def calculate_data_transfer_cost(self, data_transferred_gb: float, same_region: bool = True) -> float:
        """
        Calculate data transfer costs.
        
        S3 to Snowflake data transfer is free within the same region.
        
        Args:
            data_transferred_gb: Data transferred in GB
            same_region: Whether transfer is within same region
            
        Returns:
            Data transfer cost in USD
        """
        if same_region or data_transferred_gb <= 0:
            return 0.0
        
        cost = data_transferred_gb * self.data_transfer_cross_region_per_gb
        
        logger.debug(f"Calculated data transfer cost: ${cost:.4f} ({data_transferred_gb:.2f} GB cross-region)")
        
        return cost
    
    def calculate_compute_cost(self, credits: float) -> float:
        """
        Calculate Snowflake compute cost from credits.
        
        Args:
            credits: Number of credits consumed
            
        Returns:
            Compute cost in USD
        """
        cost = credits * self.snowflake_credit_price
        
        logger.debug(f"Calculated compute cost: ${cost:.6f} ({credits:.6f} credits)")
        
        return cost
    
    def calculate_total_query_cost(self, query_metrics: Dict[str, Any], format_name: str) -> Dict[str, Any]:
        """
        Calculate total cost for a query execution.
        
        Args:
            query_metrics: Query metrics dictionary containing:
                - warehouse_size: Warehouse size string
                - execution_time_seconds: Execution time
                - bytes_scanned: Bytes scanned (optional)
                - storage_bytes: Storage size in bytes (optional)
                - s3_storage_gb: S3 storage in GB (optional)
                - s3_requests: Dict with get, put, list counts (optional)
                - glue_api_calls: Dict with get, create_update counts (optional)
            format_name: Table format name (native, iceberg_sf, iceberg_glue, external)
            
        Returns:
            Dictionary with cost breakdown:
                - compute_credits: Credits consumed
                - compute_cost_usd: Compute cost
                - storage_cost_usd: Storage cost
                - s3_storage_cost_usd: S3 storage cost
                - s3_request_cost_usd: S3 request cost
                - glue_cost_usd: Glue catalog cost
                - data_transfer_cost_usd: Data transfer cost
                - total_cost_usd: Total cost
        """
        # Calculate compute credits and cost
        warehouse_size = query_metrics.get('warehouse_size', 'X-Small')
        execution_time = query_metrics.get('execution_time_seconds', 0)
        
        if execution_time <= 0:
            execution_time = query_metrics.get('average_time', 0)
        
        credits = self.calculate_snowflake_credits(warehouse_size, execution_time)
        compute_cost = self.calculate_compute_cost(credits)
        
        # Calculate storage costs
        storage_cost = 0.0
        s3_storage_cost = 0.0
        
        if format_name == 'native':
            storage_bytes = query_metrics.get('storage_bytes', 0)
            storage_cost = self.calculate_snowflake_storage_cost(storage_bytes, format_name)
        elif format_name in ['iceberg_glue', 'external']:
            s3_storage_gb = query_metrics.get('s3_storage_gb', 0)
            storage_class = query_metrics.get('s3_storage_class', 'STANDARD')
            s3_storage_cost = self.calculate_s3_storage_cost(s3_storage_gb, storage_class)
        
        # Calculate S3 request costs
        s3_request_cost = 0.0
        s3_requests = query_metrics.get('s3_requests', {})
        if s3_requests:
            s3_request_cost = self.calculate_s3_request_cost(
                get_requests=s3_requests.get('get', 0),
                put_requests=s3_requests.get('put', 0),
                list_requests=s3_requests.get('list', 0)
            )
        
        # Calculate Glue catalog costs (only for iceberg_glue format)
        glue_cost = 0.0
        if format_name == 'iceberg_glue':
            glue_api_calls = query_metrics.get('glue_api_calls', {})
            metadata_storage_tb = query_metrics.get('glue_metadata_storage_tb', 0.0)
            glue_cost = self.calculate_glue_catalog_cost(
                metadata_storage_tb=metadata_storage_tb,
                api_get_calls=glue_api_calls.get('get', 0),
                api_create_update_calls=glue_api_calls.get('create_update', 0)
            )
        
        # Calculate data transfer costs
        data_transfer_cost = 0.0
        data_transferred_gb = query_metrics.get('data_transferred_gb', 0)
        same_region = query_metrics.get('same_region', True)
        if data_transferred_gb > 0:
            data_transfer_cost = self.calculate_data_transfer_cost(data_transferred_gb, same_region)
        
        # Calculate total
        total_cost = compute_cost + storage_cost + s3_storage_cost + s3_request_cost + glue_cost + data_transfer_cost
        
        cost_breakdown = {
            'compute_credits': credits,
            'compute_cost_usd': compute_cost,
            'storage_cost_usd': storage_cost,
            's3_storage_cost_usd': s3_storage_cost,
            's3_request_cost_usd': s3_request_cost,
            'glue_cost_usd': glue_cost,
            'data_transfer_cost_usd': data_transfer_cost,
            'total_cost_usd': total_cost,
            'format': format_name,
            'warehouse_size': warehouse_size,
            'execution_time_seconds': execution_time
        }
        
        logger.debug(f"Total query cost for {format_name}: ${total_cost:.6f}")
        
        return cost_breakdown
    
    def estimate_format_cost(self, format_name: str, 
                           execution_time_seconds: float,
                           warehouse_size: str = 'X-Small',
                           storage_bytes: int = 0,
                           s3_storage_gb: float = 0,
                           months: float = 1.0) -> Dict[str, Any]:
        """
        Estimate cost for a format over a period.
        
        Simplified cost estimation for planning purposes.
        
        Args:
            format_name: Table format name
            execution_time_seconds: Total execution time
            warehouse_size: Warehouse size
            storage_bytes: Storage size in bytes (for native format)
            s3_storage_gb: S3 storage in GB (for external formats)
            months: Time period in months
            
        Returns:
            Estimated cost breakdown
        """
        credits = self.calculate_snowflake_credits(warehouse_size, execution_time_seconds)
        compute_cost = self.calculate_compute_cost(credits)
        
        storage_cost = 0.0
        s3_storage_cost = 0.0
        
        if format_name == 'native':
            storage_cost = self.calculate_snowflake_storage_cost(storage_bytes, format_name, months)
        elif format_name in ['iceberg_glue', 'external', 'iceberg_sf']:
            s3_storage_cost = self.calculate_s3_storage_cost(s3_storage_gb, 'STANDARD', months)
        
        total_cost = compute_cost + storage_cost + s3_storage_cost
        
        return {
            'format': format_name,
            'compute_cost_usd': compute_cost,
            'storage_cost_usd': storage_cost,
            's3_storage_cost_usd': s3_storage_cost,
            'total_cost_usd': total_cost,
            'months': months
        }

