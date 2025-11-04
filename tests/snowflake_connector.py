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

