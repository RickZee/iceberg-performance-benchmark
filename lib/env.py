"""
Environment Variable Loader
Utility to load and validate environment variables with sensible defaults
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any


def load_env_var(key: str, default: Optional[str] = None, required: bool = False) -> str:
    """
    Load an environment variable with optional default value
    
    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raise error if not set and no default
    
    Returns:
        Environment variable value
    """
    value = os.getenv(key, default)
    if required and value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    return value


def get_project_root() -> Path:
    """Get the project root directory"""
    # Try to get from environment, otherwise use relative path
    root_str = os.getenv('PROJECT_ROOT')
    if root_str:
        return Path(root_str)
    
    # Default: assume we're in a subdirectory, go up to find project root
    current = Path(__file__).resolve()
    # Go up from lib/env.py to project root
    return current.parent.parent


def get_snowflake_config() -> Dict[str, Any]:
    """Get Snowflake configuration from environment variables"""
    project_root = get_project_root()
    
    # Get private key file path
    key_file = os.getenv('SNOWFLAKE_PRIVATE_KEY_FILE')
    if key_file:
        key_file = Path(key_file).expanduser().resolve()
    else:
        # Try default locations (updated paths)
        default_locations = [
            project_root / 'setup' / 'data' / 'rsa_key.p8',
            project_root / 'benchmark' / 'rsa_key.p8',
            Path.home() / '.ssh' / 'rsa_key.p8',
        ]
        for loc in default_locations:
            if loc.exists():
                key_file = loc
                break
    
    return {
        'user': load_env_var('SNOWFLAKE_USER', required=True),
        'account': load_env_var('SNOWFLAKE_ACCOUNT', required=True),
        'warehouse': load_env_var('SNOWFLAKE_WAREHOUSE', required=True),
        'database': load_env_var('SNOWFLAKE_DATABASE', 'tpcds_performance_test'),
        'role': load_env_var('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        'private_key_file': str(key_file) if key_file else None,
        'private_key_passphrase': load_env_var('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', ''),
    }


def get_snowflake_schemas() -> Dict[str, str]:
    """Get Snowflake schema names from environment variables"""
    return {
        'native': load_env_var('SNOWFLAKE_SCHEMA_NATIVE', 'tpcds_native_format'),
        'iceberg_sf': load_env_var('SNOWFLAKE_SCHEMA_ICEBERG_SF', 'tpcds_iceberg_sf_format'),
        'iceberg_glue': load_env_var('SNOWFLAKE_SCHEMA_ICEBERG_GLUE', 'tpcds_iceberg_glue_format'),
        'external': load_env_var('SNOWFLAKE_SCHEMA_EXTERNAL', 'tpcds_external_format'),
    }


def get_aws_config() -> Dict[str, Any]:
    """Get AWS configuration from environment variables"""
    account_id = load_env_var('AWS_ACCOUNT_ID', required=True)
    bucket = load_env_var('AWS_S3_BUCKET', required=True)
    prefix = load_env_var('AWS_S3_ICEBERG_PREFIX', 'iceberg-performance-test/iceberg-performance-test')
    
    return {
        'region': load_env_var('AWS_REGION', 'us-east-1'),
        'account_id': account_id,
        's3_bucket': bucket,
        's3_prefix': prefix,
        's3_iceberg_glue_path': load_env_var(
            'AWS_S3_ICEBERG_GLUE_PATH',
            f's3://{bucket}/{prefix}/iceberg_glue_format/'
        ),
        'glue_database': load_env_var('AWS_GLUE_DATABASE', 'iceberg_performance_test'),
        'glue_catalog_id': load_env_var('AWS_GLUE_CATALOG_ID', account_id),
        'glue_catalog_name': load_env_var('AWS_GLUE_CATALOG_NAME', 'glue_catalog'),
        'role_name': load_env_var('AWS_ROLE_NAME', 'snowflake-iceberg-role'),
        'role_arn': load_env_var(
            'AWS_ROLE_ARN',
            f'arn:aws:iam::{account_id}:role/snowflake-iceberg-role'
        ),
    }


def get_tpcds_data_dir() -> Path:
    """Get TPC-DS data directory path"""
    project_root = get_project_root()
    data_dir_str = load_env_var('TPCDS_DATA_DIR', 'data/tpcds_data_sf0.1')
    
    if Path(data_dir_str).is_absolute():
        return Path(data_dir_str)
    else:
        return project_root / data_dir_str

