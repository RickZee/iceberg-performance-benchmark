#!/usr/bin/env python3
"""
Create Glue-managed Iceberg tables for TPC-DS data
This script uses Spark with Iceberg extensions to create tables in AWS Glue catalog
"""

import os
import sys
import yaml
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, DecimalType

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

def setup_logging():
    """Setup logging configuration"""
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / "create_glue_tables.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def load_config():
    """Load configuration files with environment variable substitution"""
    import os
    import re
    
    config_dir = Path(__file__).parent.parent / "config"
    
    def substitute_env_vars(content: str) -> str:
        """Substitute ${VAR_NAME} with environment variable values"""
        def replace_var(match):
            var_name = match.group(1)
            return os.getenv(var_name, match.group(0))  # Return original if not found
    
        return re.sub(r'\$\{(\w+)\}', replace_var, content)
    
    # Load and substitute environment variables
    with open(config_dir / "spark_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        spark_config = yaml.safe_load(content)
    
    with open(config_dir / "glue_catalog_config.yaml", 'r') as f:
        content = substitute_env_vars(f.read())
        glue_config = yaml.safe_load(content)
    
    with open(config_dir / "tpcds_table_schemas.yaml", 'r') as f:
        table_schemas = yaml.safe_load(f)
    
    return spark_config, glue_config, table_schemas

def create_spark_session(spark_config):
    """Create Spark session with Iceberg extensions"""
    logger = logging.getLogger(__name__)
    
    # Build Spark configuration
    spark_conf = {
        "spark.app.name": spark_config['spark']['app_name'],
        "spark.master": spark_config['spark']['master']
    }
    
    # Add all Spark configs
    spark_conf.update(spark_config['spark']['config'])
    
    # Add JAR files
    jar_files = []
    for jar_path in spark_config['jars']:
        full_jar_path = project_root / jar_path
        if full_jar_path.exists():
            jar_files.append(str(full_jar_path))
        else:
            logger.warning(f"JAR file not found: {full_jar_path}")
    
    if jar_files:
        spark_conf["spark.jars"] = ",".join(jar_files)
    
    logger.info(f"Creating Spark session with config: {spark_conf}")
    
    # Create Spark session
    spark = SparkSession.builder
    
    for key, value in spark_conf.items():
        spark = spark.config(key, value)
    
    return spark.getOrCreate()

def format_table_name(catalog_name, database_name, table_name):
    """Format table name with proper quoting for hyphenated identifiers
    
    Based on simple-glue-setup approach: hyphenated names must be quoted with backticks
    """
    return f"{catalog_name}.`{database_name}`.`{table_name}`"

def create_table_schema(table_name, table_def):
    """Create Spark DataFrame schema from table definition"""
    fields = []
    
    for col_def in table_def['columns']:
        col_name = col_def['name']
        col_type = col_def['type']
        nullable = col_def.get('nullable', True)
        
        # Map type strings to Spark types
        if col_type == "string":
            spark_type = StringType()
        elif col_type == "int":
            spark_type = IntegerType()
        elif col_type == "double":
            spark_type = DoubleType()
        elif col_type == "date":
            spark_type = DateType()
        elif col_type.startswith("decimal"):
            # Parse decimal(7,2) format
            precision, scale = 7, 2
            if "(" in col_type:
                params = col_type.split("(")[1].split(")")[0].split(",")
                precision = int(params[0])
                scale = int(params[1]) if len(params) > 1 else 2
            spark_type = DecimalType(precision, scale)
        else:
            spark_type = StringType()  # Default to string
        
        fields.append(StructField(col_name, spark_type, nullable))
    
    return StructType(fields)

def create_iceberg_table(spark, table_name, table_def, glue_config, replace_if_exists=False):
    """Create an Iceberg table in Glue catalog"""
    logger = logging.getLogger(__name__)
    
    try:
        # Create empty DataFrame with the table schema
        schema = create_table_schema(table_name, table_def)
        empty_df = spark.createDataFrame([], schema)
        
        # Get table properties
        table_properties = glue_config['glue']['table_properties']
        
        # Create table using Iceberg
        # Use the Spark catalog name from config (glue_catalog) instead of AWS catalog name
        # The AWS catalog name may contain hyphens which are invalid in SQL identifiers
        # IMPORTANT: Use backticks for hyphenated database/table names (from simple-glue-setup)
        catalog_name = "glue_catalog"  # This matches the Spark config catalog name
        database_name = glue_config['glue']['database_name']
        full_table_name = format_table_name(catalog_name, database_name, table_name)
        
        logger.info(f"Creating table: {full_table_name}")
        
        # Drop table if it exists and replace_if_exists is True
        if replace_if_exists:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                logger.info(f"Dropped existing table: {full_table_name}")
            except Exception as drop_err:
                logger.warning(f"Could not drop table (may not exist): {drop_err}")
        
        # Write empty DataFrame to create table structure
        writer = empty_df.writeTo(full_table_name)
        
        # Add table properties
        for key, value in table_properties.items():
            writer = writer.tableProperty(key, value)
        
        # Create the table
        try:
            writer.create()
        except Exception as create_err:
            create_msg = str(create_err)
            if "already exists" in create_msg.lower() or "TABLE_OR_VIEW_ALREADY_EXISTS" in create_msg:
                if replace_if_exists:
                    # Drop and retry
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    writer.create()
                else:
                    logger.warning(f"Table already exists: {full_table_name}. Use replace_if_exists=True to recreate.")
                    return False
            else:
                raise
        
        # Get and log metadata location (important for Snowflake integration)
        try:
            metadata_result = spark.sql(
                f"SHOW TBLPROPERTIES {full_table_name} ('metadata_location')"
            ).collect()
            if metadata_result:
                metadata_location = metadata_result[0][0]
                logger.info(f"Metadata location: {metadata_location}")
        except Exception as e:
            logger.warning(f"Could not retrieve metadata location: {e}")
        
        logger.info(f"✅ Successfully created table: {full_table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create table {table_name}: {e}")
        return False

def main():
    """Main function"""
    logger = setup_logging()
    logger.info("🚀 Starting Glue Iceberg table creation...")
    
    try:
        # Load configurations
        spark_config, glue_config, table_schemas = load_config()
        logger.info("✅ Configuration loaded successfully")
        
        # Create Spark session
        spark = create_spark_session(spark_config)
        logger.info("✅ Spark session created successfully")
        
        # Create tables
        success_count = 0
        total_tables = len(table_schemas['tables'])
        
        logger.info(f"Creating {total_tables} tables in Glue catalog...")
        
        # Check if we should replace existing tables
        import sys
        replace_existing = '--replace' in sys.argv or '-r' in sys.argv
        
        if replace_existing:
            logger.info("⚠️  --replace flag detected: Will drop and recreate existing tables")
        
        for table_name, table_def in table_schemas['tables'].items():
            logger.info(f"Creating table: {table_name}")
            
            if create_iceberg_table(spark, table_name, table_def, glue_config, replace_if_exists=replace_existing):
                success_count += 1
            else:
                logger.error(f"Failed to create table: {table_name}")
        
        # Summary
        logger.info(f"✅ Table creation completed!")
        logger.info(f"Successfully created: {success_count}/{total_tables} tables")
        
        if success_count == total_tables:
            logger.info("🎉 All tables created successfully!")
        else:
            logger.warning(f"⚠️  {total_tables - success_count} tables failed to create")
        
        # Show created tables
        logger.info("📋 Created tables:")
        catalog_name = "glue_catalog"  # Use Spark catalog name
        database_name = glue_config['glue']['database_name']
        
        try:
            # Use backticks for hyphenated database name
            tables = spark.sql(f"SHOW TABLES IN {catalog_name}.`{database_name}`")
            tables.show()
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
        
        spark.stop()
        return 0 if success_count == total_tables else 1
        
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
