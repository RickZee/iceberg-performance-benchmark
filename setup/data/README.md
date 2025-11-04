# TPC-DS Data Loader

A comprehensive Python application for generating and loading TPC-DS (Transaction Processing Performance Council Decision Support) data into Snowflake across multiple storage formats.

## Overview

This application provides a complete solution for:
- Generating standard TPC-DS data using the official TPC-DS toolkit
- Converting generated data to Parquet format
- Loading data into Snowflake across four different storage formats:
  - **Native Snowflake**: Standard Snowflake tables
  - **Iceberg SF**: Iceberg Snowflake-managed tables
  - **Iceberg Glue**: Iceberg AWS Glue-managed tables
  - **External**: External tables reading from S3

## Features

- **Automated Data Generation**: Uses the official TPC-DS toolkit to generate realistic test data
- **Multi-Format Support**: Loads data into all four Snowflake storage formats
- **Parquet Conversion**: Converts generated .dat files to efficient Parquet format
- **Comprehensive Verification**: Validates data loading and integrity
- **Configurable Scale Factors**: Support for different data sizes (1GB, 10GB, 100GB, etc.)
- **Error Handling**: Robust error handling and logging
- **Progress Tracking**: Detailed progress reporting and status updates

## Prerequisites

### Required Software
- Python 3.8+
- Snowflake Connector for Python
- AWS CLI (for S3-based formats)
- TPC-DS Toolkit (will be installed automatically if not found)

### Required Python Packages
```bash
pip install pandas pyarrow boto3 snowflake-connector-python pyyaml
```

### Snowflake Setup
1. Ensure Snowflake database and schemas are created:
   ```sql
   @setup/schemas/create_tpcds_objects.sql
   ```

2. Configure Snowflake connection in your environment or config files

### AWS Setup (for S3-based formats)
1. Configure AWS credentials
2. Ensure S3 bucket exists and is accessible
3. Set up proper IAM roles for Snowflake access

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install TPC-DS toolkit (optional - will be installed automatically):
   ```bash
   # The application will attempt to install this automatically
   # Or install manually following TPC-DS documentation
   ```

## Usage

### Command Line Interface

The main application provides several commands:

```bash
# Run complete pipeline (generate + load + verify)
python tpcds_data_loader/main.py --action full --scale-factor 1.0

# Generate data only
python tpcds_data_loader/main.py --action generate --scale-factor 1.0

# Load existing data only
python tpcds_data_loader/main.py --action load --data-dir data/tpcds_data_sf1.0

# Verify loaded data
python tpcds_data_loader/main.py --action verify

# Cleanup data
python tpcds_data_loader/main.py --action cleanup
```

### Command Line Options

- `--action`: Action to perform (`generate`, `load`, `verify`, `cleanup`, `full`)
- `--scale-factor`: Scale factor for data generation (default: 1.0)
- `--data-dir`: Directory to store generated data
- `--formats`: Storage formats to process (default: all formats)
- `--verbose`: Enable verbose logging

### Examples

```bash
# Generate 10GB of data and load into all formats
python tpcds_data_loader/main.py --action full --scale-factor 10.0 --verbose

# Load data into specific formats only
python tpcds_data_loader/main.py --action load --formats native iceberg_sf

# Generate data with custom directory
python tpcds_data_loader/main.py --action generate --scale-factor 5.0 --data-dir /path/to/data
```

## Configuration

### Configuration File
Edit `config/tpcds_config.yaml` to customize settings:

```yaml
generation:
  scale_factor: 1.0
  data_directory: "tpcds_data"
  convert_to_parquet: true

snowflake:
  database: "tpcds_performance_test"
  schemas:
    native: "tpcds_native_format"
    iceberg_sf: "tpcds_iceberg_sf_format"
    iceberg_glue: "tpcds_iceberg_glue_format"
    external: "tpcds_external_format"
```

### Environment Variables
Set the following environment variables:

```bash
export SNOWFLAKE_ACCOUNT="your_account"
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_DATABASE="tpcds_performance_test"
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_S3_BUCKET="your_bucket"
```

## Architecture

### Components

1. **TPCDSDataGenerator**: Generates TPC-DS data using the official toolkit
2. **TPCDSDataLoader**: Loads data into Snowflake across different formats
3. **TPCDSApplication**: Main orchestrator for the complete pipeline

### Data Flow

```
TPC-DS Toolkit → .dat files → Parquet files → Snowflake Tables
                     ↓
                S3 Storage (for Iceberg/External formats)
```

### Storage Formats

1. **Native Snowflake**: Direct insertion into standard Snowflake tables
2. **Iceberg SF**: Snowflake-managed Iceberg tables with external volume
3. **Iceberg Glue**: AWS Glue-managed Iceberg tables
4. **External**: External tables reading Parquet files from S3

## TPC-DS Tables

The application loads 24 standard TPC-DS tables:

### Dimension Tables (17)
- call_center, catalog_page, customer, customer_address
- customer_demographics, date_dim, household_demographics
- income_band, item, promotion, reason, ship_mode
- store, time_dim, warehouse, web_page, web_site

### Fact Tables (7)
- catalog_returns, catalog_sales, inventory
- store_returns, store_sales, web_returns, web_sales

## Monitoring and Logging

### Log Files
- Application logs: `results/logs/tpcds_data_loader.log`
- Snowflake query logs: Available in Snowflake web interface

### Progress Tracking
The application provides detailed progress information:
- Data generation progress
- Loading progress per table and format
- Verification results
- Performance metrics

### Error Handling
- Comprehensive error logging
- Graceful failure handling
- Detailed error messages
- Rollback capabilities

## Performance Considerations

### Scale Factors
- **SF=1**: ~1GB of data, suitable for testing
- **SF=10**: ~10GB of data, suitable for development
- **SF=100**: ~100GB of data, suitable for performance testing
- **SF=1000**: ~1TB of data, suitable for large-scale testing

### Optimization Tips
1. Use appropriate warehouse sizes for your scale factor
2. Consider data partitioning for large datasets
3. Monitor Snowflake credits usage
4. Use batch loading for better performance

## Troubleshooting

### Common Issues

1. **TPC-DS Toolkit Not Found**
   - The application will attempt to install it automatically
   - Manual installation may be required on some systems

2. **Snowflake Connection Issues**
   - Verify connection parameters
   - Check network connectivity
   - Ensure proper permissions

3. **S3 Access Issues**
   - Verify AWS credentials
   - Check S3 bucket permissions
   - Ensure IAM roles are properly configured

4. **Memory Issues with Large Datasets**
   - Increase system memory
   - Use smaller batch sizes
   - Process data in chunks

### Debug Mode
Enable verbose logging for detailed debugging:
```bash
python tpcds_data_loader/main.py --action full --verbose
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Additional Scripts

The `tpcds_data_loader` package includes several additional utility scripts:

### **Data Management Scripts**
- **`clear_tpcds_data.py`** - Clears all TPC-DS data from Snowflake tables
  - Truncates/deletes data from native and Iceberg SF tables
  - Deletes S3 objects for external tables
  - Refreshes external table metadata
  - Usage: `python tpcds_data_loader/clear_tpcds_data.py`

### **Query and Reporting Scripts**
- **`query_all_tables.py`** - Queries all tables from all schemas (defaults to TPC-DS schemas) and outputs schema|table|row_count
  - Automatically discovers all schemas and tables
  - Outputs in table format or CSV
  - Can filter by schema or query all schemas
  - Usage: `python setup/data/query_all_tables.py` (defaults to TPC-DS schemas)
  - Usage: `python setup/data/query_all_tables.py --all-schemas` (all schemas)
  - Usage: `python setup/data/query_all_tables.py --output csv --file results.csv` (CSV output)

### **Test and Runner Scripts**
- **`test_tpcds_loader.py`** - Tests the TPC-DS data loader functionality
  - Tests data generation and Parquet conversion
  - Runs without requiring Snowflake connection
  - Usage: `python tpcds_data_loader/test_tpcds_loader.py`

- **`run_tpcds_loader.py`** - Simple runner script for the main application
  - Provides easy command-line access to the main application
  - Usage: `python tpcds_data_loader/run_tpcds_loader.py --help`
