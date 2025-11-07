# Setup Guide

Complete setup guide for the Snowflake Iceberg Performance Testing project. This directory contains all components needed to set up the testing environment across four Snowflake table formats.

## Table of Contents

- [Overview](#overview)
- [Setup Workflow](#setup-workflow)
- [Quick Start](#quick-start)
- [Component Documentation](#component-documentation)
  - [Infrastructure](#infrastructure-infrastructure)
  - [Schemas](#schemas-schemas)
  - [Glue Tables](#glue-tables-glue)
  - [Data Loading](#data-loading-data)
- [Setup Dependencies](#setup-dependencies)
- [Format-Specific Setup](#format-specific-setup)
- [Troubleshooting](#troubleshooting)
- [Verification](#verification)
- [Next Steps](#next-steps)
- [Additional Resources](#additional-resources)

## Overview

The setup process consists of four main components that work together:

1. **Infrastructure** - AWS resources (S3, Glue, IAM) via Terraform
2. **Schemas** - Snowflake database and table creation scripts
3. **Glue Tables** - AWS Glue-managed Iceberg tables (for Iceberg Glue format)
4. **Data Loading** - TPC-DS data generation and loading

## Setup Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Infrastructure Setup (AWS)                               │
│    └─> Terraform: S3, Glue, IAM                             │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Database Schema Creation (Snowflake)                     │
│    └─> SQL Scripts: All 4 table formats                     │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Glue Tables Setup (Iceberg Glue format only)             │
│    └─> Spark/Glue: Create Iceberg tables in Glue catalog    │
└─────────────────────────────────────────────────────────────┘
                        ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Data Loading                                             │
│    └─> Python: Generate & load TPC-DS data                  │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- **Python 3.8+** with dependencies installed
- **Snowflake account** with ACCOUNTADMIN role
- **AWS account** with appropriate permissions
- **Terraform** (for infrastructure setup)
- **Spark** (for Glue table creation, if using Iceberg Glue format)

### Setup Steps

1. **Configure Environment Variables**
   ```bash
   cp env.example .env
   # Edit .env with your credentials
   set -a && source .env && set +a
   ```

2. **Set Up AWS Infrastructure**
   ```bash
   cd infrastructure/
   terraform init && terraform apply
   # Configure cross-account access (see infrastructure/README.md)
   ```

3. **Create Snowflake Schemas**
   ```sql
   -- Execute in Snowflake
   @schemas/create_tpcds_objects.sql
   ```

4. **Set Up Glue Tables** (Iceberg Glue format only)
   ```bash
   cd glue/
   pip install -r requirements.txt
   python scripts/create/create_glue_tables.py
   python scripts/create/load_tpcds_data.py
   python scripts/create/create_snowflake_glue_tables.py
   ```

5. **Load Test Data**
   ```bash
   cd data/
   python main.py
   ```

## Component Documentation

### Infrastructure (`infrastructure/`)

AWS infrastructure provisioning using Terraform:
- S3 bucket for Iceberg data storage
- AWS Glue database and catalog
- IAM roles and policies for Snowflake integration
- Cross-account configuration scripts

**Documentation:** [`infrastructure/README.md`](infrastructure/README.md)

**Key Files:**
- `main.tf` - Main Terraform configuration
- `s3.tf` - S3 bucket setup
- `glue.tf` - Glue catalog setup
- `iam.tf` - IAM roles and policies
- `get_snowflake_iam_details.py` - Retrieve Snowflake IAM details
- `update_iam_trust_policy.py` - Update IAM trust policy

### Schemas (`schemas/`)

SQL scripts for creating Snowflake database schemas and tables:
- Master setup script for all formats
- Individual format scripts (Native, Iceberg SF, Iceberg Glue, External)
- Cleanup scripts

**Key Files:**
- `create_tpcds_objects.sql` - Master script (creates all formats)
- `create_native.sql` - Native Snowflake tables
- `create_iceberg_sf.sql` - Iceberg Snowflake-managed tables
- `create_iceberg_glue.sql` - Iceberg Glue-managed tables (references)
- `create_external.sql` - External tables
- `cleanup_tpcds_objects.sql` - Cleanup script

**Usage:**
```sql
-- Execute in Snowflake
@setup/schemas/create_tpcds_objects.sql
```

### Glue Tables (`glue/`)

AWS Glue-managed Iceberg table setup for the Iceberg Glue format. This setup follows the correct Iceberg workflow:

1. **Create Iceberg tables** in Glue catalog (with proper metadata)
2. **Snowflake** references the existing Glue tables
3. **Data** is loaded with proper Iceberg metadata tracking

#### Directory Structure

```
setup/glue/
├── config/
│   ├── spark_config.yaml              # Spark configuration
│   ├── glue_catalog_config.yaml       # Glue catalog settings
│   └── tpcds_table_schemas.yaml       # TPC-DS table schemas
├── scripts/
│   ├── create/                        # Table creation scripts
│   │   ├── create_glue_tables.py      # Main script to create Glue tables (Spark approach)
│   │   ├── load_tpcds_data.py         # Load TPC-DS data into Glue tables
│   │   └── create_snowflake_glue_tables.py # Create Snowflake references to Glue tables
│   ├── verify/                        # Verification scripts
│   │   ├── verify_glue_tables.py      # Verify tables in Glue and Snowflake
│   │   ├── verify_glue_table_counts.py # Verify row counts
│   │   └── verify_snowflake_glue_tables.py # Verify in Snowflake
│   ├── maintenance/                   # Maintenance and cleanup scripts
│   │   ├── cleanup_glue_tables.py     # Cleanup script
│   │   └── clear_glue_table_data.py   # Clear table data
│   └── troubleshooting/               # One-off troubleshooting scripts (for reference)
├── logs/                              # Log files
└── requirements.txt                   # Python dependencies
```

#### Prerequisites

1. **AWS Credentials** configured
2. **Spark** with Iceberg extensions (for local Spark approach) OR **AWS Glue 4.0+** (for ETL approach)
3. **Snowflake** connection configured
4. **TPC-DS data** available in Parquet format (for data loading)
5. **AWS Infrastructure** deployed (see Infrastructure section)
6. **Required JAR files** (see Downloading JAR Files below)

#### Downloading JAR Files

Required JAR files must be downloaded before creating Glue tables. Download to `setup/glue/libs/jars/`:

**Required JARs:**
- `iceberg-aws-1.4.2.jar`
- `iceberg-spark-runtime-3.3_2.12-1.4.2.jar`
- `aws-sdk-bundle-2.20.66.jar` (for Glue ETL approach only)

**Quick Download:**

```bash
mkdir -p setup/glue/libs/jars
cd setup/glue/libs/jars

# Download Iceberg JARs
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar

# Download AWS SDK Bundle (for Glue ETL approach)
curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.66/bundle-2.20.66.jar
mv bundle-2.20.66.jar aws-sdk-bundle-2.20.66.jar
```

#### Two Approaches

**Approach 1: Local Spark (Recommended for Development)**
- Uses local Spark with Iceberg extensions
- Full control over Spark configuration
- Faster iteration for development
- No AWS Glue job costs

**Approach 2: AWS Glue ETL (Recommended for Production)**
- Uses AWS Glue ETL jobs
- No local Spark setup required
- Managed environment
- Requires AWS Glue 4.0+

#### Detailed Workflow

**Step 1: Create Glue Tables**

Using Local Spark:
```bash
cd glue/
pip install -r requirements.txt
python scripts/create/create_glue_tables.py
```

Using AWS Glue ETL:
```bash
cd glue/scripts
python generate_glue_etl_job.py
./deploy_and_run_glue_job.sh
```

**Step 2: Load TPC-DS Data**
```bash
python scripts/create/load_tpcds_data.py
```

**Step 3: Create Snowflake References**
```bash
python scripts/create/create_snowflake_glue_tables.py
```

**Step 4: Verify Setup**
```bash
python scripts/verify/verify_glue_tables.py
```

#### Configuration

Edit configuration files in `config/`:
- `spark_config.yaml` - Spark configuration (for local Spark approach)
- `glue_catalog_config.yaml` - Glue catalog settings
- `tpcds_table_schemas.yaml` - TPC-DS table schemas

#### Troubleshooting Scripts

The `scripts/troubleshooting/` directory contains one-off scripts used during initial setup and debugging. They're kept for reference but are **not part of the standard workflow**:

- `drop_failing_tables.py` - Drop specific failing tables (Spark SQL)
- `drop_failing_tables_glue.py` - Drop tables via Glue API
- `fix_and_integrate_glue_tables.sh` - Complete fix process orchestration
- `recreate_all_glue_tables.py` - Recreate all tables from Parquet schemas
- `fix_glue_schemas.py` - Fix table schemas by dropping and recreating

For standard operations, use scripts in `scripts/create/`, `scripts/verify/`, and `scripts/maintenance/`.

### Data Loading (`data/`)

Comprehensive Python application for generating and loading TPC-DS data into Snowflake across multiple storage formats.

#### Features

- **Automated Data Generation**: Uses the official TPC-DS toolkit to generate realistic test data
- **Multi-Format Support**: Loads data into all four Snowflake storage formats
- **Parquet Conversion**: Converts generated .dat files to efficient Parquet format
- **Comprehensive Verification**: Validates data loading and integrity
- **Configurable Scale Factors**: Support for different data sizes (1GB, 10GB, 100GB, etc.)
- **Error Handling**: Robust error handling and logging
- **Progress Tracking**: Detailed progress reporting and status updates

#### Prerequisites

- **Python 3.8+** with dependencies: `pip install pandas pyarrow boto3 snowflake-connector-python pyyaml`
- **Snowflake** connection configured
- **AWS CLI** (for S3-based formats)
- **TPC-DS Toolkit** (will be installed automatically if not found)

#### Usage

**Command Line Interface:**

```bash
# Run complete pipeline (generate + load + verify)
python setup/data/main.py --action full --scale-factor 1.0

# Generate data only
python setup/data/main.py --action generate --scale-factor 1.0

# Load existing data only
python setup/data/main.py --action load --data-dir data/tpcds_data_sf1.0 --formats native iceberg_sf external

# Verify loaded data
python setup/data/main.py --action verify

# Cleanup data
python setup/data/main.py --action cleanup
```

**Command Line Options:**
- `--action`: Action to perform (`generate`, `load`, `verify`, `cleanup`, `full`)
- `--scale-factor`: Scale factor for data generation (default: 1.0)
- `--data-dir`: Directory to store generated data
- `--formats`: Storage formats to process (default: all formats)
- `--verbose`: Enable verbose logging

**Scale Factors:**
- **SF=0.01**: ~10MB of data, quick testing
- **SF=0.1**: ~100MB of data, development
- **SF=1.0**: ~1GB of data, standard testing
- **SF=10.0**: ~10GB of data, performance testing
- **SF=100.0**: ~100GB of data, large-scale testing

#### Data Flow

```
TPC-DS Toolkit → .dat files → Parquet files → Snowflake Tables
                     ↓
                S3 Storage (for Iceberg/External formats)
```

#### Storage Formats

1. **Native Snowflake**: Direct insertion into standard Snowflake tables
2. **Iceberg SF**: Snowflake-managed Iceberg tables with external volume
3. **Iceberg Glue**: AWS Glue-managed Iceberg tables (uses different workflow - see Glue Tables section)
4. **External**: External tables reading Parquet files from S3

#### TPC-DS Tables

The application loads 24 standard TPC-DS tables:

**Dimension Tables (17):** call_center, catalog_page, customer, customer_address, customer_demographics, date_dim, household_demographics, income_band, item, promotion, reason, ship_mode, store, time_dim, warehouse, web_page, web_site

**Fact Tables (7):** catalog_returns, catalog_sales, inventory, store_returns, store_sales, web_returns, web_sales

#### Additional Scripts

**Data Management:**
- `clear_tpcds_data.py` - Clears all TPC-DS data from Snowflake tables

**Query and Reporting:**
- `query_all_tables.py` - Queries all tables and outputs schema|table|row_count
  ```bash
  python setup/data/query_all_tables.py
  python setup/data/query_all_tables.py --all-schemas
  python setup/data/query_all_tables.py --output csv --file results.csv
  ```

**Test and Runner:**
- `test_tpcds_loader.py` - Tests the TPC-DS data loader functionality
- `run_tpcds_loader.py` - Simple runner script for the main application

#### Monitoring and Logging

- Application logs: `results/logs/tpcds_data_loader.log`
- Progress tracking: Data generation, loading progress per table, verification results
- Error handling: Comprehensive error logging with detailed messages

#### Performance Considerations

- Use appropriate warehouse sizes for your scale factor
- Consider data partitioning for large datasets
- Monitor Snowflake credits usage
- Use batch loading for better performance

**Note:** Glue-managed Iceberg tables use a different workflow (see Glue Tables section above).

## Setup Dependencies

```
Infrastructure (AWS)
    ↓
Schemas (Snowflake)
    ↓
Glue Tables (Iceberg Glue format only)
    ↓
Data Loading (All formats)
```

**Important Notes:**
- Infrastructure must be set up before schemas (for S3/Glue references)
- Schemas must be created before data loading
- Glue tables must be created before loading data into Iceberg Glue format
- Cross-account IAM configuration required after infrastructure setup

## Format-Specific Setup

### Native Snowflake Tables
1. Create schemas (`schemas/create_native.sql`)
2. Load data (`data/main.py --formats native`)

### Iceberg Snowflake-Managed
1. Create schemas (`schemas/create_iceberg_sf.sql`)
2. Load data (`data/main.py --formats iceberg_sf`)

### Iceberg AWS Glue-Managed
1. Set up infrastructure (Terraform)
2. Create schemas (`schemas/create_iceberg_glue.sql`)
3. Create Glue tables (`glue/scripts/create/`)
4. Load data into Glue tables (`glue/scripts/create/load_tpcds_data.py`)
5. Create Snowflake references (`glue/scripts/create/create_snowflake_glue_tables.py`)

### External Tables
1. Set up infrastructure (Terraform - S3 bucket)
2. Create schemas (`schemas/create_external.sql`)
3. Load data (`data/main.py --formats external`)

## Troubleshooting

### Common Issues

#### Infrastructure
- **Cross-account IAM configuration not completed**: After creating Snowflake integrations, run `get_snowflake_iam_details.py` and `update_iam_trust_policy.py`
- **S3 bucket permissions incorrect**: Verify IAM role has S3 read/write permissions
- **Glue database not accessible**: Check Glue database exists and IAM role has Glue catalog permissions

#### Schemas
- **Snowflake permissions insufficient**: Need ACCOUNTADMIN role for external volume and catalog integration creation
- **External volume/catalog integration not created**: Execute `schemas/create_tpcds_objects.sql` in Snowflake
- **Schema names don't match environment variables**: Verify environment variables match SQL script values

#### Glue Tables

**Local Spark Approach:**
- **AWS Permissions**: Verify AWS credentials and IAM permissions
- **Spark Configuration**: Check `glue/config/spark_config.yaml` and Iceberg extensions
- **Glue Catalog Access**: Ensure Glue database exists and is accessible
- **Dependencies**: Verify all required JARs are available in `glue/libs/jars/`

**AWS Glue ETL Approach:**
- **Job Fails with "Table Already Exists"**: Script handles existing tables gracefully
- **Job Fails with S3 Access Errors**: Check Glue service role has S3 permissions, verify bucket policy
- **Job Fails with Glue Catalog Errors**: Verify Glue database exists, check service role permissions
- **Tables Created but No Metadata Files**: Verify Glue version is 4.0+, check Iceberg extensions

**General Glue Issues:**
- **Snowflake Connectivity**: Check Snowflake connection and permissions
- **Cross-Account Access**: Verify IAM trust policy is configured correctly
- **Path Mismatches**: Ensure external volume path matches S3 metadata location

#### Data Loading
- **TPC-DS Toolkit Not Found**: Application will attempt to install automatically; manual installation may be required on some systems
- **Snowflake Connection Issues**: Verify connection parameters, check network connectivity, ensure proper permissions
- **S3 Access Issues**: Verify AWS credentials, check S3 bucket permissions, ensure IAM roles are properly configured
- **Memory Issues with Large Datasets**: Increase system memory, use smaller batch sizes, process data in chunks

### Debug Mode

Enable verbose logging for detailed debugging:
```bash
# Data loading
python setup/data/main.py --action full --verbose

# Glue table creation
python setup/glue/scripts/create/create_glue_tables.py --verbose
```

### Documentation Links

- **Infrastructure Issues:** [`infrastructure/README.md`](infrastructure/README.md)
- **General Troubleshooting:** [`../docs/glue-integration-journey.md`](../docs/glue-integration-journey.md)

## Verification

After setup, verify each component:

1. **Infrastructure:**
   ```bash
   cd infrastructure/
   terraform show  # Verify resources created
   ```

2. **Schemas:**
   ```sql
   -- In Snowflake
   SHOW SCHEMAS IN DATABASE tpcds_performance_test;
   SHOW TABLES IN SCHEMA tpcds_native_format;
   ```

3. **Glue Tables:**
   ```bash
   cd glue/
   python scripts/verify/verify_glue_tables.py
   ```

4. **Data:**
   ```sql
   -- In Snowflake
   SELECT COUNT(*) FROM tpcds_native_format.call_center;
   ```

## Next Steps

After completing setup:
1. Run performance tests: `python benchmark/src/main.py`
2. Review results in `results/reports/`
3. See [`../benchmark/README.md`](../benchmark/README.md) for testing documentation

## Additional Resources

- **Main Project README:** [`../README.md`](../README.md)
- **Benchmark Testing:** [`../benchmark/README.md`](../benchmark/README.md)
- **Cost Optimization Guide:** [`../docs/cost-optimization-guide.md`](../docs/cost-optimization-guide.md)

