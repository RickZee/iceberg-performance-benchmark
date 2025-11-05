# Snowflake Iceberg Performance Testing Suite

A comprehensive performance testing framework for comparing 4 different Snowflake table formats using the TPC-DS benchmark:

1. **Native Snowflake Tables** - Standard Snowflake managed tables
2. **Iceberg Snowflake-Managed** - Iceberg format with Snowflake catalog
3. **Iceberg AWS Glue-Managed** - Iceberg format with external AWS Glue catalog
4. **External Iceberg Tables** - External tables pointing to parquet data

## 🚀 Features

- **TPC-DS Benchmark Testing**: 99 standard TPC-DS queries across all formats
- **Automated Test Execution**: Python framework with comprehensive metrics collection
- **Multiple Table Formats**: Native, Iceberg (Snowflake), Iceberg (Glue), External
- **Performance Analytics**: Detailed metrics, statistical analysis, and recommendations
- **Scalable Testing**: Support for multiple TPC-DS scale factors (0.01, 0.1, 1.0, etc.)
- **Automated Reporting**: HTML, JSON, CSV, and PDF report generation
- **Complete Setup Workflow**: Automated infrastructure and data loading

## 📁 Project Structure

```text
iceberg-performance-benchmark/
├── setup/                    # All setup functionality
│   ├── infrastructure/       # AWS Terraform infrastructure
│   │   ├── main.tf           # Main Terraform configuration
│   │   ├── s3.tf             # S3 bucket configuration
│   │   ├── glue.tf           # Glue catalog configuration
│   │   ├── iam.tf            # IAM roles and policies
│   │   ├── variables.tf      # Terraform variables
│   │   ├── outputs.tf        # Terraform outputs
│   │   ├── terraform.tfvars.example  # Example variables file
│   │   ├── deploy_glue_iceberg.sh    # Deployment script
│   │   └── README.md         # Infrastructure documentation
│   ├── schemas/              # SQL schema creation scripts
│   │   ├── create_native.sql
│   │   ├── create_iceberg_sf.sql
│   │   ├── create_iceberg_glue.sql
│   │   ├── create_external.sql
│   │   └── create_tpcds_objects.sql  # Master setup script
│   ├── glue/                 # AWS Glue table setup
│   │   ├── scripts/          # Setup scripts
│   │   │   ├── create_glue_tables.py
│   │   │   ├── load_tpcds_data.py
│   │   │   ├── create_snowflake_glue_tables.py
│   │   │   ├── verify_glue_tables.py
│   │   │   └── cleanup_glue_tables.py
│   │   ├── config/           # Glue configuration files
│   │   │   ├── spark_config.yaml
│   │   │   ├── glue_catalog_config.yaml
│   │   │   └── tpcds_table_schemas.yaml
│   │   ├── libs/             # JAR files and dependencies
│   │   │   └── jars/         # Iceberg/Spark runtime JARs
│   │   ├── logs/             # Glue setup logs
│   │   ├── requirements.txt  # Glue-specific dependencies
│   │   └── README.md         # Glue setup documentation
│   └── data/                 # Data generation and loading
│       ├── main.py           # Main data loading application
│       ├── tpcds_data_loader.py  # Data loading logic
│       ├── tpcds_generator.py    # TPC-DS data generation
│       ├── mock_tpcds_generator.py  # Mock generator for testing
│       ├── run_tpcds_loader.py    # Runner script
│       ├── test_tpcds_loader.py   # Test utilities
│       ├── clear_tpcds_data.py    # Data cleanup utility
│       ├── query_all_tables.py  # Query all tables utility (schema|table|row_count)
│       └── README.md         # Data loader documentation
├── benchmark/                # Performance testing framework
│   ├── src/                  # Source code
│   │   ├── main.py           # Main application entry point
│   │   ├── query_engine.py   # Query execution engine
│   │   ├── metrics_collector.py # Performance metrics collection
│   │   ├── analytics_engine.py  # Advanced analytics
│   │   └── report_generator.py  # Report generation
│   ├── queries/              # TPC-DS query files (99 queries per format)
│   │   ├── native/           # Queries for native format
│   │   ├── iceberg_sf/       # Queries for Iceberg SF format
│   │   ├── iceberg_glue/     # Queries for Iceberg Glue format
│   │   └── external/         # Queries for external format
│   ├── config/               # Test configuration files
│   │   ├── perf_test_config.yaml
│   │   ├── test_scenarios.yaml
│   │   ├── reporting_config.yaml
│   │   └── smoke_test*.yaml
│   ├── logs/                 # Benchmark execution logs
│   ├── fix_limit_clauses.py  # Utility script for query fixes
│   ├── quick_test_fix.py     # Quick test utility
│   └── README.md             # Performance testing documentation
├── config/                   # Global shared configuration files
│   ├── snowflake_config.yaml
│   ├── aws_config.yaml
│   └── tpcds_config.yaml
├── lib/                      # Shared utility modules
│   ├── env.py                # Environment variable loader
│   └── output.py             # Output manager
├── data/                     # TPC-DS generated data files (gitignored)
├── results/                  # Test results and reports (gitignored)
├── logs/                     # Log files (gitignored)
├── load_env_vars.sh          # Environment variable loader script
├── requirements.txt
├── env.example
└── README.md                 # This file
```

## 🛠️ Prerequisites

### Software Requirements

- Python 3.8+
- Snowflake account with appropriate permissions
- AWS account (for Glue catalog and S3)
- Git
- Terraform (for AWS infrastructure)
- Spark (for Glue table creation, if using setup/glue)

### Python Dependencies

```bash
pip install -r requirements.txt
```

Key dependencies:

- `snowflake-connector-python`
- `pandas`
- `numpy`
- `matplotlib`
- `seaborn`
- `pyyaml`
- `pyarrow` (for Parquet file handling)
- `boto3` (for AWS integration)
- `scipy` (for statistical analysis)

### Snowflake Requirements

- Warehouse with appropriate size (X-Small to X-Large)
- Database and schema creation permissions
- External volume creation permissions (for Iceberg)
- Storage integration permissions (for S3)
- ACCOUNTADMIN role (for catalog integrations)

### AWS Requirements

- S3 bucket for Iceberg data storage
- AWS Glue catalog access
- IAM role with appropriate permissions
- Storage integration in Snowflake

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd iceberg-performance-benchmark
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables

This project uses environment variables for configuration to keep sensitive information and deployment-specific settings out of the codebase.

#### Quick Setup

1. **Copy the example file:**

   ```bash
   cp env.example .env
   ```

2. **Edit `.env` with your values:**

   ```bash
   # Edit with your actual credentials and configuration
   nano .env
   # or
   vim .env
   ```

3. **Load environment variables:**

   ```bash
   # Method 1: Source .env file (recommended)
   set -a  # automatically export all variables
   source .env
   set +a
   
   # Method 2: Export manually
   export $(cat .env | xargs)
   
   # Method 3: Use python-dotenv in your scripts
   # pip install python-dotenv
   # Then add to scripts: from dotenv import load_dotenv; load_dotenv()
   ```

#### Environment Variables Reference

**Snowflake Configuration:**

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `SNOWFLAKE_USER` | Snowflake username | `your_username` | ✅ Yes |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `xy12345.us-east-1` | ✅ Yes |
| `SNOWFLAKE_WAREHOUSE` | Warehouse name | `TPCDS_TEST_WH` | ✅ Yes |
| `SNOWFLAKE_DATABASE` | Database name | `tpcds_performance_test` | No (default: `tpcds_performance_test`) |
| `SNOWFLAKE_ROLE` | Role name | `ACCOUNTADMIN` | No (default: `ACCOUNTADMIN`) |
| `SNOWFLAKE_PRIVATE_KEY_FILE` | Path to RSA private key file | `/path/to/rsa_key.p8` | No* |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | Passphrase for private key | (empty if no passphrase) | No |

**Snowflake Schema Names (all optional with defaults):**

| Variable | Default | Description |
|----------|---------|-------------|
| `SNOWFLAKE_SCHEMA_NATIVE` | `tpcds_native_format` | Native Snowflake table schema |
| `SNOWFLAKE_SCHEMA_ICEBERG_SF` | `tpcds_iceberg_sf_format` | Iceberg Snowflake-managed schema |
| `SNOWFLAKE_SCHEMA_ICEBERG_GLUE` | `tpcds_iceberg_glue_format` | Iceberg Glue-managed schema |
| `SNOWFLAKE_SCHEMA_EXTERNAL` | `tpcds_external_format` | External table schema |

**AWS Configuration:**

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `AWS_REGION` | AWS region | `us-east-1` | No (default: `us-east-1`) |
| `AWS_ACCOUNT_ID` | AWS account ID | `123456789012` | ✅ Yes |
| `AWS_S3_BUCKET` | S3 bucket name | `my-iceberg-bucket` | ✅ Yes |
| `AWS_S3_ICEBERG_PREFIX` | S3 prefix for Iceberg data | `iceberg-data/prefix` | No |
| `AWS_S3_ICEBERG_GLUE_PATH` | Full S3 path for Glue Iceberg tables | `s3://bucket/prefix/iceberg_glue_format/` | No |
| `AWS_GLUE_DATABASE` | Glue database name | `iceberg_performance_test` | No |
| `AWS_GLUE_CATALOG_ID` | Glue catalog ID | `123456789012` | No |
| `AWS_GLUE_CATALOG_NAME` | Glue catalog name | `glue_catalog` | No |
| `AWS_ROLE_NAME` | IAM role name | `snowflake-iceberg-role` | No |
| `AWS_ROLE_ARN` | Full IAM role ARN | `arn:aws:iam::123456789012:role/snowflake-iceberg-role` | No |

**Project Paths (all optional with defaults):**

| Variable | Description | Example |
|----------|-------------|---------|
| `PROJECT_ROOT` | Absolute path to project root | `/path/to/iceberg-performance-benchmark` |
| `TPCDS_DATA_DIR` | Path to TPC-DS data directory | `data/tpcds_data_sf0.1` |

*Private key file is required for key pair authentication. The script will search in:

1. Path specified in `SNOWFLAKE_PRIVATE_KEY_FILE`
2. `project_root/setup/data/rsa_key.p8`
3. `project_root/benchmark/rsa_key.p8`
4. `~/.ssh/rsa_key.p8`

#### Using Environment Variables in Scripts

Scripts use the `lib.env` module to load configuration:

```python
from lib.env import get_snowflake_config, get_aws_config, get_snowflake_schemas

# Load Snowflake configuration
snowflake_config = get_snowflake_config()
conn = snowflake.connector.connect(**snowflake_config)

# Load AWS configuration
aws_config = get_aws_config()
s3_path = aws_config['s3_iceberg_glue_path']

# Load schema names
schemas = get_snowflake_schemas()
schema_name = schemas['iceberg_glue']
```

**Note:** Configuration files in `setup/glue/config/` also support environment variable substitution using `${VAR_NAME}` syntax.

### 4. Authentication Setup

#### Option A: Password Authentication

Set the `SNOWFLAKE_PASSWORD` environment variable as shown above.

#### Option B: Key Pair Authentication (Recommended)

Key Pair Authentication uses a private/public key pair instead of passwords:

- ✅ Bypasses MFA requirements
- ✅ More secure than passwords
- ✅ No need for TOTP codes

**Setup Steps:**

1. **Generate Key Pair**

   ```bash
   # Generate private key
   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

   # Generate public key
   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
   ```

2. **Configure Snowflake**

   - Log into Snowflake Web UI
   - Go to your user profile (click your username → Profile)
   - Navigate to "Key Pairs" tab
   - Click "Add Key Pair"
   - Upload the public key (`rsa_key.pub`)
   - Copy the public key fingerprint

3. **Update Environment Variables**

```bash
export SNOWFLAKE_PRIVATE_KEY_FILE="/full/path/to/rsa_key.p8"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=""  # Leave empty if no passphrase
```

**Security Notes:**

- Keep your private keys secure and never commit them to git (already in `.gitignore`)
- Never commit `.env` files - they contain sensitive credentials
- Use `env.example` as a template - this file can be safely committed
- Rotate credentials regularly
- Use least privilege principles

### 5. AWS Infrastructure Setup

#### Option A: Automated Setup (Recommended)

```bash
cd setup/infrastructure/
terraform init
terraform plan
terraform apply
```

This will create:

- S3 bucket for data storage
- AWS Glue database
- IAM roles and policies
- All necessary AWS resources

See [`setup/infrastructure/README.md`](setup/infrastructure/README.md) for detailed AWS infrastructure setup instructions.

#### ⚠️ Important: Cross-Account Configuration

**Snowflake runs on its own AWS backend accounts**, requiring cross-account access configuration:

1. **After creating Snowflake integrations** (external volume and catalog integration), retrieve IAM details:
   
   **Automated (recommended):**
   ```bash
   python3 setup/infrastructure/get_snowflake_iam_details.py
   ```
   
   **Manual SQL:**
   ```sql
   DESC EXTERNAL VOLUME iceberg_glue_volume;
   DESC CATALOG INTEGRATION aws_glue_catalog;
   ```

2. **Update the IAM role trust policy** with Snowflake's IAM user ARN and external ID:
   
   **Automated (recommended):**
   ```bash
   python3 setup/infrastructure/update_iam_trust_policy.py
   ```
   
   **Manual:** Update the trust policy in AWS IAM Console (see detailed instructions below)

3. **For detailed cross-account setup instructions**, see:
   - [`setup/infrastructure/README.md`](setup/infrastructure/README.md#cross-account-configuration) (includes comprehensive cross-account setup)
   - [`docs/glue-integration-journey.md`](docs/glue-integration-journey.md) (troubleshooting guide)

### 6. Create Database Schema

Execute the master setup script to create all TPC-DS tables in all formats:

```sql
-- Execute in Snowflake
@setup/schemas/create_tpcds_objects.sql
```

Or run individual setup scripts:

```sql
-- Native tables
@setup/schemas/create_native.sql

-- Iceberg Snowflake-managed tables
@setup/schemas/create_iceberg_sf.sql

-- Iceberg Glue-managed tables (requires AWS setup first)
@setup/schemas/create_iceberg_glue.sql

-- External tables
@setup/schemas/create_external.sql
```

This will create:

- All 4 table formats (Native, Iceberg SF, Iceberg Glue, External)
- 24 TPC-DS tables in each format
- All necessary AWS integrations and external volumes

### 7. Set Up Glue-Managed Tables (For Iceberg Glue Format)

If you're testing the Iceberg Glue format, you need to set up Glue-managed tables:

```bash
cd setup/glue/

# Install additional dependencies
pip install -r requirements.txt

# Step 1: Create Glue tables (using Spark)
python scripts/create/create_glue_tables.py

# Step 2: Load TPC-DS data
python scripts/create/load_tpcds_data.py

# Step 3: Create Snowflake references
python scripts/create/create_snowflake_glue_tables.py

# Step 4: Verify setup
python scripts/verify/verify_glue_tables.py
```

See [`setup/glue/README.md`](setup/glue/README.md) for detailed instructions.

### 8. Load Test Data

#### Using TPC-DS Data Loader

```bash
# Load environment variables (from project root)
source load_env_vars.sh

# Run data loader
python setup/data/main.py
```

Or use the runner script:

```bash
python setup/data/run_tpcds_loader.py
```

The data loader will:

- Generate TPC-DS data (if not already present)
- Convert to Parquet format
- Load data into all configured table formats

See [`setup/data/README.md`](setup/data/README.md) for detailed instructions.

## 📊 Generating and Loading TPC-DS Data

This section provides comprehensive guidance on generating TPC-DS benchmark data and loading it into all four table formats. The workflow varies by format, with Glue-managed Iceberg tables requiring special handling.

### Overview

The TPC-DS data generation and loading process consists of:

1. **Data Generation**: Generate standard TPC-DS data using the official toolkit
2. **Format Conversion**: Convert generated `.dat` files to Parquet format
3. **Data Loading**: Load data into Snowflake tables (format-specific methods)

**Important**: Glue-managed Iceberg tables require a different workflow than other formats. They must use Spark with Iceberg extensions for both table creation and data loading, as they cannot use standard SQL INSERT operations.

### Prerequisites

#### TPC-DS Toolkit Installation

The TPC-DS toolkit is required for data generation. The generator will attempt to auto-install it, but manual installation is recommended:

**Option 1: Auto-Install (Automatic)**

The generator will automatically download and compile the TPC-DS toolkit if not found. This requires:
- Internet connection
- `wget` or `curl` command-line tool
- `unzip` command
- `gcc` compiler (for Linux/macOS)
- `make` command

**Option 2: Manual Installation**

1. Download the TPC-DS toolkit from [TPC.org](https://www.tpc.org/tpcds/)
2. Extract to a directory (e.g., `/opt/tpcds-kit` or `~/tpcds-kit`)
3. Compile the tools:
   ```bash
   cd /path/to/tpcds-kit/tools
   make OS=LINUX CC=gcc  # For Linux
   # or
   make OS=MACOS CC=gcc  # For macOS
   ```

The generator will search for the toolkit in common locations:
- `/opt/tpcds-kit`
- `/usr/local/tpcds-kit`
- `~/tpcds-kit`
- `tools/tpcds-kit` (relative to project root)

#### Python Packages

Ensure all required packages are installed:

```bash
pip install -r requirements.txt
```

Key packages for data generation and loading:
- `pandas` - Data manipulation
- `pyarrow` - Parquet file handling
- `boto3` - AWS S3 integration
- `snowflake-connector-python` - Snowflake connectivity

#### Format-Specific Requirements

**For Native and Iceberg SF formats:**
- Snowflake connection configured
- Database and schemas created (see Setup Instructions)

**For External format:**
- AWS credentials configured
- S3 bucket accessible
- External tables created in Snowflake

**For Glue-managed Iceberg format:**
- Spark installation (local) or AWS Glue 4.0+ access
- Iceberg JAR files (see Glue-Managed Iceberg Tables section)
- AWS Glue catalog access
- Glue database created

### Data Generation

#### Understanding Scale Factors

TPC-DS scale factors determine the data size:

| Scale Factor | Approximate Size | Use Case |
|--------------|------------------|----------|
| 0.01 | ~10 MB | Quick testing |
| 0.1 | ~100 MB | Development |
| 1.0 | ~1 GB | Standard testing |
| 10.0 | ~10 GB | Performance testing |
| 100.0 | ~100 GB | Large-scale testing |

#### Generating Data

**Using the Main Application:**

```bash
# Generate data with scale factor 1.0 (1GB)
python setup/data/main.py --action generate --scale-factor 1.0

# Generate with custom data directory
python setup/data/main.py --action generate --scale-factor 10.0 --data-dir /path/to/data
```

**Using the Generator Directly:**

```bash
python setup/data/tpcds_generator.py --scale-factor 1.0 --convert-parquet
```

#### Data Structure

After generation, data is organized as:

```
data/tpcds_data_sf1.0/
├── *.dat                    # Original TPC-DS data files (24 tables)
└── parquet/                 # Parquet format files
    ├── call_center.parquet
    ├── catalog_page.parquet
    ├── customer.parquet
    └── ... (24 tables total)
```

The generator automatically converts `.dat` files to Parquet format for efficient loading.

### Loading Data by Format

The loading process differs significantly between formats. Below are format-specific instructions.

#### Native Snowflake Tables

**Method**: Direct INSERT via Snowflake connector

**Workflow:**
1. Generate and convert data to Parquet (if not already done)
2. Load data using the standard loader

**Commands:**

```bash
# Load into native tables only
python setup/data/main.py --action load --formats native --data-dir data/tpcds_data_sf1.0

# Full pipeline (generate + load)
python setup/data/main.py --action full --scale-factor 1.0 --formats native
```

**What Happens:**
- Parquet files are read into pandas DataFrames
- Data is inserted using `INSERT INTO` statements via Snowflake connector
- All 24 tables are loaded sequentially

**Verification:**

```sql
-- Check row counts in Snowflake
SELECT COUNT(*) FROM tpcds_native_format.call_center;
SELECT COUNT(*) FROM tpcds_native_format.store_sales;
```

#### Iceberg Snowflake-Managed Tables

**Method**: Direct INSERT via Snowflake connector (Iceberg metadata managed by Snowflake)

**Workflow:**
1. Generate and convert data to Parquet
2. Load data using the standard loader (Snowflake handles Iceberg metadata)

**Commands:**

```bash
# Load into Iceberg SF tables only
python setup/data/main.py --action load --formats iceberg_sf --data-dir data/tpcds_data_sf1.0

# Full pipeline
python setup/data/main.py --action full --scale-factor 1.0 --formats iceberg_sf
```

**What Happens:**
- Same as native tables, but Snowflake automatically manages Iceberg metadata
- Iceberg files are written to the external volume
- ACID properties and versioning are enabled

**Verification:**

```sql
-- Check row counts
SELECT COUNT(*) FROM tpcds_iceberg_sf_format.call_center;

-- Check Iceberg metadata
SHOW ICEBERG TABLES IN SCHEMA tpcds_iceberg_sf_format;
```

#### External Tables

**Method**: Write Parquet files to S3, refresh external table metadata

**Workflow:**
1. Generate and convert data to Parquet
2. Upload Parquet files to S3
3. Refresh external table metadata

**Commands:**

```bash
# Load into external tables only
python setup/data/main.py --action load --formats external --data-dir data/tpcds_data_sf1.0 --refresh-external

# Full pipeline
python setup/data/main.py --action full --scale-factor 1.0 --formats external --refresh-external
```

**What Happens:**
- Parquet files are uploaded to S3 at the external table location
- External table metadata is refreshed to discover new files
- Tables read directly from S3 without data loading into Snowflake

**S3 Structure:**

```
s3://your-bucket/iceberg-performance-test/tpcds_external_format/
├── call_center_external/
│   └── data.parquet
├── store_sales_external/
│   └── data.parquet
└── ... (24 tables)
```

**Verification:**

```sql
-- Check row counts
SELECT COUNT(*) FROM tpcds_external_format.call_center;

-- Refresh if needed
ALTER EXTERNAL TABLE tpcds_external_format.call_center REFRESH;
```

#### Iceberg Glue-Managed Tables

**⚠️ IMPORTANT**: Glue-managed Iceberg tables require a **completely different workflow** than other formats. They cannot use the standard data loader.

**Why Special Handling is Required:**

1. **ACID Properties**: Iceberg tables require proper metadata management for ACID transactions
2. **Glue Catalog**: Tables must be created in AWS Glue catalog first
3. **Iceberg Operations**: Data must be loaded using Iceberg append operations via Spark, not standard SQL INSERT
4. **Metadata Files**: Iceberg generates metadata files that must be tracked in the Glue catalog

**Prerequisites for Glue-Managed Tables:**

1. **JAR Files**: Download Iceberg runtime JARs for Spark

   ```bash
   mkdir -p setup/glue/libs/jars
   cd setup/glue/libs/jars
   
   # Download Iceberg JARs
   curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar
   curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar
   ```

   See [`setup/glue/README.md`](setup/glue/README.md#downloading-jar-files) for detailed download instructions.

2. **Spark Setup** (for local approach):
   - Install Spark 3.3+ with Python support
   - Configure Spark to use Iceberg extensions
   - Ensure AWS credentials are configured

3. **AWS Glue Setup** (for ETL approach):
   - AWS Glue 4.0+ access
   - Glue service role with S3 and Glue catalog permissions
   - Upload JARs to S3 for Glue job access

4. **Configuration Files**:
   - `setup/glue/config/spark_config.yaml` - Spark configuration
   - `setup/glue/config/glue_catalog_config.yaml` - Glue catalog settings
   - `setup/glue/config/tpcds_table_schemas.yaml` - Table schemas

**Step-by-Step Workflow for Glue-Managed Tables:**

**Step 1: Create Glue Tables**

You have two approaches:

**Approach A: Local Spark (Recommended for Development)**

```bash
cd setup/glue/

# Install additional dependencies
pip install -r requirements.txt

# Create Glue tables using local Spark
python scripts/create/create_glue_tables.py
```

**Approach B: AWS Glue ETL (Recommended for Production)**

```bash
cd setup/glue/scripts

# Generate the Glue ETL script
python generate_glue_etl_job.py

# Deploy and run the Glue job
./deploy_and_run_glue_job.sh
```

See [`setup/glue/README.md`](setup/glue/README.md) for detailed instructions on both approaches.

**Step 2: Load TPC-DS Data into Glue Tables**

⚠️ **Critical**: This step must use Spark with Iceberg extensions, NOT the standard data loader.

```bash
cd setup/glue/

# Load data using Spark (reads from Parquet files)
python scripts/create/load_tpcds_data.py
```

**What Happens:**
- Spark reads Parquet files from your data directory
- Uses Iceberg append operations to load data
- Maintains ACID properties and metadata
- Creates Iceberg metadata files in S3

**S3 Structure for Glue Iceberg Tables:**

```
s3://your-bucket/iceberg-performance-test/iceberg_glue_format/
├── call_center/
│   ├── data/
│   │   └── *.parquet
│   └── metadata/
│       ├── *.metadata.json
│       └── snapshots/
└── ... (24 tables)
```

**Step 3: Create Snowflake References**

After tables are created and data is loaded in Glue, create Snowflake references:

```bash
python scripts/create_snowflake_glue_tables.py
```

This creates Snowflake table references that point to the Glue-managed tables.

**Step 4: Verify Setup**

```bash
# Verify in Glue and Snowflake
python scripts/verify/verify_glue_tables.py
```

**Verification in Glue Console:**
1. Go to AWS Glue Console → Databases → `iceberg_performance_test`
2. Verify all 24 tables are listed
3. Check that table type shows "ICEBERG"

**Verification in Snowflake:**

```sql
-- Check row counts
SELECT COUNT(*) FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center;

-- List all Glue tables
SHOW TABLES IN SCHEMA AWS_GLUE_CATALOG.iceberg_performance_test;
```

**Complete Workflow Summary:**

```bash
# 1. Generate TPC-DS data (if not already done)
python setup/data/main.py --action generate --scale-factor 1.0

# 2. Create Glue tables (using Spark)
cd setup/glue/
python scripts/create/create_glue_tables.py

# 3. Load data into Glue tables (using Spark)
python scripts/create/load_tpcds_data.py

# 4. Create Snowflake references
python scripts/create/create_snowflake_glue_tables.py

# 5. Verify setup
python scripts/verify/verify_glue_tables.py
```

**Important Notes:**

- ❌ **DO NOT** use `setup/data/main.py` for Glue-managed tables
- ❌ **DO NOT** use standard SQL INSERT for Glue Iceberg tables
- ✅ **DO USE** Spark with Iceberg extensions for table creation and data loading
- ✅ **DO VERIFY** tables exist in Glue catalog before creating Snowflake references
- ✅ **DO CHECK** S3 metadata files are created correctly

**Troubleshooting Glue Tables:**

- **Table creation fails**: Check Spark configuration and JAR files
- **Data loading fails**: Verify Parquet files exist and Spark can access them
- **Snowflake can't query**: Verify external volume and catalog integration are configured
- **No metadata files**: Ensure Iceberg extensions are properly loaded in Spark

For detailed troubleshooting, see [`setup/glue/README.md`](setup/glue/README.md#troubleshooting) and [`docs/glue-integration-journey.md`](docs/glue-integration-journey.md).

### Quick Reference

**Generate Data Only:**

```bash
python setup/data/main.py --action generate --scale-factor 1.0
```

**Load Data for Native, Iceberg SF, or External:**

```bash
# Load all formats (except Glue)
python setup/data/main.py --action load --formats native iceberg_sf external

# Full pipeline
python setup/data/main.py --action full --scale-factor 1.0 --formats native iceberg_sf external
```

**Load Data for Glue-Managed Iceberg:**

```bash
# Step 1: Create tables
cd setup/glue/
python scripts/create/create_glue_tables.py

# Step 2: Load data
python scripts/create/load_tpcds_data.py

# Step 3: Create Snowflake references
python scripts/create/create_snowflake_glue_tables.py
```

**Verify Data:**

```bash
# For standard formats
python setup/data/main.py --action verify

# For Glue tables
cd setup/glue/
python scripts/verify/verify_glue_tables.py
```

**Cleanup Data:**

```bash
# For standard formats
python setup/data/main.py --action cleanup

# For Glue tables
cd setup/glue/
python scripts/maintenance/cleanup_glue_tables.py
```

### Additional Resources

- **[TPC-DS Data Loader Documentation](setup/data/README.md)** - Detailed data loader documentation
- **[Glue Table Setup Guide](setup/glue/README.md)** - Comprehensive Glue-managed table setup
- **[Glue Integration Journey](docs/glue-integration-journey.md)** - Troubleshooting and integration guide
- **[TPC-DS Configuration](config/tpcds_config.yaml)** - Configuration file reference

## 🧪 Running Performance Tests

### Quick Start

#### Quick Test (Test Mode - Limited Queries)

```bash
python benchmark/src/main.py --test-mode
```

#### Standard Test (All Formats, All Queries)

```bash
python benchmark/src/main.py
```

#### Test Specific Formats

```bash
python benchmark/src/main.py --formats native iceberg_sf
```

#### Test Query Range

```bash
python benchmark/src/main.py --query-range 1 10
```

### Configuration

Edit `benchmark/config/perf_test_config.yaml` to customize:

- Test modes (quick, standard, comprehensive, stress)
- Performance thresholds
- Reporting options
- Query execution settings

### Test Output

Results are saved to:

- `results/` - JSON and CSV results
- `results/reports/` - HTML reports with charts

See [`benchmark/README.md`](benchmark/README.md) for detailed testing documentation.

## 📊 Understanding Results

### Performance Metrics

- **Execution Time**: Query execution duration
- **Data Scanned**: Bytes of data scanned
- **Rows Produced**: Number of rows returned
- **Cache Hit Ratio**: Percentage of data served from cache
- **Credits Used**: Snowflake compute credits consumed
- **Memory Usage**: Peak memory consumption

### Format Comparison

The framework compares performance across 4 table formats:

- **Native Snowflake**: Baseline performance, fully managed
- **Iceberg (Snowflake)**: ACID transactions, time travel, versioning
- **Iceberg (Glue)**: Cross-platform compatibility, AWS ecosystem integration
- **External Iceberg**: Cost-effective, direct S3 access

### Reports

The framework generates comprehensive reports:

- **HTML Reports**: Interactive charts and visualizations
- **JSON Reports**: Machine-readable results for further analysis
- **CSV Exports**: Spreadsheet-compatible data
- **Statistical Analysis**: ANOVA, correlation analysis, trend detection

## 💰 Cost Optimization

The framework includes comprehensive cost measurement, analysis, and optimization capabilities:

### Cost Components Tracked

1. **Snowflake Compute Costs**: Warehouse credits based on execution time and warehouse size
2. **Snowflake Storage Costs**: Internal storage costs for native format
3. **AWS S3 Storage Costs**: Storage costs for external formats (Iceberg Glue, External)
4. **AWS S3 Request Costs**: GET, PUT, LIST request costs
5. **AWS Glue Catalog Costs**: Metadata storage and API call costs
6. **Data Transfer Costs**: Cross-region data transfer costs

### Cost Measurement Features

- **Automatic Cost Calculation**: Costs calculated automatically for each query execution
- **Cost Breakdown**: Detailed cost breakdown by component (compute, storage, S3, Glue)
- **Cost Comparison**: Compare costs across formats to identify most cost-effective options
- **Cost-Performance Analysis**: Analyze cost-to-performance ratios
- **Cost Optimization Recommendations**: Automated recommendations for cost reduction

### Cost Reports

Cost analysis is included in all generated reports:
- **Cost per Query**: Individual query costs across formats
- **Cost per Format**: Total and average costs per format
- **Cost Breakdown**: Component-level cost breakdowns
- **Cost Trends**: Cost trends over time
- **ROI Analysis**: Return on investment for optimizations

### Optimization Tools

The framework provides optimization tools:

1. **Query Optimizer** (`benchmark/src/query_optimizer.py`): Format-specific query optimization recommendations
2. **Warehouse Optimizer** (`benchmark/src/warehouse_optimizer.py`): Warehouse size optimization recommendations
3. **Data Optimizer** (`benchmark/src/data_optimizer.py`): Clustering and partitioning recommendations

### Configuration

Configure cost tracking and optimization in `benchmark/config/perf_test_config.yaml`:
- Snowflake credit pricing
- AWS S3 and Glue pricing
- Cost thresholds and alerts
- Optimization settings

For detailed cost optimization guidance, see the [Cost Optimization Guide](docs/cost-optimization-guide.md).

## 🗂️ TPC-DS Schema

This project uses the TPC-DS benchmark schema with 24 tables:

### Dimension Tables (17)

- call_center, catalog_page, customer, customer_address
- customer_demographics, date_dim, household_demographics
- income_band, item, promotion, reason, ship_mode
- store, time_dim, warehouse, web_page, web_site

### Fact Tables (7)

- catalog_returns, catalog_sales, inventory
- store_returns, store_sales, web_returns, web_sales

All tables are created in 4 formats:

- Native Snowflake tables
- Iceberg Snowflake-managed tables
- Iceberg AWS Glue-managed tables
- External tables

See [`setup/schemas/`](setup/schemas/) for DDL scripts.

## 🚨 Troubleshooting

### Common Issues

#### 1. Connection Errors

```bash
# Test Snowflake connection
python -c "from benchmark.src.query_engine import QueryEngine; q = QueryEngine(); print('Connected:', q.test_connection())"
```

#### 2. Permission Errors

- Ensure proper Snowflake role permissions (ACCOUNTADMIN recommended for setup)
- Check AWS IAM role permissions
- Verify S3 bucket access and policies

#### 3. Glue Table Issues

- Verify AWS Glue catalog is accessible
- Ensure tables are created in Glue before creating Snowflake references
- Check S3 paths and permissions

#### 4. Data Loading Issues

- Verify TPC-DS data files exist in `data/` directory
- Check Parquet file format compatibility
- Ensure sufficient warehouse size for large datasets

#### 5. Test Execution Issues

- Check query syntax for your Snowflake version
- Verify all tables exist and have data
- Review logs in `benchmark/logs/`

#### 6. Environment Variable Issues

**Scripts can't find environment variables:**

1. Make sure variables are exported: `export VAR_NAME=value`
2. Check if `.env` file is being loaded (use `set -a; source .env; set +a`)
3. Verify variable names match exactly (case-sensitive)
4. Ensure required variables are set (see Environment Variables Reference above)

**Private key file not found:**

- The script searches in multiple default locations (see Environment Variables Reference)
- Set `SNOWFLAKE_PRIVATE_KEY_FILE` explicitly if your key is in a different location

**Configuration conflicts:**

- If you have both environment variables and YAML config files, environment variables take precedence

## 🧹 Cleanup

### AWS Resources Cleanup

```bash
cd setup/infrastructure/
terraform destroy
```

Or manually clean up Glue tables:

```bash
cd setup/glue/
python scripts/maintenance/cleanup_glue_tables.py
```

### Snowflake Cleanup

```sql
-- Drop database and all schemas
DROP DATABASE IF EXISTS tpcds_performance_test CASCADE;

-- Or drop individual schemas
DROP SCHEMA IF EXISTS TPCDS_NATIVE_FORMAT CASCADE;
DROP SCHEMA IF EXISTS TPCDS_ICEBERG_SF_FORMAT CASCADE;
DROP SCHEMA IF EXISTS TPCDS_ICEBERG_GLUE_FORMAT CASCADE;
DROP SCHEMA IF EXISTS TPCDS_EXTERNAL_FORMAT CASCADE;
```

## 📚 Additional Documentation

### Component-Specific Documentation

- **[AWS Infrastructure Setup](setup/infrastructure/README.md)** - Detailed AWS setup with Terraform
- **[TPC-DS Performance Testing](benchmark/README.md)** - Complete testing framework documentation
- **[TPC-DS Data Loader](setup/data/README.md)** - Data generation and loading guide
- **[Glue Table Setup](setup/glue/README.md)** - AWS Glue-managed table workflow
- **[Snowflake Schema](setup/schemas/)** - Database schema and DDL files

### Quick Reference

- **Main Testing**: Use `benchmark/src/main.py` for performance tests
- **Data Loading**: Use `setup/data/` for generating and loading data
- **Glue Setup**: Use `setup/glue/scripts/` for Glue-managed tables
- **Schema Setup**: Use `setup/schemas/create_tpcds_objects.sql` for initial setup
- **AWS Infrastructure**: Use `setup/infrastructure/` for infrastructure provisioning

## 🔧 Configuration

### Main Configuration Files

- `config/snowflake_config.yaml` - Snowflake connection settings (shared)
- `config/aws_config.yaml` - AWS service configuration (shared)
- `config/tpcds_config.yaml` - TPC-DS data generation settings (shared)
- `benchmark/config/perf_test_config.yaml` - Performance test configuration
- `benchmark/config/test_scenarios.yaml` - Test scenario definitions
- `benchmark/config/reporting_config.yaml` - Report generation settings
- `setup/glue/config/glue_catalog_config.yaml` - Glue catalog settings

## 📝 Project Components

### Core Components

1. **Performance Testing Framework** (`benchmark/`)
   - Automated query execution
   - Metrics collection and analysis
   - Report generation

2. **Data Management** (`setup/data/`)
   - TPC-DS data generation
   - Multi-format data loading
   - Data validation

3. **Infrastructure** (`setup/infrastructure/`)
   - Terraform configurations
   - S3, Glue, IAM setup
   - Automated provisioning

4. **Glue Workflow** (`setup/glue/`)
   - Spark-based table creation
   - Data loading pipeline
   - Verification and cleanup

## 🤝 Contributing

When contributing to this project:

1. Ensure all sensitive files are in `.gitignore` (credentials, keys, state files)
2. Follow the existing project structure
3. Update relevant README files
4. Test your changes across all table formats

## 📄 License

[Add your license information here]
