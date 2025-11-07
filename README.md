# Snowflake Iceberg Performance Testing Suite

A comprehensive performance testing framework for comparing 4 different Snowflake table formats using the TPC-DS benchmark:

1. **Native Snowflake Tables** - Standard Snowflake managed tables
2. **Iceberg Snowflake-Managed** - Iceberg format with Snowflake catalog
3. **Iceberg AWS Glue-Managed** - Iceberg format with external AWS Glue catalog
4. **External Tables** - External tables pointing to parquet data

## 🚀 Features

- **TPC-DS Benchmark Testing**: 99 standard TPC-DS queries across all formats
- **Automated Test Execution**: Python framework with comprehensive metrics collection
- **Multiple Table Formats**: Native, Iceberg (Snowflake), Iceberg (Glue), External
- **Performance Analytics**: Detailed metrics, statistical analysis, and recommendations
- **Scalable Testing**: Support for multiple TPC-DS scale factors (0.01, 0.1, 1.0, etc.)
- **Automated Reporting**: HTML, JSON, CSV, and PDF report generation
- **Complete Setup Workflow**: Automated infrastructure and data loading

## 📁 Project Structure

```
iceberg-performance-benchmark/
├── setup/              # Setup functionality (infrastructure, schemas, glue, data)
├── benchmark/          # Performance testing framework
├── config/             # Global configuration files
├── lib/                # Shared utility modules
├── data/               # TPC-DS generated data (gitignored)
└── results/            # Test results and reports (gitignored)
```

See component READMEs for detailed structure: [`setup/data/README.md`](setup/data/README.md), [`benchmark/README.md`](benchmark/README.md), [`setup/glue/README.md`](setup/glue/README.md)

## 🛠️ Prerequisites

- **Python 3.8+** with dependencies: `pip install -r requirements.txt`
- **Snowflake account** with ACCOUNTADMIN role, warehouse, and permissions for external volumes/catalog integrations
- **AWS account** (for Glue catalog and S3) - S3 bucket, Glue catalog access, IAM roles
- **Terraform** (for AWS infrastructure setup)
- **Spark** (for Glue table creation, if using Iceberg Glue format)

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

1. Copy and edit the example file:
   ```bash
   cp env.example .env
   # Edit .env with your credentials
   ```

2. Load environment variables:
   ```bash
   set -a && source .env && set +a
   ```

**Required variables:**
- `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`
- `AWS_ACCOUNT_ID`, `AWS_S3_BUCKET`

See `env.example` for complete variable reference with descriptions.

### 4. Authentication Setup

**Option A: Password** - Set `SNOWFLAKE_PASSWORD` in `.env`

**Option B: Key Pair (Recommended)** - More secure, bypasses MFA:
```bash
# Generate key pair
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# Upload public key to Snowflake (User Profile → Key Pairs)
# Set in .env: SNOWFLAKE_PRIVATE_KEY_FILE="/path/to/rsa_key.p8"
```

### 5. AWS Infrastructure Setup

```bash
cd setup/infrastructure/
terraform init && terraform apply
```

**Important:** After creating Snowflake integrations, configure cross-account access:
```bash
python3 setup/infrastructure/get_snowflake_iam_details.py
python3 setup/infrastructure/update_iam_trust_policy.py
```

See [`setup/infrastructure/README.md`](setup/infrastructure/README.md) for detailed setup and cross-account configuration.

### 6. Create Database Schema

```sql
-- Execute in Snowflake to create all table formats
@setup/schemas/create_tpcds_objects.sql
```

### 7. Set Up Glue Tables (Iceberg Glue Format Only)

```bash
cd setup/glue/
pip install -r requirements.txt
python scripts/create/create_glue_tables.py
python scripts/create/load_tpcds_data.py
python scripts/create/create_snowflake_glue_tables.py
```

See [`setup/glue/README.md`](setup/glue/README.md) for details.

### 8. Load Test Data

```bash
python setup/data/main.py
```

See [`setup/data/README.md`](setup/data/README.md) for detailed data generation and loading instructions.

## 📊 Generating and Loading TPC-DS Data

**Quick Start:**
```bash
# Generate and load data (scale factor 1.0 = ~1GB)
python setup/data/main.py --action full --scale-factor 1.0 --formats native iceberg_sf external
```

**Scale Factors:** 0.01 (~10MB), 0.1 (~100MB), 1.0 (~1GB), 10.0 (~10GB), 100.0 (~100GB)

**Important:** Glue-managed Iceberg tables require Spark-based workflow (see Setup step 7).

See [`setup/data/README.md`](setup/data/README.md) for:
- TPC-DS toolkit installation
- Format-specific loading workflows
- Data generation options
- Verification and cleanup

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
  - 📊 [View Example Report](docs/example-report.html) - Sample HTML report with performance metrics, cost analysis, and format comparisons
  
  ![Example Performance Report](docs/example-report.png)
- **JSON Reports**: Machine-readable results for further analysis
- **CSV Exports**: Spreadsheet-compatible data
- **Statistical Analysis**: ANOVA, correlation analysis, trend detection

## 💰 Cost Optimization

The framework tracks costs across all components (Snowflake compute/storage, AWS S3, Glue, data transfer) and provides:
- Automatic cost calculation per query
- Cost comparison across formats
- Cost-performance analysis
- Optimization recommendations

See [Cost Optimization Guide](docs/cost-optimization-guide.md) for detailed guidance.

## 🗂️ TPC-DS Schema

24 tables (17 dimension, 7 fact) created in all 4 formats. See [`setup/schemas/`](setup/schemas/) for DDL scripts.

## 🚨 Troubleshooting

**Connection:** `python -c "from benchmark.src.query_engine import QueryEngine; q = QueryEngine(); print('Connected:', q.test_connection())"`

**Common Issues:**
- **Permissions**: Ensure ACCOUNTADMIN role, AWS IAM permissions, S3 access
- **Environment Variables**: Verify `.env` is loaded (`set -a && source .env && set +a`)
- **Glue Tables**: Verify tables exist in Glue catalog before creating Snowflake references
- **Data Loading**: Check data files exist, Parquet compatibility, warehouse size

See [`docs/glue-integration-journey.md`](docs/glue-integration-journey.md) and component READMEs for detailed troubleshooting.

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

- **[AWS Infrastructure Setup](setup/infrastructure/README.md)** - Terraform setup and cross-account configuration
- **[TPC-DS Performance Testing](benchmark/README.md)** - Testing framework documentation
- **[TPC-DS Data Loader](setup/data/README.md)** - Data generation and loading guide
- **[Glue Table Setup](setup/glue/README.md)** - Glue-managed table workflow
- **[Glue Integration Journey](docs/glue-integration-journey.md)** - Troubleshooting guide
- **[Cost Optimization Guide](docs/cost-optimization-guide.md)** - Cost analysis and optimization

## 🔧 Configuration

Key configuration files:
- `config/*.yaml` - Global settings (Snowflake, AWS, TPC-DS)
- `benchmark/config/*.yaml` - Test configuration
- `setup/glue/config/*.yaml` - Glue catalog settings
- `.env` - Environment variables (see `env.example`)

## 🤝 Contributing

When contributing to this project:

1. Ensure all sensitive files are in `.gitignore` (credentials, keys, state files)
2. Follow the existing project structure
3. Update relevant README files
4. Test your changes across all table formats

## 📄 License

[Add your license information here]
