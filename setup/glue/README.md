# Glue Table Setup

This directory contains all the necessary components to create Glue-managed Iceberg tables for TPC-DS data.

## Overview

This setup follows the correct Iceberg workflow:

1. **Create Iceberg tables** in Glue catalog (with proper metadata)
2. **Snowflake** references the existing Glue tables
3. **Data** is loaded with proper Iceberg metadata tracking

## Directory Structure

```
setup/glue/
├── README.md                           # This file
├── config/
│   ├── spark_config.yaml              # Spark configuration
│   ├── glue_catalog_config.yaml       # Glue catalog settings
│   └── tpcds_table_schemas.yaml       # TPC-DS table schemas
├── scripts/
│   ├── create_glue_tables.py          # Main script to create Glue tables (Spark approach)
│   ├── load_tpcds_data.py             # Load TPC-DS data into Glue tables
│   ├── create_snowflake_glue_tables.py # Create Snowflake references to Glue tables
│   ├── verify_glue_tables.py          # Verify tables in Glue and Snowflake
│   ├── cleanup_glue_tables.py         # Cleanup script
│   ├── generate_glue_etl_job.py       # Generate Glue ETL job script
│   ├── create_all_iceberg_tables_glue_etl.py # Generated ETL job script
│   ├── create_glue_job.py             # Python deployment script
│   └── deploy_and_run_glue_job.sh     # Deployment script
├── logs/                              # Log files
└── requirements.txt                   # Python dependencies
```

## Prerequisites

1. **AWS Credentials** configured
2. **Spark** with Iceberg extensions (for local Spark approach) OR **AWS Glue 4.0+** (for ETL approach)
3. **Snowflake** connection configured
4. **TPC-DS data** available in Parquet format (for data loading)
5. **AWS Infrastructure** deployed (see `setup/infrastructure/README.md`)
6. **Required JAR files** (see [Downloading JAR Files](#downloading-jar-files) below)

## Downloading JAR Files

The Iceberg and AWS SDK JAR files are required for both approaches. These files are not included in the repository and must be downloaded separately.

### Required JAR Files

For **Local Spark Approach** (Spark 3.3):
- `iceberg-aws-1.4.2.jar`
- `iceberg-spark-runtime-3.3_2.12-1.4.2.jar`

For **AWS Glue ETL Approach** (Glue 4.0 uses Spark 3.3):
- `iceberg-aws-1.4.2.jar`
- `iceberg-spark-runtime-3.3_2.12-1.4.2.jar`
- `aws-sdk-bundle-2.20.66.jar`

### Download Instructions

#### Option 1: Download from Maven Central (Recommended)

Create the directory if it doesn't exist:
```bash
mkdir -p setup/glue/libs/jars
cd setup/glue/libs/jars
```

**Download Iceberg JARs:**
```bash
# Iceberg AWS integration
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws/1.4.2/iceberg-aws-1.4.2.jar

# Iceberg Spark runtime (Spark 3.3)
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar
```

**Download AWS SDK Bundle (for Glue ETL approach):**
```bash
# AWS SDK Bundle
curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.66/bundle-2.20.66.jar
# Note: Rename it to match the expected filename
mv bundle-2.20.66.jar aws-sdk-bundle-2.20.66.jar
```

#### Option 2: Use Maven Dependency Plugin

If you have Maven installed:
```bash
mkdir -p setup/glue/libs/jars
cd setup/glue/libs/jars

# Download Iceberg JARs
mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-aws:1.4.2:jar -Ddest=iceberg-aws-1.4.2.jar
mvn dependency:get -Dartifact=org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2:jar -Ddest=iceberg-spark-runtime-3.3_2.12-1.4.2.jar

# Download AWS SDK Bundle (for Glue ETL)
mvn dependency:get -Dartifact=software.amazon.awssdk:bundle:2.20.66:jar -Ddest=aws-sdk-bundle-2.20.66.jar
```

#### Option 3: Use Gradle (if you have Gradle installed)

```bash
mkdir -p setup/glue/libs/jars
cd setup/glue/libs/jars

# Create a temporary build.gradle file
cat > build.gradle << 'EOF'
plugins {
    id 'java'
}

repositories {
    mavenCentral()
}

configurations {
    jars
}

dependencies {
    jars 'org.apache.iceberg:iceberg-aws:1.4.2'
    jars 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2'
    jars 'software.amazon.awssdk:bundle:2.20.66'
}

task downloadJars(type: Copy) {
    from configurations.jars
    into '.'
    rename { fileName ->
        if (fileName.contains('iceberg-aws-1.4.2')) {
            return 'iceberg-aws-1.4.2.jar'
        } else if (fileName.contains('iceberg-spark-runtime-3.3_2.12-1.4.2')) {
            return 'iceberg-spark-runtime-3.3_2.12-1.4.2.jar'
        } else if (fileName.contains('bundle-2.20.66')) {
            return 'aws-sdk-bundle-2.20.66.jar'
        }
        return fileName
    }
}
EOF

# Run the download task
gradle downloadJars

# Clean up
rm build.gradle build
```

### Verify Downloads

After downloading, verify the JAR files exist:
```bash
ls -lh setup/glue/libs/jars/
```

You should see:
- `iceberg-aws-1.4.2.jar` (~50-60 KB)
- `iceberg-spark-runtime-3.3_2.12-1.4.2.jar` (~15-20 MB)
- `aws-sdk-bundle-2.20.66.jar` (~15-20 MB) - only if using Glue ETL approach

### Upload JARs to S3 (for Glue ETL Approach) {#upload-jars-to-s3-for-glue-etl-approach}

If using the Glue ETL approach, you'll need to upload the JARs to S3 so they can be referenced by the Glue job:

```bash
# Set your bucket name
export AWS_S3_BUCKET="your-bucket-name"

# Upload JARs to S3
aws s3 cp setup/glue/libs/jars/iceberg-aws-1.4.2.jar \
  s3://${AWS_S3_BUCKET}/glue-scripts/jars/iceberg-aws-1.4.2.jar

aws s3 cp setup/glue/libs/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar \
  s3://${AWS_S3_BUCKET}/glue-scripts/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar

aws s3 cp setup/glue/libs/jars/aws-sdk-bundle-2.20.66.jar \
  s3://${AWS_S3_BUCKET}/glue-scripts/jars/aws-sdk-bundle-2.20.66.jar
```

**Note:** The deployment script (`deploy_and_run_glue_job.sh`) will automatically upload the JARs if they're present locally.

## Two Approaches

This setup supports two approaches for creating Glue-managed Iceberg tables:

### Approach 1: Local Spark (Recommended for Development)

Uses local Spark with Iceberg extensions to create tables directly.

**Pros:**
- Full control over Spark configuration
- Faster iteration for development
- No AWS Glue job costs

**Cons:**
- Requires local Spark installation
- Manual dependency management

### Approach 2: AWS Glue ETL (Recommended for Production)

Uses AWS Glue ETL jobs to create tables in managed Spark environment.

**Pros:**
- No local Spark setup required
- Managed environment with automatic dependency handling
- Better for production workloads

**Cons:**
- Requires AWS Glue 4.0+
- AWS Glue job costs
- Less control over Spark configuration

## Quick Start

### Approach 1: Local Spark

1. **Download required JAR files** (see [Downloading JAR Files](#downloading-jar-files) above)
2. **Configure settings** in `config/` files
3. **Install dependencies**: `pip install -r requirements.txt`
4. **Create Glue tables**: `python scripts/create_glue_tables.py`
5. **Load data**: `python scripts/load_tpcds_data.py`
6. **Create Snowflake references**: `python scripts/create_snowflake_glue_tables.py`
7. **Verify setup**: `python scripts/verify_glue_tables.py`

### Approach 2: AWS Glue ETL

1. **Download required JAR files** (see [Downloading JAR Files](#downloading-jar-files) above)
2. **Upload JARs to S3** (see [Upload JARs to S3](#upload-jars-to-s3-for-glue-etl-approach) above)
3. **Generate the Glue ETL Script**:
   ```bash
   cd setup/glue/scripts
   python3 generate_glue_etl_job.py
   ```

2. **Deploy and Run**:
   ```bash
   ./deploy_and_run_glue_job.sh
   ```

   Or manually:
   ```bash
   # Upload script to S3
   aws s3 cp create_all_iceberg_tables_glue_etl.py \
     s3://your-bucket-name/glue-scripts/create_iceberg_tables.py

   # Create and start Glue job (see Configuration section for full command)
   aws glue create-job ...
   aws glue start-job-run --job-name create-iceberg-tables-tpcds
   ```

3. **Create Snowflake references**: `python scripts/create_snowflake_glue_tables.py`
4. **Verify setup**: `python scripts/verify_glue_tables.py`

## Detailed Workflow

### Step 1: Create Glue Tables

#### Using Local Spark

- Uses Spark with Iceberg extensions
- Creates tables in AWS Glue catalog
- Registers proper Iceberg metadata
- Command: `python scripts/create_glue_tables.py`

#### Using AWS Glue ETL

- Uses Glue's managed Spark environment
- Automatically handles Iceberg dependencies
- Creates proper Iceberg metadata files
- Command: See Quick Start section above

### Step 2: Load TPC-DS Data

- Loads existing Parquet data into Glue tables
- Uses Iceberg append operations
- Maintains ACID properties
- Command: `python scripts/load_tpcds_data.py`

### Step 3: Reference in Snowflake

- Creates Snowflake table references to Glue tables
- Uses existing external volume and catalog integration
- Enables querying from Snowflake
- Command: `python scripts/create_snowflake_glue_tables.py`

## Configuration

### Environment Variables

These are loaded from `.env` file or environment:

- `AWS_S3_BUCKET`: S3 bucket name
- `AWS_GLUE_DATABASE`: Glue database name
- `AWS_S3_ICEBERG_GLUE_PATH`: S3 warehouse path for Iceberg tables
- `AWS_REGION`: AWS region
- `AWS_ACCOUNT_ID`: AWS account ID

### Configuration Files

Edit the configuration files in `config/` to match your environment:

- `spark_config.yaml`: Spark configuration (for local Spark approach)
- `glue_catalog_config.yaml`: Glue catalog settings
- `tpcds_table_schemas.yaml`: TPC-DS table schemas

### Glue ETL Job Configuration

If using AWS Glue ETL approach:

- **Glue Version**: 4.0 (required for Iceberg support)
- **Worker Type**: G.1X (2 vCPU, 4 GB RAM)
- **Number of Workers**: 2 (adjust based on workload)
- **Max Retries**: 0 (can be increased if needed)

**Full AWS CLI command example:**

```bash
aws glue create-job \
  --name create-iceberg-tables-tpcds \
  --role arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/your-glue-service-role \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://your-bucket-name/glue-scripts/create_iceberg_tables.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--job-language": "python",
    "--job-bookmark-option": "job-bookmark-disable",
    "--GLUE_DATABASE": "iceberg_performance_test",
    "--S3_WAREHOUSE_PATH": "s3://your-bucket-name/iceberg-performance-test/iceberg_glue_format",
    "--AWS_REGION": "us-east-1",
    "--enable-metrics": "",
    "--enable-spark-ui": "true",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type "G.1X" \
  --max-retries 0
```

## Iceberg Configuration

Both approaches automatically configure:

- **Iceberg Catalog**: `glue_catalog` (uses Glue catalog)
- **Iceberg Extensions**: Enabled for Spark
- **Table Properties**:
  - Format: Parquet
  - Compression: Snappy
  - Target file size: 128 MB

## Monitoring

### Using Local Spark

- Check `logs/` directory for detailed execution logs
- Monitor Spark UI (if enabled)

### Using AWS Glue ETL

- AWS Glue Console → ETL → Jobs → `create-iceberg-tables-tpcds`
- CloudWatch Logs for detailed execution logs
- Spark UI for job execution details

## Verification

After tables are created:

1. **Verify in Glue Console**:
   - AWS Glue Console → Databases → `iceberg_performance_test`
   - Check that all 24 tables are listed
   - Verify table type shows "ICEBERG"

2. **Verify in S3**:
   - Check S3 path: `s3://your-bucket-name/iceberg-performance-test/iceberg_glue_format/{table_name}/`
   - Look for `metadata/` folder with `.metadata.json` files

3. **Test from Snowflake**:
   ```sql
   SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center LIMIT 1;
   ```

4. **Use verification script**:
   ```bash
   python scripts/verify_glue_tables.py
   ```

## Troubleshooting

### Common Issues

#### Local Spark Approach

- **AWS Permissions**: Verify AWS credentials and IAM permissions
- **Spark Configuration**: Check `spark_config.yaml` and Iceberg extensions
- **Glue Catalog Access**: Ensure Glue database exists and is accessible
- **Dependencies**: Verify all required JARs are available

#### AWS Glue ETL Approach

- **Job Fails with "Table Already Exists"**: The script handles existing tables gracefully. If a table already exists, it will skip creation and continue.

- **Job Fails with S3 Access Errors**:
  - Check Glue service role has S3 permissions
  - Verify S3 bucket policy allows Glue role access
  - Ensure warehouse path is correct and accessible

- **Job Fails with Glue Catalog Errors**:
  - Verify Glue database exists
  - Check Glue service role has Glue catalog permissions
  - Ensure database name matches exactly (case-sensitive)

- **Tables Created but No Metadata Files**:
  - Verify Glue version is 4.0 or later
  - Check Iceberg extensions are properly configured
  - Ensure warehouse path is accessible

#### General Issues

- **Snowflake Connectivity**: Check Snowflake connection and permissions
- **Cross-Account Access**: Verify IAM trust policy is configured correctly
- **Path Mismatches**: Ensure external volume path matches S3 metadata location

**For detailed troubleshooting**, see:
- `setup/infrastructure/README.md` for cross-account setup
- `docs/glue-integration-journey.md` for common issues and solutions

## Files Reference

### Main Scripts

- `create_glue_tables.py` - Main table creation script (Spark approach, Step 1)
- `load_tpcds_data.py` - Data loading script (Step 2)
- `create_snowflake_glue_tables.py` - Create Snowflake table references (Step 3)
- `verify_glue_tables.py` - Verification and testing
- `cleanup_glue_tables.py` - Cleanup utilities

### ETL Scripts (Approach 2)

- `generate_glue_etl_job.py` - Generates the complete Glue ETL script
- `create_all_iceberg_tables_glue_etl.py` - Complete Glue ETL job script (generated)
- `deploy_and_run_glue_job.sh` - Deployment script
- `create_glue_job.py` - Python deployment script (alternative)

## References

- [AWS Glue Iceberg Support](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Glue ETL Jobs](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-jobs.html)
- [Snowflake Iceberg Tables](https://docs.snowflake.com/en/user-guide/tables-iceberg)
