-- ========================================
-- MASTER TPC-DS TABLE SETUP SCRIPT
-- Snowflake Iceberg Performance Testing Project
-- ========================================
-- This script creates all TPC-DS tables in four different formats:
-- Native, Iceberg SF, Iceberg Glue, External
--
-- IMPORTANT: This script uses DEFAULT values that match environment variable defaults.
-- If you customized any environment variables, you must replace the hardcoded values below:
--
-- Snowflake Configuration:
--   - Database: tpcds_performance_test 
--     (maps to SNOWFLAKE_DATABASE env var, default: tpcds_performance_test)
--   - Warehouse: tpcds_test_wh
--     (maps to SNOWFLAKE_WAREHOUSE env var, default: TPCDS_TEST_WH)
--     Note: Snowflake stores unquoted identifiers in uppercase
--   - Schemas: TPCDS_NATIVE_FORMAT, TPCDS_ICEBERG_SF_FORMAT, TPCDS_ICEBERG_GLUE_FORMAT, 
--              TPCDS_EXTERNAL_FORMAT
--     (map to SNOWFLAKE_SCHEMA_NATIVE, SNOWFLAKE_SCHEMA_ICEBERG_SF, 
--      SNOWFLAKE_SCHEMA_ICEBERG_GLUE, SNOWFLAKE_SCHEMA_EXTERNAL env vars)
--
-- AWS Configuration:
--   - S3 Bucket: <your-bucket-name>
--     (maps to AWS_S3_BUCKET env var)
--   - S3 Path: /tcp-ds-data/
--     (maps to AWS_S3_ICEBERG_PREFIX env var)
--   - AWS Account ID: <your-aws-account-id>
--     (maps to AWS_ACCOUNT_ID env var)
--   - AWS Region: us-east-1
--     (maps to AWS_REGION env var, default: us-east-1)
--   - IAM Role: snowflake-iceberg-role
--     (maps to AWS_ROLE_NAME env var, default: snowflake-iceberg-role)
--   - Glue Database: iceberg_performance_test
--     (maps to AWS_GLUE_DATABASE env var, default: iceberg_performance_test)
--   - Glue Catalog ID: <your-aws-account-id>
--     (maps to AWS_GLUE_CATALOG_ID env var)
--
-- To use custom values, search and replace all occurrences of the default values below.
-- ========================================

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS tpcds_performance_test;
USE DATABASE tpcds_performance_test;

CREATE SCHEMA IF NOT EXISTS TPCDS_NATIVE_FORMAT;
CREATE SCHEMA IF NOT EXISTS TPCDS_ICEBERG_SF_FORMAT;
CREATE SCHEMA IF NOT EXISTS TPCDS_ICEBERG_GLUE_FORMAT;
CREATE SCHEMA IF NOT EXISTS TPCDS_EXTERNAL_FORMAT;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS tpcds_test_wh 
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE tpcds_test_wh;

-- ========================================
-- AWS INTEGRATION SETUP
-- ========================================

-- External Volume for Iceberg Snowflake-managed (S3 data access)
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_VOLUME
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_storage'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://<your-bucket-name>/tcp-ds-data/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-aws-account-id>:role/snowflake-iceberg-role'
    )
  )
  COMMENT = 'External volume for Iceberg Snowflake-managed tables';

-- External Volume for Iceberg AWS Glue-managed (S3 data access)
-- IMPORTANT: STORAGE_BASE_URL must match the S3 path where Glue table metadata files are located
CREATE OR REPLACE EXTERNAL VOLUME ICEBERG_GLUE_VOLUME
  STORAGE_LOCATIONS = (
    (
      NAME = 's3_storage'
      STORAGE_PROVIDER = 'S3'
      STORAGE_BASE_URL = 's3://<your-bucket-name>/iceberg-performance-test/iceberg_glue_format/'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-aws-account-id>:role/snowflake-iceberg-role'
    )
  )
  COMMENT = 'External volume for Iceberg AWS Glue-managed tables';

-- Catalog Integration for Iceberg Snowflake-managed
CREATE OR REPLACE CATALOG INTEGRATION iceberg_catalog
  CATALOG_SOURCE = 'OBJECT_STORE'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE
  COMMENT = 'Catalog integration for Iceberg Snowflake-managed tables';

-- Catalog Integration for Iceberg AWS Glue-managed
-- IMPORTANT: Using CATALOG_SOURCE = 'GLUE' (native integration) instead of ICEBERG_REST
-- This matches the working simple-glue-setup approach
CREATE OR REPLACE CATALOG INTEGRATION AWS_GLUE_CATALOG
  CATALOG_SOURCE = 'GLUE'
  CATALOG_NAMESPACE = 'iceberg_performance_test'
  GLUE_CATALOG_ID = '<your-aws-account-id>'
  GLUE_REGION = 'us-east-1'
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::<your-aws-account-id>:role/snowflake-iceberg-role'
  TABLE_FORMAT = 'ICEBERG'
  ENABLED = TRUE
  COMMENT = 'Native Glue catalog integration for Iceberg AWS Glue-managed tables';

-- Storage Integration for external tables (S3 access for stages/external tables)
CREATE OR REPLACE STORAGE INTEGRATION S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-aws-account-id>:role/snowflake-iceberg-role'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/tcp-ds-data/')
  COMMENT = 'Storage integration for external TPC-DS performance testing';

-- External stage for external tables
CREATE OR REPLACE STAGE TPCDS_EXTERNAL_FORMAT.TPCDS_EXTERNAL_STAGE
  URL = 's3://<your-bucket-name>/tcp-ds-data/'
  STORAGE_INTEGRATION = S3_INTEGRATION
  FILE_FORMAT = (TYPE = PARQUET);

-- Grant usage on stage to current role
GRANT USAGE ON STAGE TPCDS_EXTERNAL_FORMAT.TPCDS_EXTERNAL_STAGE TO ROLE PUBLIC;

SELECT 'Creating Native Snowflake TPC-DS tables...' as status;

-- Step 1: Create native format tables
SELECT 'Step 1: Creating native format TPC-DS tables...' as status;
@create_native.sql

-- Step 2: Create Iceberg Snowflake-managed tables
SELECT 'Step 2: Creating Iceberg Snowflake-managed TPC-DS tables...' as status;
@create_iceberg_sf.sql

-- Step 3: Create Iceberg AWS Glue-managed tables
 SELECT 'Step 3: Creating Iceberg AWS Glue-managed TPC-DS tables...' as status;
 @create_iceberg_glue.sql

-- Step 4: Create external tables
SELECT 'Step 4: Creating external TPC-DS tables...' as status;
@create_external.sql

-- Final status
SELECT 'All TPC-DS table creation scripts completed successfully!' as status;
SELECT 'Total tables created across all formats: 96 (24 tables × 4 formats)' as status;
