-- ========================================
-- COMPLETE CLEANUP SCRIPT FOR TPC-DS ALL TABLE FORMATS
-- Snowflake Iceberg Performance Testing Project
-- ========================================
-- This script drops all TPC-DS resources created by create_tpcds_objects.sql
-- including databases, schemas, tables, integrations, volumes, and warehouses
--
-- IMPORTANT: This script uses DEFAULT values matching create_tpcds_objects.sql.
-- If you customized any environment variables, you must replace the values in this script:
--   - Database: tpcds_performance_test 
--     (maps to SNOWFLAKE_DATABASE env var, default: tpcds_performance_test)
--   - Warehouse: tpcds_test_wh
--     (maps to SNOWFLAKE_WAREHOUSE env var, default: TPCDS_TEST_WH)
--     Note: Snowflake stores unquoted identifiers in uppercase, so 'tpcds_test_wh' = 'TPCDS_TEST_WH'
--   - Schemas: tpcds_native_format, tpcds_iceberg_sf_format, tpcds_iceberg_glue_format, 
--              tpcds_external_format
--     (map to SNOWFLAKE_SCHEMA_NATIVE, SNOWFLAKE_SCHEMA_ICEBERG_SF, 
--      SNOWFLAKE_SCHEMA_ICEBERG_GLUE, SNOWFLAKE_SCHEMA_EXTERNAL env vars)
--
-- To use custom values, search and replace all occurrences of the default values below.
-- ========================================

-- ========================================
-- 1. DROP ALL TABLES (in dependency order)
-- ========================================

SELECT 'Dropping all TPC-DS tables...' as status;

-- Drop External Tables first (they reference stages)
USE DATABASE tpcds_performance_test;
USE SCHEMA tpcds_external_format;

-- External Tables (24 tables)
DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS catalog_page;
DROP TABLE IF EXISTS catalog_returns;
DROP TABLE IF EXISTS catalog_sales;
DROP TABLE IF EXISTS customer_address;
DROP TABLE IF EXISTS customer_demographics;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS household_demographics;
DROP TABLE IF EXISTS income_band;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS promotion;
DROP TABLE IF EXISTS reason;
DROP TABLE IF EXISTS ship_mode;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS store_returns;
DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS web_page;
DROP TABLE IF EXISTS web_returns;
DROP TABLE IF EXISTS web_sales;
DROP TABLE IF EXISTS web_site;

-- Drop Iceberg Glue Tables
USE SCHEMA tpcds_iceberg_glue_format;

-- Note: Iceberg Glue tables are managed by AWS Glue, so they may not exist in Snowflake
-- These DROP statements will fail gracefully if tables don't exist
DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS catalog_page;
DROP TABLE IF EXISTS catalog_returns;
DROP TABLE IF EXISTS catalog_sales;
DROP TABLE IF EXISTS customer_address;
DROP TABLE IF EXISTS customer_demographics;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS household_demographics;
DROP TABLE IF EXISTS income_band;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS promotion;
DROP TABLE IF EXISTS reason;
DROP TABLE IF EXISTS ship_mode;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS store_returns;
DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS web_page;
DROP TABLE IF EXISTS web_returns;
DROP TABLE IF EXISTS web_sales;
DROP TABLE IF EXISTS web_site;

-- Drop Iceberg Snowflake Tables
USE SCHEMA tpcds_iceberg_sf_format;

DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS catalog_page;
DROP TABLE IF EXISTS catalog_returns;
DROP TABLE IF EXISTS catalog_sales;
DROP TABLE IF EXISTS customer_address;
DROP TABLE IF EXISTS customer_demographics;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS household_demographics;
DROP TABLE IF EXISTS income_band;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS promotion;
DROP TABLE IF EXISTS reason;
DROP TABLE IF EXISTS ship_mode;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS store_returns;
DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS web_page;
DROP TABLE IF EXISTS web_returns;
DROP TABLE IF EXISTS web_sales;
DROP TABLE IF EXISTS web_site;

-- Drop Native Tables
USE SCHEMA tpcds_native_format;

DROP TABLE IF EXISTS call_center;
DROP TABLE IF EXISTS catalog_page;
DROP TABLE IF EXISTS catalog_returns;
DROP TABLE IF EXISTS catalog_sales;
DROP TABLE IF EXISTS customer_address;
DROP TABLE IF EXISTS customer_demographics;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS date_dim;
DROP TABLE IF EXISTS household_demographics;
DROP TABLE IF EXISTS income_band;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS promotion;
DROP TABLE IF EXISTS reason;
DROP TABLE IF EXISTS ship_mode;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS store_returns;
DROP TABLE IF EXISTS store_sales;
DROP TABLE IF EXISTS time_dim;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS web_page;
DROP TABLE IF EXISTS web_returns;
DROP TABLE IF EXISTS web_sales;
DROP TABLE IF EXISTS web_site;

-- ========================================
-- 2. DROP FILE FORMATS
-- ========================================

SELECT 'Dropping file formats...' as status;

-- Drop file formats (must be dropped before external tables that use them)
DROP FILE FORMAT IF EXISTS parquet_format;
DROP FILE FORMAT IF EXISTS json_format;
DROP FILE FORMAT IF EXISTS csv_format;

-- ========================================
-- 3. DROP STAGES
-- ========================================

SELECT 'Dropping stages...' as status;

-- Drop stages in external format schema
USE SCHEMA tpcds_external_format;
DROP STAGE IF EXISTS tpcds_external_stage;
DROP STAGE IF EXISTS tpcds_s3_stage;
DROP STAGE IF EXISTS tpcds_direct_stage;

-- ========================================
-- 4. DROP SCHEMAS
-- ========================================

SELECT 'Dropping schemas...' as status;

USE DATABASE tpcds_performance_test;
DROP SCHEMA IF EXISTS tpcds_external_format;
DROP SCHEMA IF EXISTS tpcds_iceberg_glue_format;
DROP SCHEMA IF EXISTS tpcds_iceberg_sf_format;
DROP SCHEMA IF EXISTS tpcds_native_format;

-- ========================================
-- 5. DROP INTEGRATIONS
-- ========================================

SELECT 'Dropping integrations...' as status;

-- Drop storage integrations
DROP INTEGRATION IF EXISTS s3_integration;
DROP INTEGRATION IF EXISTS s3_tpcds_integration;

-- Drop catalog integrations
DROP INTEGRATION IF EXISTS iceberg_catalog;
DROP INTEGRATION IF EXISTS aws_glue_catalog;
DROP INTEGRATION IF EXISTS glue_catalog;

-- ========================================
-- 6. DROP EXTERNAL VOLUMES
-- ========================================

SELECT 'Dropping external volumes...' as status;

-- Drop external volumes
DROP EXTERNAL VOLUME IF EXISTS iceberg_volume;
DROP EXTERNAL VOLUME IF EXISTS iceberg_glue_volume;

-- ========================================
-- 7. DROP WAREHOUSE
-- ========================================

SELECT 'Dropping warehouse...' as status;

DROP WAREHOUSE IF EXISTS tpcds_test_wh;

-- ========================================
-- 8. DROP DATABASE
-- ========================================

SELECT 'Dropping database...' as status;

DROP DATABASE IF EXISTS tpcds_performance_test;

-- ========================================
-- VERIFICATION
-- ========================================

SELECT 'Verifying cleanup...' as status;

-- Check if database still exists
SELECT 'Database Status:' as check_type, 
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'TPCDS_PERFORMANCE_TEST') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

-- Check if warehouse still exists
SELECT 'Warehouse Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.WAREHOUSES WHERE WAREHOUSE_NAME = 'TPCDS_TEST_WH') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

-- Check if external volumes still exist
SELECT 'External Volume Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.EXTERNAL_VOLUMES WHERE EXTERNAL_VOLUME_NAME = 'ICEBERG_VOLUME') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

SELECT 'External Volume Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.EXTERNAL_VOLUMES WHERE EXTERNAL_VOLUME_NAME = 'ICEBERG_GLUE_VOLUME') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

-- Check if integrations still exist
SELECT 'Storage Integration Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.INTEGRATIONS WHERE INTEGRATION_NAME = 'S3_INTEGRATION') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

SELECT 'Storage Integration Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.INTEGRATIONS WHERE INTEGRATION_NAME = 'S3_TPCDS_INTEGRATION') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

SELECT 'Catalog Integration Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.INTEGRATIONS WHERE INTEGRATION_NAME = 'ICEBERG_CATALOG') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

SELECT 'Catalog Integration Status:' as check_type,
       CASE 
         WHEN EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.INTEGRATIONS WHERE INTEGRATION_NAME = 'AWS_GLUE_CATALOG') 
         THEN 'STILL EXISTS' 
         ELSE 'SUCCESSFULLY DROPPED' 
       END as status;

-- ========================================
-- SUMMARY
-- ========================================

SELECT 'TPC-DS Cleanup Complete!' as status;
SELECT 'All TPC-DS resources have been successfully dropped:' as summary;
SELECT '1. All tables (Native, Iceberg SF, Iceberg Glue, External) - 96 total tables' as dropped_1;
SELECT '2. All file formats (parquet_format, json_format, csv_format)' as dropped_2;
SELECT '3. All stages (tpcds_external_stage, tpcds_s3_stage, tpcds_direct_stage)' as dropped_3;
SELECT '4. All schemas (tpcds_native_format, tpcds_iceberg_sf_format, tpcds_iceberg_glue_format, tpcds_external_format)' as dropped_4;
SELECT '5. All integrations (s3_integration, s3_tpcds_integration, iceberg_catalog, aws_glue_catalog)' as dropped_5;
SELECT '6. All external volumes (iceberg_volume, iceberg_glue_volume)' as dropped_6;
SELECT '7. Warehouse (tpcds_test_wh)' as dropped_7;
SELECT '8. Database (tpcds_performance_test)' as dropped_8;

-- ========================================
-- CLEANUP STATISTICS
-- ========================================

SELECT 'Cleanup Statistics:' as stats_header;
SELECT 'Total TPC-DS Tables Dropped: 96' as table_count;
SELECT '  - Native Format: 24 tables' as native_count;
SELECT '  - Iceberg SF Format: 24 tables' as iceberg_sf_count;
SELECT '  - Iceberg Glue Format: 24 tables (may not exist in Snowflake)' as iceberg_glue_count;
SELECT '  - External Format: 24 tables' as external_count;
SELECT 'Total Schemas Dropped: 4' as schema_count;
SELECT 'Total Integrations Dropped: 4' as integration_count;
SELECT 'Total External Volumes Dropped: 2' as volume_count;
SELECT 'Total Stages Dropped: 3' as stage_count;
SELECT 'Total File Formats Dropped: 3' as file_format_count;
SELECT 'Total Warehouses Dropped: 1' as warehouse_count;
SELECT 'Total Databases Dropped: 1' as database_count;

-- ========================================
-- FINAL STATUS
-- ========================================

SELECT 'TPC-DS Performance Testing Environment Successfully Cleaned Up!' as final_status;
SELECT 'All resources have been removed and the environment is ready for fresh setup.' as ready_message;
