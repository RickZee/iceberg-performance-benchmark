-- ========================================
-- ICEBERG AWS GLUE-MANAGED TPC-DS TABLES
-- ========================================
-- This file contains the complete Iceberg AWS Glue-managed table definitions
-- for all TPC-DS tables
-- ========================================



-- Call Center Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.call_center
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'call_center'
COMMENT = 'Iceberg AWS Glue-managed call center table';

-- Catalog Page Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.catalog_page
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'catalog_page'
COMMENT = 'Iceberg AWS Glue-managed catalog page table';

-- Customer Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.customer
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'customer'
COMMENT = 'Iceberg AWS Glue-managed customer table';

-- Customer Address Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.customer_address
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'customer_address'
COMMENT = 'Iceberg AWS Glue-managed customer address table';

-- Customer Demographics Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.customer_demographics
EXTERNAL_VOLUME = 'ICEBERG_GLUE_VOLUME'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'customer_demographics'
COMMENT = 'Iceberg AWS Glue-managed customer demographics table';

-- Date Dimension Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.date_dim
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'date_dim'
COMMENT = 'Iceberg AWS Glue-managed date dimension table';

-- Household Demographics Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.household_demographics
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'household_demographics'
COMMENT = 'Iceberg AWS Glue-managed household demographics table';

-- Income Band Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.income_band
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'income_band'
COMMENT = 'Iceberg AWS Glue-managed income band table';

-- Item Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.item
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'item'
COMMENT = 'Iceberg AWS Glue-managed item table';

-- Promotion Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.promotion
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'promotion'
COMMENT = 'Iceberg AWS Glue-managed promotion table';

-- Reason Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.reason
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'reason'
COMMENT = 'Iceberg AWS Glue-managed reason table';

-- Ship Mode Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.ship_mode
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'ship_mode'
COMMENT = 'Iceberg AWS Glue-managed ship mode table';

-- Store Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.store
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'store'
COMMENT = 'Iceberg AWS Glue-managed store table';

-- Time Dimension Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.time_dim
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'time_dim'
COMMENT = 'Iceberg AWS Glue-managed time dimension table';

-- Warehouse Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.warehouse
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'warehouse'
COMMENT = 'Iceberg AWS Glue-managed warehouse table';

-- Web Page Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.web_page
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'web_page'
COMMENT = 'Iceberg AWS Glue-managed web page table';

-- Web Site Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.web_site
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'web_site'
COMMENT = 'Iceberg AWS Glue-managed web site table';

-- ========================================
-- FACT TABLES - ICEBERG GLUE FORMAT
-- ========================================

-- Catalog Returns Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.catalog_returns
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'catalog_returns'
COMMENT = 'Iceberg AWS Glue-managed catalog returns table';

-- Catalog Sales Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.catalog_sales
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'catalog_sales'
COMMENT = 'Iceberg AWS Glue-managed catalog sales table';

-- Inventory Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.inventory
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'inventory'
COMMENT = 'Iceberg AWS Glue-managed inventory table';

-- Store Returns Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.store_returns
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'store_returns'
COMMENT = 'Iceberg AWS Glue-managed store returns table';

-- Store Sales Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.store_sales
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'store_sales'
COMMENT = 'Iceberg AWS Glue-managed store sales table';

-- Web Returns Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.web_returns
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'web_returns'
COMMENT = 'Iceberg AWS Glue-managed web returns table';

-- Web Sales Iceberg Glue Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_glue_format.web_sales
EXTERNAL_VOLUME = \'ICEBERG_GLUE_VOLUME\'
CATALOG = 'AWS_GLUE_CATALOG'
CATALOG_TABLE_NAME = 'web_sales'
COMMENT = 'Iceberg AWS Glue-managed web sales table';
