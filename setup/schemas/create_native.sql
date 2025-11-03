-- ========================================
-- 1. TPC-DS TABLES - NATIVE FORMAT
-- ========================================

-- Call Center Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.call_center (
    cc_call_center_sk NUMBER NOT NULL,
    cc_call_center_id VARCHAR(16) NOT NULL,
    cc_rec_start_date DATE,
    cc_rec_end_date DATE,
    cc_closed_date_sk NUMBER,
    cc_open_date_sk NUMBER,
    cc_name VARCHAR(50),
    cc_class VARCHAR(50),
    cc_employees NUMBER,
    cc_sq_ft NUMBER,
    cc_hours VARCHAR(20),
    cc_manager VARCHAR(40),
    cc_mkt_id NUMBER,
    cc_mkt_class VARCHAR(50),
    cc_mkt_desc VARCHAR(100),
    cc_market_manager VARCHAR(40),
    cc_division NUMBER,
    cc_division_name VARCHAR(50),
    cc_company NUMBER,
    cc_company_name VARCHAR(50),
    cc_street_number VARCHAR(10),
    cc_street_name VARCHAR(60),
    cc_street_type VARCHAR(15),
    cc_suite_number VARCHAR(10),
    cc_city VARCHAR(60),
    cc_county VARCHAR(30),
    cc_state VARCHAR(2),
    cc_zip VARCHAR(10),
    cc_country VARCHAR(20),
    cc_gmt_offset NUMBER(5,2),
    cc_tax_percentage NUMBER(5,2)
)
CLUSTER BY (cc_call_center_sk)
COMMENT = 'Call center dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.call_center ADD CONSTRAINT pk_call_center PRIMARY KEY (cc_call_center_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.call_center ADD SEARCH OPTIMIZATION;

-- Catalog Page Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.catalog_page (
    cp_catalog_page_sk NUMBER NOT NULL,
    cp_catalog_page_id VARCHAR(16) NOT NULL,
    cp_start_date_sk NUMBER,
    cp_end_date_sk NUMBER,
    cp_department VARCHAR(50),
    cp_catalog_number NUMBER,
    cp_catalog_page_number NUMBER,
    cp_description VARCHAR(100),
    cp_type VARCHAR(50)
)
CLUSTER BY (cp_catalog_page_sk)
COMMENT = 'Catalog page dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.catalog_page ADD CONSTRAINT pk_catalog_page PRIMARY KEY (cp_catalog_page_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.catalog_page ADD SEARCH OPTIMIZATION;

-- Customer Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.customer (
    c_customer_sk NUMBER NOT NULL,
    c_customer_id VARCHAR(16) NOT NULL,
    c_current_cdemo_sk NUMBER,
    c_current_hdemo_sk NUMBER,
    c_current_addr_sk NUMBER,
    c_first_shipto_date_sk NUMBER,
    c_first_sales_date_sk NUMBER,
    c_salutation VARCHAR(10),
    c_first_name VARCHAR(20),
    c_last_name VARCHAR(30),
    c_preferred_cust_flag VARCHAR(1),
    c_birth_day NUMBER,
    c_birth_month NUMBER,
    c_birth_year NUMBER,
    c_birth_country VARCHAR(20),
    c_login VARCHAR(13),
    c_email_address VARCHAR(50),
    c_last_review_date_sk NUMBER
)
CLUSTER BY (c_customer_sk)
COMMENT = 'Customer dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_customer_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.customer ADD SEARCH OPTIMIZATION;

-- Customer Address Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.customer_address (
    ca_address_sk NUMBER NOT NULL,
    ca_address_id VARCHAR(16) NOT NULL,
    ca_street_number VARCHAR(10),
    ca_street_name VARCHAR(60),
    ca_street_type VARCHAR(15),
    ca_suite_number VARCHAR(10),
    ca_city VARCHAR(60),
    ca_county VARCHAR(30),
    ca_state VARCHAR(2),
    ca_zip VARCHAR(10),
    ca_country VARCHAR(20),
    ca_gmt_offset NUMBER(5,2),
    ca_location_type VARCHAR(20)
)
CLUSTER BY (ca_address_sk)
COMMENT = 'Customer address dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.customer_address ADD CONSTRAINT pk_customer_address PRIMARY KEY (ca_address_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.customer_address ADD SEARCH OPTIMIZATION;

-- Customer Demographics Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.customer_demographics (
    cd_demo_sk NUMBER NOT NULL,
    cd_gender VARCHAR(1),
    cd_marital_status VARCHAR(1),
    cd_education_status VARCHAR(20),
    cd_purchase_estimate NUMBER,
    cd_credit_rating VARCHAR(10),
    cd_dep_count NUMBER,
    cd_dep_employed_count NUMBER,
    cd_dep_college_count NUMBER
)
CLUSTER BY (cd_demo_sk)
COMMENT = 'Customer demographics dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.customer_demographics ADD CONSTRAINT pk_customer_demographics PRIMARY KEY (cd_demo_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.customer_demographics ADD SEARCH OPTIMIZATION;

-- Date Dimension Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.date_dim (
    d_date_sk NUMBER NOT NULL,
    d_date_id VARCHAR(16) NOT NULL,
    d_date DATE,
    d_month_seq NUMBER,
    d_week_seq NUMBER,
    d_quarter_seq NUMBER,
    d_year NUMBER,
    d_dow NUMBER,
    d_moy NUMBER,
    d_dom NUMBER,
    d_qoy NUMBER,
    d_fy_year NUMBER,
    d_fy_quarter_seq NUMBER,
    d_fy_week_seq NUMBER,
    d_day_name VARCHAR(9),
    d_quarter_name VARCHAR(6),
    d_holiday VARCHAR(1),
    d_weekend VARCHAR(1),
    d_following_holiday VARCHAR(1),
    d_first_dom NUMBER,
    d_last_dom NUMBER,
    d_same_day_ly NUMBER,
    d_same_day_lq NUMBER,
    d_current_day VARCHAR(1),
    d_current_week VARCHAR(1),
    d_current_month VARCHAR(1),
    d_current_quarter VARCHAR(1),
    d_current_year VARCHAR(1)
)
CLUSTER BY (d_date_sk)
COMMENT = 'Date dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.date_dim ADD CONSTRAINT pk_date_dim PRIMARY KEY (d_date_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.date_dim ADD SEARCH OPTIMIZATION;

-- Household Demographics Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.household_demographics (
    hd_demo_sk NUMBER NOT NULL,
    hd_income_band_sk NUMBER,
    hd_buy_potential VARCHAR(15),
    hd_dep_count NUMBER,
    hd_vehicle_count NUMBER
)
CLUSTER BY (hd_demo_sk)
COMMENT = 'Household demographics dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.household_demographics ADD CONSTRAINT pk_household_demographics PRIMARY KEY (hd_demo_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.household_demographics ADD SEARCH OPTIMIZATION;

-- Income Band Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.income_band (
    ib_income_band_sk NUMBER NOT NULL,
    ib_lower_bound NUMBER,
    ib_upper_bound NUMBER
)
CLUSTER BY (ib_income_band_sk)
COMMENT = 'Income band dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.income_band ADD CONSTRAINT pk_income_band PRIMARY KEY (ib_income_band_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.income_band ADD SEARCH OPTIMIZATION;

-- Item Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.item (
    i_item_sk NUMBER NOT NULL,
    i_item_id VARCHAR(16) NOT NULL,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc VARCHAR(200),
    i_current_price NUMBER(7,2),
    i_wholesale_cost NUMBER(7,2),
    i_brand_id NUMBER,
    i_brand VARCHAR(50),
    i_class_id NUMBER,
    i_class VARCHAR(50),
    i_category_id NUMBER,
    i_category VARCHAR(50),
    i_manufact_id NUMBER,
    i_manufact VARCHAR(50),
    i_size VARCHAR(20),
    i_formulation VARCHAR(20),
    i_color VARCHAR(20),
    i_units VARCHAR(10),
    i_container VARCHAR(10),
    i_manager_id NUMBER,
    i_product_name VARCHAR(50)
)
CLUSTER BY (i_item_sk)
COMMENT = 'Item dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.item ADD CONSTRAINT pk_item PRIMARY KEY (i_item_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.item ADD SEARCH OPTIMIZATION;

-- Promotion Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.promotion (
    p_promo_sk NUMBER NOT NULL,
    p_promo_id VARCHAR(16) NOT NULL,
    p_start_date_sk NUMBER,
    p_end_date_sk NUMBER,
    p_item_sk NUMBER,
    p_cost NUMBER(15,2),
    p_response_target NUMBER,
    p_promo_name VARCHAR(50),
    p_channel_dmail VARCHAR(1),
    p_channel_email VARCHAR(1),
    p_channel_catalog VARCHAR(1),
    p_channel_tv VARCHAR(1),
    p_channel_radio VARCHAR(1),
    p_channel_press VARCHAR(1),
    p_channel_event VARCHAR(1),
    p_channel_demo VARCHAR(1),
    p_channel_details VARCHAR(100),
    p_purpose VARCHAR(15),
    p_discount_active VARCHAR(1)
)
CLUSTER BY (p_promo_sk)
COMMENT = 'Promotion dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.promotion ADD CONSTRAINT pk_promotion PRIMARY KEY (p_promo_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.promotion ADD SEARCH OPTIMIZATION;

-- Reason Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.reason (
    r_reason_sk NUMBER NOT NULL,
    r_reason_id VARCHAR(16) NOT NULL,
    r_reason_desc VARCHAR(100)
)
CLUSTER BY (r_reason_sk)
COMMENT = 'Reason dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.reason ADD CONSTRAINT pk_reason PRIMARY KEY (r_reason_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.reason ADD SEARCH OPTIMIZATION;

-- Ship Mode Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.ship_mode (
    sm_ship_mode_sk NUMBER NOT NULL,
    sm_ship_mode_id VARCHAR(16) NOT NULL,
    sm_type VARCHAR(30),
    sm_code VARCHAR(10),
    sm_carrier VARCHAR(20),
    sm_contract VARCHAR(20)
)
CLUSTER BY (sm_ship_mode_sk)
COMMENT = 'Ship mode dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.ship_mode ADD CONSTRAINT pk_ship_mode PRIMARY KEY (sm_ship_mode_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.ship_mode ADD SEARCH OPTIMIZATION;

-- Store Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.store (
    s_store_sk NUMBER NOT NULL,
    s_store_id VARCHAR(16) NOT NULL,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk NUMBER,
    s_store_name VARCHAR(50),
    s_number_employees NUMBER,
    s_floor_space NUMBER,
    s_hours VARCHAR(20),
    s_manager VARCHAR(40),
    s_market_id NUMBER,
    s_geography_class VARCHAR(100),
    s_market_desc VARCHAR(100),
    s_market_manager VARCHAR(40),
    s_division_id NUMBER,
    s_division_name VARCHAR(50),
    s_company_id NUMBER,
    s_company_name VARCHAR(50),
    s_street_number VARCHAR(10),
    s_street_name VARCHAR(60),
    s_street_type VARCHAR(15),
    s_suite_number VARCHAR(10),
    s_city VARCHAR(60),
    s_county VARCHAR(30),
    s_state VARCHAR(2),
    s_zip VARCHAR(10),
    s_country VARCHAR(20),
    s_gmt_offset NUMBER(5,2),
    s_tax_precentage NUMBER(5,2)
)
CLUSTER BY (s_store_sk)
COMMENT = 'Store dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.store ADD CONSTRAINT pk_store PRIMARY KEY (s_store_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.store ADD SEARCH OPTIMIZATION;

-- Time Dimension Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.time_dim (
    t_time_sk NUMBER NOT NULL,
    t_time_id VARCHAR(16) NOT NULL,
    t_time NUMBER,
    t_hour NUMBER,
    t_minute NUMBER,
    t_second NUMBER,
    t_am_pm VARCHAR(2),
    t_shift VARCHAR(20),
    t_sub_shift VARCHAR(20),
    t_meal_time VARCHAR(20)
)
CLUSTER BY (t_time_sk)
COMMENT = 'Time dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.time_dim ADD CONSTRAINT pk_time_dim PRIMARY KEY (t_time_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.time_dim ADD SEARCH OPTIMIZATION;

-- Warehouse Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.warehouse (
    w_warehouse_sk NUMBER NOT NULL,
    w_warehouse_id VARCHAR(16) NOT NULL,
    w_warehouse_name VARCHAR(20),
    w_warehouse_sq_ft NUMBER,
    w_street_number VARCHAR(10),
    w_street_name VARCHAR(60),
    w_street_type VARCHAR(15),
    w_suite_number VARCHAR(10),
    w_city VARCHAR(60),
    w_county VARCHAR(30),
    w_state VARCHAR(2),
    w_zip VARCHAR(10),
    w_country VARCHAR(20),
    w_gmt_offset NUMBER(5,2)
)
CLUSTER BY (w_warehouse_sk)
COMMENT = 'Warehouse dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.warehouse ADD CONSTRAINT pk_warehouse PRIMARY KEY (w_warehouse_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.warehouse ADD SEARCH OPTIMIZATION;

-- Web Page Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.web_page (
    wp_web_page_sk NUMBER NOT NULL,
    wp_web_page_id VARCHAR(16) NOT NULL,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk NUMBER,
    wp_access_date_sk NUMBER,
    wp_autogen_flag VARCHAR(1),
    wp_customer_sk NUMBER,
    wp_url VARCHAR(100),
    wp_type VARCHAR(50),
    wp_char_count NUMBER,
    wp_link_count NUMBER,
    wp_image_count NUMBER,
    wp_max_ad_count NUMBER
)
CLUSTER BY (wp_web_page_sk)
COMMENT = 'Web page dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.web_page ADD CONSTRAINT pk_web_page PRIMARY KEY (wp_web_page_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.web_page ADD SEARCH OPTIMIZATION;

-- Web Site Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.web_site (
    web_site_sk NUMBER NOT NULL,
    web_site_id VARCHAR(16) NOT NULL,
    web_rec_start_date DATE,
    web_rec_end_date DATE,
    web_name VARCHAR(50),
    web_open_date_sk NUMBER,
    web_close_date_sk NUMBER,
    web_class VARCHAR(50),
    web_manager VARCHAR(40),
    web_mkt_id NUMBER,
    web_mkt_class VARCHAR(50),
    web_mkt_desc VARCHAR(100),
    web_market_manager VARCHAR(40),
    web_company_id NUMBER,
    web_company_name VARCHAR(50),
    web_street_number VARCHAR(10),
    web_street_name VARCHAR(60),
    web_street_type VARCHAR(15),
    web_suite_number VARCHAR(10),
    web_city VARCHAR(60),
    web_county VARCHAR(30),
    web_state VARCHAR(2),
    web_zip VARCHAR(10),
    web_country VARCHAR(20),
    web_gmt_offset NUMBER(5,2),
    web_tax_percentage NUMBER(5,2)
)
CLUSTER BY (web_site_sk)
COMMENT = 'Web site dimension table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.web_site ADD CONSTRAINT pk_web_site PRIMARY KEY (web_site_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.web_site ADD SEARCH OPTIMIZATION;

-- ========================================
-- FACT TABLES - NATIVE FORMAT
-- ========================================

-- Catalog Returns Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.catalog_returns (
    cr_returned_date_sk NUMBER,
    cr_returned_time_sk NUMBER,
    cr_item_sk NUMBER,
    cr_refunded_customer_sk NUMBER,
    cr_refunded_cdemo_sk NUMBER,
    cr_refunded_hdemo_sk NUMBER,
    cr_refunded_addr_sk NUMBER,
    cr_returning_customer_sk NUMBER,
    cr_returning_cdemo_sk NUMBER,
    cr_returning_hdemo_sk NUMBER,
    cr_returning_addr_sk NUMBER,
    cr_call_center_sk NUMBER,
    cr_catalog_page_sk NUMBER,
    cr_ship_mode_sk NUMBER,
    cr_warehouse_sk NUMBER,
    cr_reason_sk NUMBER,
    cr_order_number NUMBER,
    cr_return_quantity NUMBER,
    cr_return_amount NUMBER(7,2),
    cr_return_tax NUMBER(7,2),
    cr_return_amt_inc_tax NUMBER(7,2),
    cr_fee NUMBER(7,2),
    cr_return_ship_cost NUMBER(7,2),
    cr_refunded_cash NUMBER(7,2),
    cr_reversed_charge NUMBER(7,2),
    cr_store_credit NUMBER(7,2),
    cr_net_loss NUMBER(7,2)
)
CLUSTER BY (cr_returned_date_sk, cr_item_sk)
COMMENT = 'Catalog returns fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.catalog_returns ADD SEARCH OPTIMIZATION;

-- Catalog Sales Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.catalog_sales (
    cs_sold_date_sk NUMBER,
    cs_sold_time_sk NUMBER,
    cs_ship_date_sk NUMBER,
    cs_bill_customer_sk NUMBER,
    cs_bill_cdemo_sk NUMBER,
    cs_bill_hdemo_sk NUMBER,
    cs_bill_addr_sk NUMBER,
    cs_ship_customer_sk NUMBER,
    cs_ship_cdemo_sk NUMBER,
    cs_ship_hdemo_sk NUMBER,
    cs_ship_addr_sk NUMBER,
    cs_call_center_sk NUMBER,
    cs_catalog_page_sk NUMBER,
    cs_ship_mode_sk NUMBER,
    cs_warehouse_sk NUMBER,
    cs_item_sk NUMBER,
    cs_promo_sk NUMBER,
    cs_order_number NUMBER,
    cs_quantity NUMBER,
    cs_wholesale_cost NUMBER(7,2),
    cs_list_price NUMBER(7,2),
    cs_sales_price NUMBER(7,2),
    cs_ext_discount_amt NUMBER(7,2),
    cs_ext_sales_price NUMBER(7,2),
    cs_ext_wholesale_cost NUMBER(7,2),
    cs_ext_list_price NUMBER(7,2),
    cs_ext_tax NUMBER(7,2),
    cs_coupon_amt NUMBER(7,2),
    cs_ext_ship_cost NUMBER(7,2),
    cs_net_paid NUMBER(7,2),
    cs_net_paid_inc_tax NUMBER(7,2),
    cs_net_paid_inc_ship NUMBER(7,2),
    cs_net_paid_inc_ship_tax NUMBER(7,2),
    cs_net_profit NUMBER(7,2)
)
CLUSTER BY (cs_sold_date_sk, cs_item_sk)
COMMENT = 'Catalog sales fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.catalog_sales ADD SEARCH OPTIMIZATION;

-- Inventory Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.inventory (
    inv_date_sk NUMBER NOT NULL,
    inv_item_sk NUMBER NOT NULL,
    inv_warehouse_sk NUMBER NOT NULL,
    inv_quantity_on_hand NUMBER
)
CLUSTER BY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
COMMENT = 'Inventory fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.inventory ADD CONSTRAINT pk_inventory PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk);
ALTER TABLE tpcds_performance_test.tpcds_native_format.inventory ADD SEARCH OPTIMIZATION;

-- Store Returns Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.store_returns (
    sr_returned_date_sk NUMBER,
    sr_return_time_sk NUMBER,
    sr_item_sk NUMBER,
    sr_customer_sk NUMBER,
    sr_cdemo_sk NUMBER,
    sr_hdemo_sk NUMBER,
    sr_addr_sk NUMBER,
    sr_store_sk NUMBER,
    sr_reason_sk NUMBER,
    sr_ticket_number NUMBER,
    sr_return_quantity NUMBER,
    sr_return_amt NUMBER(7,2),
    sr_return_tax NUMBER(7,2),
    sr_return_amt_inc_tax NUMBER(7,2),
    sr_fee NUMBER(7,2),
    sr_return_ship_cost NUMBER(7,2),
    sr_refunded_cash NUMBER(7,2),
    sr_reversed_charge NUMBER(7,2),
    sr_store_credit NUMBER(7,2),
    sr_net_loss NUMBER(7,2)
)
CLUSTER BY (sr_returned_date_sk, sr_item_sk)
COMMENT = 'Store returns fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.store_returns ADD SEARCH OPTIMIZATION;

-- Store Sales Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.store_sales (
    ss_sold_date_sk NUMBER,
    ss_sold_time_sk NUMBER,
    ss_item_sk NUMBER,
    ss_customer_sk NUMBER,
    ss_cdemo_sk NUMBER,
    ss_hdemo_sk NUMBER,
    ss_addr_sk NUMBER,
    ss_store_sk NUMBER,
    ss_promo_sk NUMBER,
    ss_ticket_number NUMBER,
    ss_quantity NUMBER,
    ss_wholesale_cost NUMBER(7,2),
    ss_list_price NUMBER(7,2),
    ss_sales_price NUMBER(7,2),
    ss_ext_discount_amt NUMBER(7,2),
    ss_ext_sales_price NUMBER(7,2),
    ss_ext_wholesale_cost NUMBER(7,2),
    ss_ext_list_price NUMBER(7,2),
    ss_ext_tax NUMBER(7,2),
    ss_coupon_amt NUMBER(7,2),
    ss_net_paid NUMBER(7,2),
    ss_net_paid_inc_tax NUMBER(7,2),
    ss_net_profit NUMBER(7,2)
)
CLUSTER BY (ss_sold_date_sk, ss_item_sk)
COMMENT = 'Store sales fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.store_sales ADD SEARCH OPTIMIZATION;

-- Web Returns Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.web_returns (
    wr_returned_date_sk NUMBER,
    wr_returned_time_sk NUMBER,
    wr_item_sk NUMBER,
    wr_refunded_customer_sk NUMBER,
    wr_refunded_cdemo_sk NUMBER,
    wr_refunded_hdemo_sk NUMBER,
    wr_refunded_addr_sk NUMBER,
    wr_returning_customer_sk NUMBER,
    wr_returning_cdemo_sk NUMBER,
    wr_returning_hdemo_sk NUMBER,
    wr_returning_addr_sk NUMBER,
    wr_web_page_sk NUMBER,
    wr_reason_sk NUMBER,
    wr_order_number NUMBER,
    wr_return_quantity NUMBER,
    wr_return_amt NUMBER(7,2),
    wr_return_tax NUMBER(7,2),
    wr_return_amt_inc_tax NUMBER(7,2),
    wr_fee NUMBER(7,2),
    wr_return_ship_cost NUMBER(7,2),
    wr_refunded_cash NUMBER(7,2),
    wr_reversed_charge NUMBER(7,2),
    wr_account_credit NUMBER(7,2),
    wr_net_loss NUMBER(7,2)
)
CLUSTER BY (wr_returned_date_sk, wr_item_sk)
COMMENT = 'Web returns fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.web_returns ADD SEARCH OPTIMIZATION;

-- Web Sales Table
CREATE OR REPLACE TABLE tpcds_performance_test.tpcds_native_format.web_sales (
    ws_sold_date_sk NUMBER,
    ws_sold_time_sk NUMBER,
    ws_ship_date_sk NUMBER,
    ws_item_sk NUMBER,
    ws_bill_customer_sk NUMBER,
    ws_bill_cdemo_sk NUMBER,
    ws_bill_hdemo_sk NUMBER,
    ws_bill_addr_sk NUMBER,
    ws_ship_customer_sk NUMBER,
    ws_ship_cdemo_sk NUMBER,
    ws_ship_hdemo_sk NUMBER,
    ws_ship_addr_sk NUMBER,
    ws_web_page_sk NUMBER,
    ws_web_site_sk NUMBER,
    ws_ship_mode_sk NUMBER,
    ws_warehouse_sk NUMBER,
    ws_promo_sk NUMBER,
    ws_order_number NUMBER,
    ws_quantity NUMBER,
    ws_wholesale_cost NUMBER(7,2),
    ws_list_price NUMBER(7,2),
    ws_sales_price NUMBER(7,2),
    ws_ext_discount_amt NUMBER(7,2),
    ws_ext_sales_price NUMBER(7,2),
    ws_ext_wholesale_cost NUMBER(7,2),
    ws_ext_list_price NUMBER(7,2),
    ws_ext_tax NUMBER(7,2),
    ws_coupon_amt NUMBER(7,2),
    ws_ext_ship_cost NUMBER(7,2),
    ws_net_paid NUMBER(7,2),
    ws_net_paid_inc_tax NUMBER(7,2),
    ws_net_paid_inc_ship NUMBER(7,2),
    ws_net_paid_inc_ship_tax NUMBER(7,2),
    ws_net_profit NUMBER(7,2)
)
CLUSTER BY (ws_sold_date_sk, ws_item_sk)
COMMENT = 'Web sales fact table';

ALTER TABLE tpcds_performance_test.tpcds_native_format.web_sales ADD SEARCH OPTIMIZATION;

-- ========================================
-- 2. TPC-DS TABLES - ICEBERG SNOWFLAKE FORMAT
-- ========================================

SELECT 'Creating Iceberg Snowflake-managed TPC-DS tables...' as status;




-- ========================================
-- 2. TPC-DS TABLES - ICEBERG SNOWFLAKE FORMAT
-- ========================================
-- Note: Complete Iceberg Snowflake-managed table definitions are in:
-- create_iceberg_sf.sql
-- Run that script after this one to create all Iceberg SF tables

-- ========================================
-- 3. TPC-DS TABLES - ICEBERG GLUE FORMAT
-- ========================================
-- Note: Complete Iceberg AWS Glue-managed table definitions are in:
-- create_iceberg_glue.sql
-- Run that script after this one to create all Iceberg Glue tables

-- ========================================
-- 4. TPC-DS TABLES - EXTERNAL FORMAT
-- ========================================
-- Note: Complete external table definitions are in:
-- create_external.sql
-- Run that script after this one to create all external tables

SELECT 'TPC-DS native table creation completed successfully!' as status;
SELECT 'Run the following scripts to create additional table formats:' as status;
SELECT '1. @create_iceberg_sf.sql - for Iceberg Snowflake-managed tables' as status;
SELECT '2. @create_iceberg_glue.sql - for Iceberg AWS Glue-managed tables' as status;
SELECT '3. @create_external.sql - for external tables' as status;
