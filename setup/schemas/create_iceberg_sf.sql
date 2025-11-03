-- ========================================
-- ICEBERG SNOWFLAKE-MANAGED TPC-DS TABLES
-- ========================================
-- This file contains the complete Iceberg Snowflake-managed table definitions
-- for all TPC-DS tables
-- ========================================



-- Call Center Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.call_center (
    cc_call_center_sk NUMBER(38,0) NOT NULL,
    cc_call_center_id STRING NOT NULL,
    cc_rec_start_date DATE,
    cc_rec_end_date DATE,
    cc_closed_date_sk NUMBER(38,0),
    cc_open_date_sk NUMBER(38,0),
    cc_name STRING,
    cc_class STRING,
    cc_employees NUMBER(38,0),
    cc_sq_ft NUMBER(38,0),
    cc_hours STRING,
    cc_manager STRING,
    cc_mkt_id NUMBER(38,0),
    cc_mkt_class STRING,
    cc_mkt_desc STRING,
    cc_market_manager STRING,
    cc_division NUMBER(38,0),
    cc_division_name STRING,
    cc_company NUMBER(38,0),
    cc_company_name STRING,
    cc_street_number STRING,
    cc_street_name STRING,
    cc_street_type STRING,
    cc_suite_number STRING,
    cc_city STRING,
    cc_county STRING,
    cc_state STRING,
    cc_zip STRING,
    cc_country STRING,
    cc_gmt_offset NUMBER(5,2),
    cc_tax_percentage NUMBER(5,2)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/call_center'
COMMENT = 'Iceberg Snowflake-managed call center table';

-- Catalog Page Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.catalog_page (
    cp_catalog_page_sk NUMBER(38,0) NOT NULL,
    cp_catalog_page_id STRING NOT NULL,
    cp_start_date_sk NUMBER(38,0),
    cp_end_date_sk NUMBER(38,0),
    cp_department STRING,
    cp_catalog_number NUMBER(38,0),
    cp_catalog_page_number NUMBER(38,0),
    cp_description STRING,
    cp_type STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/catalog_page'
COMMENT = 'Iceberg Snowflake-managed catalog page table';

-- Customer Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.customer (
    c_customer_sk NUMBER(38,0) NOT NULL,
    c_customer_id STRING NOT NULL,
    c_current_cdemo_sk NUMBER(38,0),
    c_current_hdemo_sk NUMBER(38,0),
    c_current_addr_sk NUMBER(38,0),
    c_first_shipto_date_sk NUMBER(38,0),
    c_first_sales_date_sk NUMBER(38,0),
    c_salutation STRING,
    c_first_name STRING,
    c_last_name STRING,
    c_preferred_cust_flag STRING,
    c_birth_day NUMBER(38,0),
    c_birth_month NUMBER(38,0),
    c_birth_year NUMBER(38,0),
    c_birth_country STRING,
    c_login STRING,
    c_email_address STRING,
    c_last_review_date_sk NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/customer'
COMMENT = 'Iceberg Snowflake-managed customer table';

-- Customer Address Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.customer_address (
    ca_address_sk NUMBER(38,0) NOT NULL,
    ca_address_id STRING NOT NULL,
    ca_street_number STRING,
    ca_street_name STRING,
    ca_street_type STRING,
    ca_suite_number STRING,
    ca_city STRING,
    ca_county STRING,
    ca_state STRING,
    ca_zip STRING,
    ca_country STRING,
    ca_gmt_offset NUMBER(5,2),
    ca_location_type STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/customer_address'
COMMENT = 'Iceberg Snowflake-managed customer address table';

-- Customer Demographics Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.customer_demographics (
    cd_demo_sk NUMBER(38,0) NOT NULL,
    cd_gender STRING,
    cd_marital_status STRING,
    cd_education_status STRING,
    cd_purchase_estimate NUMBER(38,0),
    cd_credit_rating STRING,
    cd_dep_count NUMBER(38,0),
    cd_dep_employed_count NUMBER(38,0),
    cd_dep_college_count NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/customer_demographics'
COMMENT = 'Iceberg Snowflake-managed customer demographics table';

-- Date Dimension Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.date_dim (
    d_date_sk NUMBER(38,0) NOT NULL,
    d_date_id STRING NOT NULL,
    d_date DATE,
    d_month_seq NUMBER(38,0),
    d_week_seq NUMBER(38,0),
    d_quarter_seq NUMBER(38,0),
    d_year NUMBER(38,0),
    d_dow NUMBER(38,0),
    d_moy NUMBER(38,0),
    d_dom NUMBER(38,0),
    d_qoy NUMBER(38,0),
    d_fy_year NUMBER(38,0),
    d_fy_quarter_seq NUMBER(38,0),
    d_fy_week_seq NUMBER(38,0),
    d_day_name STRING,
    d_quarter_name STRING,
    d_holiday STRING,
    d_weekend STRING,
    d_following_holiday STRING,
    d_first_dom NUMBER(38,0),
    d_last_dom NUMBER(38,0),
    d_same_day_ly NUMBER(38,0),
    d_same_day_lq NUMBER(38,0),
    d_current_day STRING,
    d_current_week STRING,
    d_current_month STRING,
    d_current_quarter STRING,
    d_current_year STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/date_dim'
COMMENT = 'Iceberg Snowflake-managed date dimension table';

-- Household Demographics Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.household_demographics (
    hd_demo_sk NUMBER(38,0) NOT NULL,
    hd_income_band_sk NUMBER(38,0),
    hd_buy_potential STRING,
    hd_dep_count NUMBER(38,0),
    hd_vehicle_count NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/household_demographics'
COMMENT = 'Iceberg Snowflake-managed household demographics table';

-- Income Band Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.income_band (
    ib_income_band_sk NUMBER(38,0) NOT NULL,
    ib_lower_bound NUMBER(38,0),
    ib_upper_bound NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/income_band'
COMMENT = 'Iceberg Snowflake-managed income band table';

-- Item Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.item (
    i_item_sk NUMBER(38,0) NOT NULL,
    i_item_id STRING NOT NULL,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc STRING,
    i_current_price NUMBER(7,2),
    i_wholesale_cost NUMBER(7,2),
    i_brand_id NUMBER(38,0),
    i_brand STRING,
    i_class_id NUMBER(38,0),
    i_class STRING,
    i_category_id NUMBER(38,0),
    i_category STRING,
    i_manufact_id NUMBER(38,0),
    i_manufact STRING,
    i_size STRING,
    i_formulation STRING,
    i_color STRING,
    i_units STRING,
    i_container STRING,
    i_manager_id NUMBER(38,0),
    i_product_name STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/item'
COMMENT = 'Iceberg Snowflake-managed item table';

-- Promotion Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.promotion (
    p_promo_sk NUMBER(38,0) NOT NULL,
    p_promo_id STRING NOT NULL,
    p_start_date_sk NUMBER(38,0),
    p_end_date_sk NUMBER(38,0),
    p_item_sk NUMBER(38,0),
    p_cost NUMBER(15,2),
    p_response_target NUMBER(38,0),
    p_promo_name STRING,
    p_channel_dmail STRING,
    p_channel_email STRING,
    p_channel_catalog STRING,
    p_channel_tv STRING,
    p_channel_radio STRING,
    p_channel_press STRING,
    p_channel_event STRING,
    p_channel_demo STRING,
    p_channel_details STRING,
    p_purpose STRING,
    p_discount_active STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/promotion'
COMMENT = 'Iceberg Snowflake-managed promotion table';

-- Reason Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.reason (
    r_reason_sk NUMBER(38,0) NOT NULL,
    r_reason_id STRING NOT NULL,
    r_reason_desc STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/reason'
COMMENT = 'Iceberg Snowflake-managed reason table';

-- Ship Mode Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.ship_mode (
    sm_ship_mode_sk NUMBER(38,0) NOT NULL,
    sm_ship_mode_id STRING NOT NULL,
    sm_type STRING,
    sm_code STRING,
    sm_carrier STRING,
    sm_contract STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/ship_mode'
COMMENT = 'Iceberg Snowflake-managed ship mode table';

-- Store Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.store (
    s_store_sk NUMBER(38,0) NOT NULL,
    s_store_id STRING NOT NULL,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk NUMBER(38,0),
    s_store_name STRING,
    s_number_employees NUMBER(38,0),
    s_floor_space NUMBER(38,0),
    s_hours STRING,
    s_manager STRING,
    s_market_id NUMBER(38,0),
    s_geography_class STRING,
    s_market_desc STRING,
    s_market_manager STRING,
    s_division_id NUMBER(38,0),
    s_division_name STRING,
    s_company_id NUMBER(38,0),
    s_company_name STRING,
    s_street_number STRING,
    s_street_name STRING,
    s_street_type STRING,
    s_suite_number STRING,
    s_city STRING,
    s_county STRING,
    s_state STRING,
    s_zip STRING,
    s_country STRING,
    s_gmt_offset NUMBER(5,2),
    s_tax_precentage NUMBER(5,2)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/store'
COMMENT = 'Iceberg Snowflake-managed store table';

-- Time Dimension Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.time_dim (
    t_time_sk NUMBER(38,0) NOT NULL,
    t_time_id STRING NOT NULL,
    t_time NUMBER(38,0),
    t_hour NUMBER(38,0),
    t_minute NUMBER(38,0),
    t_second NUMBER(38,0),
    t_am_pm STRING,
    t_shift STRING,
    t_sub_shift STRING,
    t_meal_time STRING
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/time_dim'
COMMENT = 'Iceberg Snowflake-managed time dimension table';

-- Warehouse Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.warehouse (
    w_warehouse_sk NUMBER(38,0) NOT NULL,
    w_warehouse_id STRING NOT NULL,
    w_warehouse_name STRING,
    w_warehouse_sq_ft NUMBER(38,0),
    w_street_number STRING,
    w_street_name STRING,
    w_street_type STRING,
    w_suite_number STRING,
    w_city STRING,
    w_county STRING,
    w_state STRING,
    w_zip STRING,
    w_country STRING,
    w_gmt_offset NUMBER(5,2)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/warehouse'
COMMENT = 'Iceberg Snowflake-managed warehouse table';

-- Web Page Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.web_page (
    wp_web_page_sk NUMBER(38,0) NOT NULL,
    wp_web_page_id STRING NOT NULL,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk NUMBER(38,0),
    wp_access_date_sk NUMBER(38,0),
    wp_autogen_flag STRING,
    wp_customer_sk NUMBER(38,0),
    wp_url STRING,
    wp_type STRING,
    wp_char_count NUMBER(38,0),
    wp_link_count NUMBER(38,0),
    wp_image_count NUMBER(38,0),
    wp_max_ad_count NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/web_page'
COMMENT = 'Iceberg Snowflake-managed web page table';

-- Web Site Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.web_site (
    web_site_sk NUMBER(38,0) NOT NULL,
    web_site_id STRING NOT NULL,
    web_rec_start_date DATE,
    web_rec_end_date DATE,
    web_name STRING,
    web_open_date_sk NUMBER(38,0),
    web_close_date_sk NUMBER(38,0),
    web_class STRING,
    web_manager STRING,
    web_mkt_id NUMBER(38,0),
    web_mkt_class STRING,
    web_mkt_desc STRING,
    web_market_manager STRING,
    web_company_id NUMBER(38,0),
    web_company_name STRING,
    web_street_number STRING,
    web_street_name STRING,
    web_street_type STRING,
    web_suite_number STRING,
    web_city STRING,
    web_county STRING,
    web_state STRING,
    web_zip STRING,
    web_country STRING,
    web_gmt_offset NUMBER(5,2),
    web_tax_percentage NUMBER(5,2)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/web_site'
COMMENT = 'Iceberg Snowflake-managed web site table';

-- ========================================
-- FACT TABLES - ICEBERG SNOWFLAKE FORMAT
-- ========================================

-- Catalog Returns Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.catalog_returns (
    cr_returned_date_sk NUMBER(38,0),
    cr_returned_time_sk NUMBER(38,0),
    cr_item_sk NUMBER(38,0),
    cr_refunded_customer_sk NUMBER(38,0),
    cr_refunded_cdemo_sk NUMBER(38,0),
    cr_refunded_hdemo_sk NUMBER(38,0),
    cr_refunded_addr_sk NUMBER(38,0),
    cr_returning_customer_sk NUMBER(38,0),
    cr_returning_cdemo_sk NUMBER(38,0),
    cr_returning_hdemo_sk NUMBER(38,0),
    cr_returning_addr_sk NUMBER(38,0),
    cr_call_center_sk NUMBER(38,0),
    cr_catalog_page_sk NUMBER(38,0),
    cr_ship_mode_sk NUMBER(38,0),
    cr_warehouse_sk NUMBER(38,0),
    cr_reason_sk NUMBER(38,0),
    cr_order_number NUMBER(38,0),
    cr_return_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/catalog_returns'
COMMENT = 'Iceberg Snowflake-managed catalog returns table';

-- Catalog Sales Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.catalog_sales (
    cs_sold_date_sk NUMBER(38,0),
    cs_sold_time_sk NUMBER(38,0),
    cs_ship_date_sk NUMBER(38,0),
    cs_bill_customer_sk NUMBER(38,0),
    cs_bill_cdemo_sk NUMBER(38,0),
    cs_bill_hdemo_sk NUMBER(38,0),
    cs_bill_addr_sk NUMBER(38,0),
    cs_ship_customer_sk NUMBER(38,0),
    cs_ship_cdemo_sk NUMBER(38,0),
    cs_ship_hdemo_sk NUMBER(38,0),
    cs_ship_addr_sk NUMBER(38,0),
    cs_call_center_sk NUMBER(38,0),
    cs_catalog_page_sk NUMBER(38,0),
    cs_ship_mode_sk NUMBER(38,0),
    cs_warehouse_sk NUMBER(38,0),
    cs_item_sk NUMBER(38,0),
    cs_promo_sk NUMBER(38,0),
    cs_order_number NUMBER(38,0),
    cs_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/catalog_sales'
COMMENT = 'Iceberg Snowflake-managed catalog sales table';

-- Inventory Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.inventory (
    inv_date_sk NUMBER(38,0) NOT NULL,
    inv_item_sk NUMBER(38,0) NOT NULL,
    inv_warehouse_sk NUMBER(38,0) NOT NULL,
    inv_quantity_on_hand NUMBER(38,0)
)
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/inventory'
COMMENT = 'Iceberg Snowflake-managed inventory table';

-- Store Returns Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.store_returns (
    sr_returned_date_sk NUMBER(38,0),
    sr_return_time_sk NUMBER(38,0),
    sr_item_sk NUMBER(38,0),
    sr_customer_sk NUMBER(38,0),
    sr_cdemo_sk NUMBER(38,0),
    sr_hdemo_sk NUMBER(38,0),
    sr_addr_sk NUMBER(38,0),
    sr_store_sk NUMBER(38,0),
    sr_reason_sk NUMBER(38,0),
    sr_ticket_number NUMBER(38,0),
    sr_return_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/store_returns'
COMMENT = 'Iceberg Snowflake-managed store returns table';

-- Store Sales Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.store_sales (
    ss_sold_date_sk NUMBER(38,0),
    ss_sold_time_sk NUMBER(38,0),
    ss_item_sk NUMBER(38,0),
    ss_customer_sk NUMBER(38,0),
    ss_cdemo_sk NUMBER(38,0),
    ss_hdemo_sk NUMBER(38,0),
    ss_addr_sk NUMBER(38,0),
    ss_store_sk NUMBER(38,0),
    ss_promo_sk NUMBER(38,0),
    ss_ticket_number NUMBER(38,0),
    ss_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/store_sales'
COMMENT = 'Iceberg Snowflake-managed store sales table';

-- Web Returns Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.web_returns (
    wr_returned_date_sk NUMBER(38,0),
    wr_returned_time_sk NUMBER(38,0),
    wr_item_sk NUMBER(38,0),
    wr_refunded_customer_sk NUMBER(38,0),
    wr_refunded_cdemo_sk NUMBER(38,0),
    wr_refunded_hdemo_sk NUMBER(38,0),
    wr_refunded_addr_sk NUMBER(38,0),
    wr_returning_customer_sk NUMBER(38,0),
    wr_returning_cdemo_sk NUMBER(38,0),
    wr_returning_hdemo_sk NUMBER(38,0),
    wr_returning_addr_sk NUMBER(38,0),
    wr_web_page_sk NUMBER(38,0),
    wr_reason_sk NUMBER(38,0),
    wr_order_number NUMBER(38,0),
    wr_return_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/web_returns'
COMMENT = 'Iceberg Snowflake-managed web returns table';

-- Web Sales Iceberg Table
CREATE OR REPLACE ICEBERG TABLE tpcds_performance_test.tpcds_iceberg_sf_format.web_sales (
    ws_sold_date_sk NUMBER(38,0),
    ws_sold_time_sk NUMBER(38,0),
    ws_ship_date_sk NUMBER(38,0),
    ws_item_sk NUMBER(38,0),
    ws_bill_customer_sk NUMBER(38,0),
    ws_bill_cdemo_sk NUMBER(38,0),
    ws_bill_hdemo_sk NUMBER(38,0),
    ws_bill_addr_sk NUMBER(38,0),
    ws_ship_customer_sk NUMBER(38,0),
    ws_ship_cdemo_sk NUMBER(38,0),
    ws_ship_hdemo_sk NUMBER(38,0),
    ws_ship_addr_sk NUMBER(38,0),
    ws_web_page_sk NUMBER(38,0),
    ws_web_site_sk NUMBER(38,0),
    ws_ship_mode_sk NUMBER(38,0),
    ws_warehouse_sk NUMBER(38,0),
    ws_promo_sk NUMBER(38,0),
    ws_order_number NUMBER(38,0),
    ws_quantity NUMBER(38,0),
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
EXTERNAL_VOLUME = 'iceberg_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'iceberg_sf/web_sales'
COMMENT = 'Iceberg Snowflake-managed web sales table';
