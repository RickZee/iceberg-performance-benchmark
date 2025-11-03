-- ========================================
-- EXTERNAL TPC-DS TABLES
-- ========================================
-- This file contains the complete external table definitions
-- for all TPC-DS tables reading from S3 Parquet data files
-- ========================================



-- External Call Center Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.call_center (
    CC_CALL_CENTER_SK NUMBER AS ($1:cc_call_center_sk::NUMBER),
    CC_CALL_CENTER_ID VARCHAR AS ($1:cc_call_center_id::VARCHAR),
    CC_REC_START_DATE VARCHAR AS ($1:cc_rec_start_date::VARCHAR),
    CC_REC_END_DATE VARCHAR AS ($1:cc_rec_end_date::VARCHAR),
    CC_CLOSED_DATE_SK NUMBER AS ($1:cc_closed_date_sk::NUMBER),
    CC_OPEN_DATE_SK NUMBER AS ($1:cc_open_date_sk::NUMBER),
    CC_NAME VARCHAR AS ($1:cc_name::VARCHAR),
    CC_CLASS VARCHAR AS ($1:cc_class::VARCHAR),
    CC_EMPLOYEES NUMBER AS ($1:cc_employees::NUMBER),
    CC_SQ_FT NUMBER AS ($1:cc_sq_ft::NUMBER),
    CC_HOURS VARCHAR AS ($1:cc_hours::VARCHAR),
    CC_MANAGER VARCHAR AS ($1:cc_manager::VARCHAR),
    CC_MKT_ID NUMBER AS ($1:cc_mkt_id::NUMBER),
    CC_MKT_CLASS VARCHAR AS ($1:cc_mkt_class::VARCHAR),
    CC_MKT_DESC VARCHAR AS ($1:cc_mkt_desc::VARCHAR),
    CC_MARKET_MANAGER VARCHAR AS ($1:cc_market_manager::VARCHAR),
    CC_DIVISION NUMBER AS ($1:cc_division::NUMBER),
    CC_DIVISION_NAME VARCHAR AS ($1:cc_division_name::VARCHAR),
    CC_COMPANY NUMBER AS ($1:cc_company::NUMBER),
    CC_COMPANY_NAME VARCHAR AS ($1:cc_company_name::VARCHAR),
    CC_STREET_NUMBER VARCHAR AS ($1:cc_street_number::VARCHAR),
    CC_STREET_NAME VARCHAR AS ($1:cc_street_name::VARCHAR),
    CC_STREET_TYPE VARCHAR AS ($1:cc_street_type::VARCHAR),
    CC_SUITE_NUMBER VARCHAR AS ($1:cc_suite_number::VARCHAR),
    CC_CITY VARCHAR AS ($1:cc_city::VARCHAR),
    CC_COUNTY VARCHAR AS ($1:cc_county::VARCHAR),
    CC_STATE VARCHAR AS ($1:cc_state::VARCHAR),
    CC_ZIP VARCHAR AS ($1:cc_zip::VARCHAR),
    CC_COUNTRY VARCHAR AS ($1:cc_country::VARCHAR),
    CC_GMT_OFFSET NUMBER AS ($1:cc_gmt_offset::NUMBER),
    CC_TAX_PERCENTAGE NUMBER AS ($1:cc_tax_percentage::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/call_center_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External call center table reading from S3 Parquet data files';

-- External Catalog Page Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.catalog_page (
    CP_CATALOG_PAGE_SK NUMBER AS ($1:cp_catalog_page_sk::NUMBER),
    CP_CATALOG_PAGE_ID VARCHAR AS ($1:cp_catalog_page_id::VARCHAR),
    CP_START_DATE_SK NUMBER AS ($1:cp_start_date_sk::NUMBER),
    CP_END_DATE_SK NUMBER AS ($1:cp_end_date_sk::NUMBER),
    CP_DEPARTMENT VARCHAR AS ($1:cp_department::VARCHAR),
    CP_CATALOG_NUMBER NUMBER AS ($1:cp_catalog_number::NUMBER),
    CP_CATALOG_PAGE_NUMBER NUMBER AS ($1:cp_catalog_page_number::NUMBER),
    CP_DESCRIPTION VARCHAR AS ($1:cp_description::VARCHAR),
    CP_TYPE VARCHAR AS ($1:cp_type::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/catalog_page_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External catalog page table reading from S3 Parquet data files';

-- External Customer Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.customer (
    C_CUSTOMER_SK NUMBER AS ($1:c_customer_sk::NUMBER),
    C_CUSTOMER_ID VARCHAR AS ($1:c_customer_id::VARCHAR),
    C_CURRENT_CDEMO_SK NUMBER AS ($1:c_current_cdemo_sk::NUMBER),
    C_CURRENT_HDEMO_SK NUMBER AS ($1:c_current_hdemo_sk::NUMBER),
    C_CURRENT_ADDR_SK NUMBER AS ($1:c_current_addr_sk::NUMBER),
    C_FIRST_SHIPTO_DATE_SK NUMBER AS ($1:c_first_shipto_date_sk::NUMBER),
    C_FIRST_SALES_DATE_SK NUMBER AS ($1:c_first_sales_date_sk::NUMBER),
    C_SALUTATION VARCHAR AS ($1:c_salutation::VARCHAR),
    C_FIRST_NAME VARCHAR AS ($1:c_first_name::VARCHAR),
    C_LAST_NAME VARCHAR AS ($1:c_last_name::VARCHAR),
    C_PREFERRED_CUST_FLAG VARCHAR AS ($1:c_preferred_cust_flag::VARCHAR),
    C_BIRTH_DAY NUMBER AS ($1:c_birth_day::NUMBER),
    C_BIRTH_MONTH NUMBER AS ($1:c_birth_month::NUMBER),
    C_BIRTH_YEAR NUMBER AS ($1:c_birth_year::NUMBER),
    C_BIRTH_COUNTRY VARCHAR AS ($1:c_birth_country::VARCHAR),
    C_LOGIN VARCHAR AS ($1:c_login::VARCHAR),
    C_EMAIL_ADDRESS VARCHAR AS ($1:c_email_address::VARCHAR),
    C_LAST_REVIEW_DATE_SK NUMBER AS ($1:c_last_review_date_sk::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/customer_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External customer table reading from S3 Parquet data files';

-- External Customer Address Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.customer_address (
    CA_ADDRESS_SK NUMBER AS ($1:ca_address_sk::NUMBER),
    CA_ADDRESS_ID VARCHAR AS ($1:ca_address_id::VARCHAR),
    CA_STREET_NUMBER VARCHAR AS ($1:ca_street_number::VARCHAR),
    CA_STREET_NAME VARCHAR AS ($1:ca_street_name::VARCHAR),
    CA_STREET_TYPE VARCHAR AS ($1:ca_street_type::VARCHAR),
    CA_SUITE_NUMBER VARCHAR AS ($1:ca_suite_number::VARCHAR),
    CA_CITY VARCHAR AS ($1:ca_city::VARCHAR),
    CA_COUNTY VARCHAR AS ($1:ca_county::VARCHAR),
    CA_STATE VARCHAR AS ($1:ca_state::VARCHAR),
    CA_ZIP VARCHAR AS ($1:ca_zip::VARCHAR),
    CA_COUNTRY VARCHAR AS ($1:ca_country::VARCHAR),
    CA_GMT_OFFSET NUMBER AS ($1:ca_gmt_offset::NUMBER),
    CA_LOCATION_TYPE VARCHAR AS ($1:ca_location_type::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/customer_address_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External customer address table reading from S3 Parquet data files';

-- External Customer Demographics Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.customer_demographics (
    CD_DEMO_SK NUMBER AS ($1:cd_demo_sk::NUMBER),
    CD_GENDER VARCHAR AS ($1:cd_gender::VARCHAR),
    CD_MARITAL_STATUS VARCHAR AS ($1:cd_marital_status::VARCHAR),
    CD_EDUCATION_STATUS VARCHAR AS ($1:cd_education_status::VARCHAR),
    CD_PURCHASE_ESTIMATE NUMBER AS ($1:cd_purchase_estimate::NUMBER),
    CD_CREDIT_RATING VARCHAR AS ($1:cd_credit_rating::VARCHAR),
    CD_DEP_COUNT NUMBER AS ($1:cd_dep_count::NUMBER),
    CD_DEP_EMPLOYED_COUNT NUMBER AS ($1:cd_dep_employed_count::NUMBER),
    CD_DEP_COLLEGE_COUNT NUMBER AS ($1:cd_dep_college_count::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/customer_demographics_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External customer demographics table reading from S3 Parquet data files';

-- External Date Dimension Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.date_dim (
    D_DATE_SK NUMBER AS ($1:d_date_sk::NUMBER),
    D_DATE_ID VARCHAR AS ($1:d_date_id::VARCHAR),
    D_DATE VARCHAR AS ($1:d_date::VARCHAR),
    D_MONTH_SEQ NUMBER AS ($1:d_month_seq::NUMBER),
    D_WEEK_SEQ NUMBER AS ($1:d_week_seq::NUMBER),
    D_QUARTER_SEQ NUMBER AS ($1:d_quarter_seq::NUMBER),
    D_YEAR NUMBER AS ($1:d_year::NUMBER),
    D_DOW NUMBER AS ($1:d_dow::NUMBER),
    D_MOY NUMBER AS ($1:d_moy::NUMBER),
    D_DOM NUMBER AS ($1:d_dom::NUMBER),
    D_QOY NUMBER AS ($1:d_qoy::NUMBER),
    D_FY_YEAR NUMBER AS ($1:d_fy_year::NUMBER),
    D_FY_QUARTER_SEQ NUMBER AS ($1:d_fy_quarter_seq::NUMBER),
    D_FY_WEEK_SEQ NUMBER AS ($1:d_fy_week_seq::NUMBER),
    D_DAY_NAME VARCHAR AS ($1:d_day_name::VARCHAR),
    D_QUARTER_NAME VARCHAR AS ($1:d_quarter_name::VARCHAR),
    D_HOLIDAY VARCHAR AS ($1:d_holiday::VARCHAR),
    D_WEEKEND VARCHAR AS ($1:d_weekend::VARCHAR),
    D_FOLLOWING_HOLIDAY VARCHAR AS ($1:d_following_holiday::VARCHAR),
    D_FIRST_DOM NUMBER AS ($1:d_first_dom::NUMBER),
    D_LAST_DOM NUMBER AS ($1:d_last_dom::NUMBER),
    D_SAME_DAY_LY NUMBER AS ($1:d_same_day_ly::NUMBER),
    D_SAME_DAY_LQ NUMBER AS ($1:d_same_day_lq::NUMBER),
    D_CURRENT_DAY VARCHAR AS ($1:d_current_day::VARCHAR),
    D_CURRENT_WEEK VARCHAR AS ($1:d_current_week::VARCHAR),
    D_CURRENT_MONTH VARCHAR AS ($1:d_current_month::VARCHAR),
    D_CURRENT_QUARTER VARCHAR AS ($1:d_current_quarter::VARCHAR),
    D_CURRENT_YEAR VARCHAR AS ($1:d_current_year::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/date_dim_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External date dimension table reading from S3 Parquet data files';

-- External Household Demographics Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.household_demographics (
    HD_DEMO_SK NUMBER AS ($1:hd_demo_sk::NUMBER),
    HD_INCOME_BAND_SK NUMBER AS ($1:hd_income_band_sk::NUMBER),
    HD_BUY_POTENTIAL VARCHAR AS ($1:hd_buy_potential::VARCHAR),
    HD_DEP_COUNT NUMBER AS ($1:hd_dep_count::NUMBER),
    HD_VEHICLE_COUNT NUMBER AS ($1:hd_vehicle_count::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/household_demographics_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External household demographics table reading from S3 Parquet data files';

-- External Income Band Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.income_band (
    IB_INCOME_BAND_SK NUMBER AS ($1:ib_income_band_sk::NUMBER),
    IB_LOWER_BOUND NUMBER AS ($1:ib_lower_bound::NUMBER),
    IB_UPPER_BOUND NUMBER AS ($1:ib_upper_bound::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/income_band_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External income band table reading from S3 Parquet data files';

-- External Item Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.item (
    I_ITEM_SK NUMBER AS ($1:i_item_sk::NUMBER),
    I_ITEM_ID VARCHAR AS ($1:i_item_id::VARCHAR),
    I_REC_START_DATE VARCHAR AS ($1:i_rec_start_date::VARCHAR),
    I_REC_END_DATE VARCHAR AS ($1:i_rec_end_date::VARCHAR),
    I_ITEM_DESC VARCHAR AS ($1:i_item_desc::VARCHAR),
    I_CURRENT_PRICE NUMBER AS ($1:i_current_price::NUMBER),
    I_WHOLESALE_COST NUMBER AS ($1:i_wholesale_cost::NUMBER),
    I_BRAND_ID NUMBER AS ($1:i_brand_id::NUMBER),
    I_BRAND VARCHAR AS ($1:i_brand::VARCHAR),
    I_CLASS_ID NUMBER AS ($1:i_class_id::NUMBER),
    I_CLASS VARCHAR AS ($1:i_class::VARCHAR),
    I_CATEGORY_ID NUMBER AS ($1:i_category_id::NUMBER),
    I_CATEGORY VARCHAR AS ($1:i_category::VARCHAR),
    I_MANUFACT_ID NUMBER AS ($1:i_manufact_id::NUMBER),
    I_MANUFACT VARCHAR AS ($1:i_manufact::VARCHAR),
    I_SIZE VARCHAR AS ($1:i_size::VARCHAR),
    I_FORMULATION VARCHAR AS ($1:i_formulation::VARCHAR),
    I_COLOR VARCHAR AS ($1:i_color::VARCHAR),
    I_UNITS VARCHAR AS ($1:i_units::VARCHAR),
    I_CONTAINER VARCHAR AS ($1:i_container::VARCHAR),
    I_MANAGER_ID NUMBER AS ($1:i_manager_id::NUMBER),
    I_PRODUCT_NAME VARCHAR AS ($1:i_product_name::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/item_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External item table reading from S3 Parquet data files';

-- External Promotion Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.promotion (
    P_PROMO_SK NUMBER AS ($1:p_promo_sk::NUMBER),
    P_PROMO_ID VARCHAR AS ($1:p_promo_id::VARCHAR),
    P_START_DATE_SK NUMBER AS ($1:p_start_date_sk::NUMBER),
    P_END_DATE_SK NUMBER AS ($1:p_end_date_sk::NUMBER),
    P_ITEM_SK NUMBER AS ($1:p_item_sk::NUMBER),
    P_COST NUMBER AS ($1:p_cost::NUMBER),
    P_RESPONSE_TARGET NUMBER AS ($1:p_response_target::NUMBER),
    P_PROMO_NAME VARCHAR AS ($1:p_promo_name::VARCHAR),
    P_CHANNEL_DMAIL VARCHAR AS ($1:p_channel_dmail::VARCHAR),
    P_CHANNEL_EMAIL VARCHAR AS ($1:p_channel_email::VARCHAR),
    P_CHANNEL_CATALOG VARCHAR AS ($1:p_channel_catalog::VARCHAR),
    P_CHANNEL_TV VARCHAR AS ($1:p_channel_tv::VARCHAR),
    P_CHANNEL_RADIO VARCHAR AS ($1:p_channel_radio::VARCHAR),
    P_CHANNEL_PRESS VARCHAR AS ($1:p_channel_press::VARCHAR),
    P_CHANNEL_EVENT VARCHAR AS ($1:p_channel_event::VARCHAR),
    P_CHANNEL_DEMO VARCHAR AS ($1:p_channel_demo::VARCHAR),
    P_CHANNEL_DETAILS VARCHAR AS ($1:p_channel_details::VARCHAR),
    P_PURPOSE VARCHAR AS ($1:p_purpose::VARCHAR),
    P_DISCOUNT_ACTIVE VARCHAR AS ($1:p_discount_active::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/promotion_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External promotion table reading from S3 Parquet data files';

-- External Reason Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.reason (
    R_REASON_SK NUMBER AS ($1:r_reason_sk::NUMBER),
    R_REASON_ID VARCHAR AS ($1:r_reason_id::VARCHAR),
    R_REASON_DESC VARCHAR AS ($1:r_reason_desc::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/reason_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External reason table reading from S3 Parquet data files';

-- External Ship Mode Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.ship_mode (
    SM_SHIP_MODE_SK NUMBER AS ($1:sm_ship_mode_sk::NUMBER),
    SM_SHIP_MODE_ID VARCHAR AS ($1:sm_ship_mode_id::VARCHAR),
    SM_TYPE VARCHAR AS ($1:sm_type::VARCHAR),
    SM_CODE VARCHAR AS ($1:sm_code::VARCHAR),
    SM_CARRIER VARCHAR AS ($1:sm_carrier::VARCHAR),
    SM_CONTRACT VARCHAR AS ($1:sm_contract::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/ship_mode_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External ship mode table reading from S3 Parquet data files';

-- External Store Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.store (
    S_STORE_SK NUMBER AS ($1:s_store_sk::NUMBER),
    S_STORE_ID VARCHAR AS ($1:s_store_id::VARCHAR),
    S_REC_START_DATE VARCHAR AS ($1:s_rec_start_date::VARCHAR),
    S_REC_END_DATE VARCHAR AS ($1:s_rec_end_date::VARCHAR),
    S_CLOSED_DATE_SK NUMBER AS ($1:s_closed_date_sk::NUMBER),
    S_STORE_NAME VARCHAR AS ($1:s_store_name::VARCHAR),
    S_NUMBER_EMPLOYEES NUMBER AS ($1:s_number_employees::NUMBER),
    S_FLOOR_SPACE NUMBER AS ($1:s_floor_space::NUMBER),
    S_HOURS VARCHAR AS ($1:s_hours::VARCHAR),
    S_MANAGER VARCHAR AS ($1:s_manager::VARCHAR),
    S_MARKET_ID NUMBER AS ($1:s_market_id::NUMBER),
    S_GEOGRAPHY_CLASS VARCHAR AS ($1:s_geography_class::VARCHAR),
    S_MARKET_DESC VARCHAR AS ($1:s_market_desc::VARCHAR),
    S_MARKET_MANAGER VARCHAR AS ($1:s_market_manager::VARCHAR),
    S_DIVISION_ID NUMBER AS ($1:s_division_id::NUMBER),
    S_DIVISION_NAME VARCHAR AS ($1:s_division_name::VARCHAR),
    S_COMPANY_ID NUMBER AS ($1:s_company_id::NUMBER),
    S_COMPANY_NAME VARCHAR AS ($1:s_company_name::VARCHAR),
    S_STREET_NUMBER VARCHAR AS ($1:s_street_number::VARCHAR),
    S_STREET_NAME VARCHAR AS ($1:s_street_name::VARCHAR),
    S_STREET_TYPE VARCHAR AS ($1:s_street_type::VARCHAR),
    S_SUITE_NUMBER VARCHAR AS ($1:s_suite_number::VARCHAR),
    S_CITY VARCHAR AS ($1:s_city::VARCHAR),
    S_COUNTY VARCHAR AS ($1:s_county::VARCHAR),
    S_STATE VARCHAR AS ($1:s_state::VARCHAR),
    S_ZIP VARCHAR AS ($1:s_zip::VARCHAR),
    S_COUNTRY VARCHAR AS ($1:s_country::VARCHAR),
    S_GMT_OFFSET NUMBER AS ($1:s_gmt_offset::NUMBER),
    S_TAX_PRECENTAGE NUMBER AS ($1:S_TAX_PRECENTAGE::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/store_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External store table reading from S3 Parquet data files';

-- External Time Dimension Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.time_dim (
    T_TIME_SK NUMBER AS ($1:t_time_sk::NUMBER),
    T_TIME_ID VARCHAR AS ($1:t_time_id::VARCHAR),
    T_TIME NUMBER AS ($1:t_time::NUMBER),
    T_HOUR NUMBER AS ($1:t_hour::NUMBER),
    T_MINUTE NUMBER AS ($1:t_minute::NUMBER),
    T_SECOND NUMBER AS ($1:t_second::NUMBER),
    T_AM_PM VARCHAR AS ($1:t_am_pm::VARCHAR),
    T_SHIFT VARCHAR AS ($1:t_shift::VARCHAR),
    T_SUB_SHIFT VARCHAR AS ($1:t_sub_shift::VARCHAR),
    T_MEAL_TIME VARCHAR AS ($1:t_meal_time::VARCHAR)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/time_dim_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External time dimension table reading from S3 Parquet data files';

-- External Warehouse Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.warehouse (
    W_WAREHOUSE_SK NUMBER AS ($1:w_warehouse_sk::NUMBER),
    W_WAREHOUSE_ID VARCHAR AS ($1:w_warehouse_id::VARCHAR),
    W_WAREHOUSE_NAME VARCHAR AS ($1:w_warehouse_name::VARCHAR),
    W_WAREHOUSE_SQ_FT NUMBER AS ($1:w_warehouse_sq_ft::NUMBER),
    W_STREET_NUMBER VARCHAR AS ($1:w_street_number::VARCHAR),
    W_STREET_NAME VARCHAR AS ($1:w_street_name::VARCHAR),
    W_STREET_TYPE VARCHAR AS ($1:w_street_type::VARCHAR),
    W_SUITE_NUMBER VARCHAR AS ($1:w_suite_number::VARCHAR),
    W_CITY VARCHAR AS ($1:w_city::VARCHAR),
    W_COUNTY VARCHAR AS ($1:w_county::VARCHAR),
    W_STATE VARCHAR AS ($1:w_state::VARCHAR),
    W_ZIP VARCHAR AS ($1:w_zip::VARCHAR),
    W_COUNTRY VARCHAR AS ($1:w_country::VARCHAR),
    W_GMT_OFFSET NUMBER AS ($1:w_gmt_offset::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/warehouse_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External warehouse table reading from S3 Parquet data files';

-- External Web Page Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.web_page (
    WP_WEB_PAGE_SK NUMBER AS ($1:wp_web_page_sk::NUMBER),
    WP_WEB_PAGE_ID VARCHAR AS ($1:wp_web_page_id::VARCHAR),
    WP_REC_START_DATE VARCHAR AS ($1:wp_rec_start_date::VARCHAR),
    WP_REC_END_DATE VARCHAR AS ($1:wp_rec_end_date::VARCHAR),
    WP_CREATION_DATE_SK NUMBER AS ($1:wp_creation_date_sk::NUMBER),
    WP_ACCESS_DATE_SK NUMBER AS ($1:wp_access_date_sk::NUMBER),
    WP_AUTOGEN_FLAG VARCHAR AS ($1:wp_autogen_flag::VARCHAR),
    WP_CUSTOMER_SK NUMBER AS ($1:wp_customer_sk::NUMBER),
    WP_URL VARCHAR AS ($1:wp_url::VARCHAR),
    WP_TYPE VARCHAR AS ($1:wp_type::VARCHAR),
    WP_CHAR_COUNT NUMBER AS ($1:wp_char_count::NUMBER),
    WP_LINK_COUNT NUMBER AS ($1:wp_link_count::NUMBER),
    WP_IMAGE_COUNT NUMBER AS ($1:wp_image_count::NUMBER),
    WP_MAX_AD_COUNT NUMBER AS ($1:wp_max_ad_count::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/web_page_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External web page table reading from S3 Parquet data files';

-- External Web Site Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.web_site (
    WEB_SITE_SK NUMBER AS ($1:web_site_sk::NUMBER),
    WEB_SITE_ID VARCHAR AS ($1:web_site_id::VARCHAR),
    WEB_REC_START_DATE VARCHAR AS ($1:web_rec_start_date::VARCHAR),
    WEB_REC_END_DATE VARCHAR AS ($1:web_rec_end_date::VARCHAR),
    WEB_NAME VARCHAR AS ($1:web_name::VARCHAR),
    WEB_OPEN_DATE_SK NUMBER AS ($1:web_open_date_sk::NUMBER),
    WEB_CLOSE_DATE_SK NUMBER AS ($1:web_close_date_sk::NUMBER),
    WEB_CLASS VARCHAR AS ($1:web_class::VARCHAR),
    WEB_MANAGER VARCHAR AS ($1:web_manager::VARCHAR),
    WEB_MKT_ID NUMBER AS ($1:web_mkt_id::NUMBER),
    WEB_MKT_CLASS VARCHAR AS ($1:web_mkt_class::VARCHAR),
    WEB_MKT_DESC VARCHAR AS ($1:web_mkt_desc::VARCHAR),
    WEB_MARKET_MANAGER VARCHAR AS ($1:web_market_manager::VARCHAR),
    WEB_COMPANY_ID NUMBER AS ($1:WEB_COMPANY_ID::NUMBER),
    WEB_COMPANY_NAME VARCHAR AS ($1:WEB_COMPANY_NAME::VARCHAR),
    WEB_STREET_NUMBER VARCHAR AS ($1:WEB_STREET_NUMBER::VARCHAR),
    WEB_STREET_NAME VARCHAR AS ($1:WEB_STREET_NAME::VARCHAR),
    WEB_STREET_TYPE VARCHAR AS ($1:WEB_STREET_TYPE::VARCHAR),
    WEB_SUITE_NUMBER VARCHAR AS ($1:WEB_SUITE_NUMBER::VARCHAR),
    WEB_CITY VARCHAR AS ($1:WEB_CITY::VARCHAR),
    WEB_COUNTY VARCHAR AS ($1:WEB_COUNTY::VARCHAR),
    WEB_STATE VARCHAR AS ($1:WEB_STATE::VARCHAR),
    WEB_ZIP VARCHAR AS ($1:WEB_ZIP::VARCHAR),
    WEB_COUNTRY VARCHAR AS ($1:WEB_COUNTRY::VARCHAR),
    WEB_GMT_OFFSET NUMBER AS ($1:WEB_GMT_OFFSET::NUMBER),
    WEB_TAX_PERCENTAGE NUMBER AS ($1:WEB_TAX_PERCENTAGE::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/web_site_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External web site table reading from S3 Parquet data files';

-- ========================================
-- FACT TABLES - EXTERNAL FORMAT
-- ========================================

-- External Catalog Returns Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.catalog_returns (
    CR_RETURNED_DATE_SK NUMBER AS ($1:cr_returned_date_sk::NUMBER),
    CR_RETURNED_TIME_SK NUMBER AS ($1:cr_returned_time_sk::NUMBER),
    CR_ITEM_SK NUMBER AS ($1:cr_item_sk::NUMBER),
    CR_REFUNDED_CUSTOMER_SK NUMBER AS ($1:cr_refunded_customer_sk::NUMBER),
    CR_REFUNDED_CDEMO_SK NUMBER AS ($1:cr_refunded_cdemo_sk::NUMBER),
    CR_REFUNDED_HDEMO_SK NUMBER AS ($1:cr_refunded_hdemo_sk::NUMBER),
    CR_REFUNDED_ADDR_SK NUMBER AS ($1:cr_refunded_addr_sk::NUMBER),
    CR_RETURNING_CUSTOMER_SK NUMBER AS ($1:cr_returning_customer_sk::NUMBER),
    CR_RETURNING_CDEMO_SK NUMBER AS ($1:cr_returning_cdemo_sk::NUMBER),
    CR_RETURNING_HDEMO_SK NUMBER AS ($1:cr_returning_hdemo_sk::NUMBER),
    CR_RETURNING_ADDR_SK NUMBER AS ($1:cr_returning_addr_sk::NUMBER),
    CR_CALL_CENTER_SK NUMBER AS ($1:cr_call_center_sk::NUMBER),
    CR_CATALOG_PAGE_SK NUMBER AS ($1:cr_catalog_page_sk::NUMBER),
    CR_SHIP_MODE_SK NUMBER AS ($1:cr_ship_mode_sk::NUMBER),
    CR_WAREHOUSE_SK NUMBER AS ($1:cr_warehouse_sk::NUMBER),
    CR_REASON_SK NUMBER AS ($1:cr_reason_sk::NUMBER),
    CR_ORDER_NUMBER NUMBER AS ($1:cr_order_number::NUMBER),
    CR_RETURN_QUANTITY NUMBER AS ($1:cr_return_quantity::NUMBER),
    CR_RETURN_AMOUNT NUMBER AS ($1:CR_RETURN_AMOUNT::NUMBER),
    CR_RETURN_TAX NUMBER AS ($1:cr_return_tax::NUMBER),
    CR_RETURN_AMT_INC_TAX NUMBER AS ($1:cr_return_amt_inc_tax::NUMBER),
    CR_FEE NUMBER AS ($1:cr_fee::NUMBER),
    CR_RETURN_SHIP_COST NUMBER AS ($1:cr_return_ship_cost::NUMBER),
    CR_REFUNDED_CASH NUMBER AS ($1:cr_refunded_cash::NUMBER),
    CR_REVERSED_CHARGE NUMBER AS ($1:cr_reversed_charge::NUMBER),
    CR_STORE_CREDIT NUMBER AS ($1:cr_store_credit::NUMBER),
    CR_NET_LOSS NUMBER AS ($1:cr_net_loss::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/catalog_returns_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External catalog returns table reading from S3 Parquet data files';

-- External Catalog Sales Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.catalog_sales (
    CS_SOLD_DATE_SK NUMBER AS ($1:cs_sold_date_sk::NUMBER),
    CS_SOLD_TIME_SK NUMBER AS ($1:cs_sold_time_sk::NUMBER),
    CS_SHIP_DATE_SK NUMBER AS ($1:cs_ship_date_sk::NUMBER),
    CS_BILL_CUSTOMER_SK NUMBER AS ($1:cs_bill_customer_sk::NUMBER),
    CS_BILL_CDEMO_SK NUMBER AS ($1:cs_bill_cdemo_sk::NUMBER),
    CS_BILL_HDEMO_SK NUMBER AS ($1:cs_bill_hdemo_sk::NUMBER),
    CS_BILL_ADDR_SK NUMBER AS ($1:cs_bill_addr_sk::NUMBER),
    CS_SHIP_CUSTOMER_SK NUMBER AS ($1:cs_ship_customer_sk::NUMBER),
    CS_SHIP_CDEMO_SK NUMBER AS ($1:cs_ship_cdemo_sk::NUMBER),
    CS_SHIP_HDEMO_SK NUMBER AS ($1:cs_ship_hdemo_sk::NUMBER),
    CS_SHIP_ADDR_SK NUMBER AS ($1:cs_ship_addr_sk::NUMBER),
    CS_CALL_CENTER_SK NUMBER AS ($1:cs_call_center_sk::NUMBER),
    CS_CATALOG_PAGE_SK NUMBER AS ($1:cs_catalog_page_sk::NUMBER),
    CS_SHIP_MODE_SK NUMBER AS ($1:cs_ship_mode_sk::NUMBER),
    CS_WAREHOUSE_SK NUMBER AS ($1:cs_warehouse_sk::NUMBER),
    CS_ITEM_SK NUMBER AS ($1:cs_item_sk::NUMBER),
    CS_PROMO_SK NUMBER AS ($1:cs_promo_sk::NUMBER),
    CS_ORDER_NUMBER NUMBER AS ($1:cs_order_number::NUMBER),
    CS_QUANTITY NUMBER AS ($1:cs_quantity::NUMBER),
    CS_WHOLESALE_COST NUMBER AS ($1:cs_wholesale_cost::NUMBER),
    CS_LIST_PRICE NUMBER AS ($1:cs_list_price::NUMBER),
    CS_SALES_PRICE NUMBER AS ($1:cs_sales_price::NUMBER),
    CS_EXT_DISCOUNT_AMT NUMBER AS ($1:cs_ext_discount_amt::NUMBER),
    CS_EXT_SALES_PRICE NUMBER AS ($1:cs_ext_sales_price::NUMBER),
    CS_EXT_WHOLESALE_COST NUMBER AS ($1:cs_ext_wholesale_cost::NUMBER),
    CS_EXT_LIST_PRICE NUMBER AS ($1:cs_ext_list_price::NUMBER),
    CS_EXT_TAX NUMBER AS ($1:cs_ext_tax::NUMBER),
    CS_COUPON_AMT NUMBER AS ($1:cs_coupon_amt::NUMBER),
    CS_EXT_SHIP_COST NUMBER AS ($1:cs_ext_ship_cost::NUMBER),
    CS_NET_PAID NUMBER AS ($1:cs_net_paid::NUMBER),
    CS_NET_PAID_INC_TAX NUMBER AS ($1:cs_net_paid_inc_tax::NUMBER),
    CS_NET_PAID_INC_SHIP NUMBER AS ($1:cs_net_paid_inc_ship::NUMBER),
    CS_NET_PAID_INC_SHIP_TAX NUMBER AS ($1:cs_net_paid_inc_ship_tax::NUMBER),
    CS_NET_PROFIT NUMBER AS ($1:cs_net_profit::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/catalog_sales_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External catalog sales table reading from S3 Parquet data files';

-- External Inventory Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.inventory (
    INV_DATE_SK NUMBER AS ($1:inv_date_sk::NUMBER),
    INV_ITEM_SK NUMBER AS ($1:inv_item_sk::NUMBER),
    INV_WAREHOUSE_SK NUMBER AS ($1:inv_warehouse_sk::NUMBER),
    INV_QUANTITY_ON_HAND NUMBER AS ($1:inv_quantity_on_hand::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/inventory_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External inventory table reading from S3 Parquet data files';

-- External Store Returns Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.store_returns (
    SR_RETURNED_DATE_SK NUMBER AS ($1:sr_returned_date_sk::NUMBER),
    SR_RETURN_TIME_SK NUMBER AS ($1:sr_return_time_sk::NUMBER),
    SR_ITEM_SK NUMBER AS ($1:sr_item_sk::NUMBER),
    SR_CUSTOMER_SK NUMBER AS ($1:sr_customer_sk::NUMBER),
    SR_CDEMO_SK NUMBER AS ($1:sr_cdemo_sk::NUMBER),
    SR_HDEMO_SK NUMBER AS ($1:sr_hdemo_sk::NUMBER),
    SR_ADDR_SK NUMBER AS ($1:sr_addr_sk::NUMBER),
    SR_STORE_SK NUMBER AS ($1:sr_store_sk::NUMBER),
    SR_REASON_SK NUMBER AS ($1:sr_reason_sk::NUMBER),
    SR_TICKET_NUMBER NUMBER AS ($1:sr_ticket_number::NUMBER),
    SR_RETURN_QUANTITY NUMBER AS ($1:sr_return_quantity::NUMBER),
    SR_RETURN_AMT NUMBER AS ($1:sr_return_amt::NUMBER),
    SR_RETURN_TAX NUMBER AS ($1:sr_return_tax::NUMBER),
    SR_RETURN_AMT_INC_TAX NUMBER AS ($1:sr_return_amt_inc_tax::NUMBER),
    SR_FEE NUMBER AS ($1:sr_fee::NUMBER),
    SR_RETURN_SHIP_COST NUMBER AS ($1:sr_return_ship_cost::NUMBER),
    SR_REFUNDED_CASH NUMBER AS ($1:sr_refunded_cash::NUMBER),
    SR_REVERSED_CHARGE NUMBER AS ($1:sr_reversed_charge::NUMBER),
    SR_STORE_CREDIT NUMBER AS ($1:sr_store_credit::NUMBER),
    SR_NET_LOSS NUMBER AS ($1:sr_net_loss::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/store_returns_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External store returns table reading from S3 Parquet data files';

-- External Store Sales Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.store_sales (
    SS_SOLD_DATE_SK NUMBER AS ($1:ss_sold_date_sk::NUMBER),
    SS_SOLD_TIME_SK NUMBER AS ($1:ss_sold_time_sk::NUMBER),
    SS_ITEM_SK NUMBER AS ($1:ss_item_sk::NUMBER),
    SS_CUSTOMER_SK NUMBER AS ($1:ss_customer_sk::NUMBER),
    SS_CDEMO_SK NUMBER AS ($1:ss_cdemo_sk::NUMBER),
    SS_HDEMO_SK NUMBER AS ($1:ss_hdemo_sk::NUMBER),
    SS_ADDR_SK NUMBER AS ($1:ss_addr_sk::NUMBER),
    SS_STORE_SK NUMBER AS ($1:ss_store_sk::NUMBER),
    SS_PROMO_SK NUMBER AS ($1:ss_promo_sk::NUMBER),
    SS_TICKET_NUMBER NUMBER AS ($1:ss_ticket_number::NUMBER),
    SS_QUANTITY NUMBER AS ($1:ss_quantity::NUMBER),
    SS_WHOLESALE_COST NUMBER AS ($1:ss_wholesale_cost::NUMBER),
    SS_LIST_PRICE NUMBER AS ($1:ss_list_price::NUMBER),
    SS_SALES_PRICE NUMBER AS ($1:ss_sales_price::NUMBER),
    SS_EXT_DISCOUNT_AMT NUMBER AS ($1:ss_ext_discount_amt::NUMBER),
    SS_EXT_SALES_PRICE NUMBER AS ($1:ss_ext_sales_price::NUMBER),
    SS_EXT_WHOLESALE_COST NUMBER AS ($1:ss_ext_wholesale_cost::NUMBER),
    SS_EXT_LIST_PRICE NUMBER AS ($1:ss_ext_list_price::NUMBER),
    SS_EXT_TAX NUMBER AS ($1:ss_ext_tax::NUMBER),
    SS_COUPON_AMT NUMBER AS ($1:ss_coupon_amt::NUMBER),
    SS_NET_PAID NUMBER AS ($1:ss_net_paid::NUMBER),
    SS_NET_PAID_INC_TAX NUMBER AS ($1:ss_net_paid_inc_tax::NUMBER),
    SS_NET_PROFIT NUMBER AS ($1:ss_net_profit::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/store_sales_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External store sales table reading from S3 Parquet data files';

-- External Web Returns Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.web_returns (
    WR_RETURNED_DATE_SK NUMBER AS ($1:wr_returned_date_sk::NUMBER),
    WR_RETURNED_TIME_SK NUMBER AS ($1:wr_returned_time_sk::NUMBER),
    WR_ITEM_SK NUMBER AS ($1:wr_item_sk::NUMBER),
    WR_REFUNDED_CUSTOMER_SK NUMBER AS ($1:wr_refunded_customer_sk::NUMBER),
    WR_REFUNDED_CDEMO_SK NUMBER AS ($1:wr_refunded_cdemo_sk::NUMBER),
    WR_REFUNDED_HDEMO_SK NUMBER AS ($1:wr_refunded_hdemo_sk::NUMBER),
    WR_REFUNDED_ADDR_SK NUMBER AS ($1:wr_refunded_addr_sk::NUMBER),
    WR_RETURNING_CUSTOMER_SK NUMBER AS ($1:wr_returning_customer_sk::NUMBER),
    WR_RETURNING_CDEMO_SK NUMBER AS ($1:wr_returning_cdemo_sk::NUMBER),
    WR_RETURNING_HDEMO_SK NUMBER AS ($1:wr_returning_hdemo_sk::NUMBER),
    WR_RETURNING_ADDR_SK NUMBER AS ($1:wr_returning_addr_sk::NUMBER),
    WR_WEB_PAGE_SK NUMBER AS ($1:wr_web_page_sk::NUMBER),
    WR_REASON_SK NUMBER AS ($1:WR_REASON_SK::NUMBER),
    WR_ORDER_NUMBER NUMBER AS ($1:wr_order_number::NUMBER),
    WR_RETURN_QUANTITY NUMBER AS ($1:wr_return_quantity::NUMBER),
    WR_RETURN_AMT NUMBER AS ($1:wr_return_amt::NUMBER),
    WR_RETURN_TAX NUMBER AS ($1:wr_return_tax::NUMBER),
    WR_RETURN_AMT_INC_TAX NUMBER AS ($1:wr_return_amt_inc_tax::NUMBER),
    WR_FEE NUMBER AS ($1:wr_fee::NUMBER),
    WR_RETURN_SHIP_COST NUMBER AS ($1:wr_return_ship_cost::NUMBER),
    WR_REFUNDED_CASH NUMBER AS ($1:wr_refunded_cash::NUMBER),
    WR_REVERSED_CHARGE NUMBER AS ($1:wr_reversed_charge::NUMBER),
    WR_ACCOUNT_CREDIT NUMBER AS ($1:wr_account_credit::NUMBER),
    WR_NET_LOSS NUMBER AS ($1:wr_net_loss::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/web_returns_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External web returns table reading from S3 Parquet data files';

-- External Web Sales Table
CREATE OR REPLACE EXTERNAL TABLE tpcds_performance_test.tpcds_external_format.web_sales (
    WS_SOLD_DATE_SK NUMBER AS ($1:ws_sold_date_sk::NUMBER),
    WS_SOLD_TIME_SK NUMBER AS ($1:ws_sold_time_sk::NUMBER),
    WS_SHIP_DATE_SK NUMBER AS ($1:ws_ship_date_sk::NUMBER),
    WS_ITEM_SK NUMBER AS ($1:ws_item_sk::NUMBER),
    WS_BILL_CUSTOMER_SK NUMBER AS ($1:ws_bill_customer_sk::NUMBER),
    WS_BILL_CDEMO_SK NUMBER AS ($1:ws_bill_cdemo_sk::NUMBER),
    WS_BILL_HDEMO_SK NUMBER AS ($1:ws_bill_hdemo_sk::NUMBER),
    WS_BILL_ADDR_SK NUMBER AS ($1:ws_bill_addr_sk::NUMBER),
    WS_SHIP_CUSTOMER_SK NUMBER AS ($1:ws_ship_customer_sk::NUMBER),
    WS_SHIP_CDEMO_SK NUMBER AS ($1:ws_ship_cdemo_sk::NUMBER),
    WS_SHIP_HDEMO_SK NUMBER AS ($1:ws_ship_hdemo_sk::NUMBER),
    WS_SHIP_ADDR_SK NUMBER AS ($1:ws_ship_addr_sk::NUMBER),
    WS_WEB_PAGE_SK NUMBER AS ($1:ws_web_page_sk::NUMBER),
    WS_WEB_SITE_SK NUMBER AS ($1:ws_web_site_sk::NUMBER),
    WS_SHIP_MODE_SK NUMBER AS ($1:ws_ship_mode_sk::NUMBER),
    WS_WAREHOUSE_SK NUMBER AS ($1:ws_warehouse_sk::NUMBER),
    WS_PROMO_SK NUMBER AS ($1:ws_promo_sk::NUMBER),
    WS_ORDER_NUMBER NUMBER AS ($1:ws_order_number::NUMBER),
    WS_QUANTITY NUMBER AS ($1:ws_quantity::NUMBER),
    WS_WHOLESALE_COST NUMBER AS ($1:ws_wholesale_cost::NUMBER),
    WS_LIST_PRICE NUMBER AS ($1:ws_list_price::NUMBER),
    WS_SALES_PRICE NUMBER AS ($1:ws_sales_price::NUMBER),
    WS_EXT_DISCOUNT_AMT NUMBER AS ($1:ws_ext_discount_amt::NUMBER),
    WS_EXT_SALES_PRICE NUMBER AS ($1:ws_ext_sales_price::NUMBER),
    WS_EXT_WHOLESALE_COST NUMBER AS ($1:ws_ext_wholesale_cost::NUMBER),
    WS_EXT_LIST_PRICE NUMBER AS ($1:ws_ext_list_price::NUMBER),
    WS_EXT_TAX NUMBER AS ($1:ws_ext_tax::NUMBER),
    WS_COUPON_AMT NUMBER AS ($1:ws_coupon_amt::NUMBER),
    WS_EXT_SHIP_COST NUMBER AS ($1:ws_ext_ship_cost::NUMBER),
    WS_NET_PAID NUMBER AS ($1:ws_net_paid::NUMBER),
    WS_NET_PAID_INC_TAX NUMBER AS ($1:ws_net_paid_inc_tax::NUMBER),
    WS_NET_PAID_INC_SHIP NUMBER AS ($1:ws_net_paid_inc_ship::NUMBER),
    WS_NET_PAID_INC_SHIP_TAX NUMBER AS ($1:ws_net_paid_inc_ship_tax::NUMBER),
    WS_NET_PROFIT NUMBER AS ($1:ws_net_profit::NUMBER)
)
WITH LOCATION = @tpcds_performance_test.tpcds_external_format.tpcds_external_stage/tpcds_external_format/web_sales_external/
FILE_FORMAT = (TYPE = 'PARQUET')
AUTO_REFRESH = TRUE
COMMENT = 'External web sales table reading from S3 Parquet data files';
