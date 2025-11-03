#!/usr/bin/env python3
"""
AWS Glue ETL Job to Create All TPC-DS Iceberg Tables with Metadata
This script creates all 24 Glue-managed Iceberg tables using AWS Glue's native Iceberg support
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Glue job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'GLUE_DATABASE',
    'S3_WAREHOUSE_PATH',
    'AWS_REGION'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("=" * 70)
print("CREATING ALL TPC-DS ICEBERG TABLES WITH AWS GLUE ETL")
print("=" * 70)

# Configuration
glue_database = args['GLUE_DATABASE']
s3_warehouse_path = args['S3_WAREHOUSE_PATH']
aws_region = args.get('AWS_REGION', 'us-east-1')

print(f"Glue Database: {glue_database}")
print(f"S3 Warehouse Path: {s3_warehouse_path}")
print(f"AWS Region: {aws_region}")

# Configure Iceberg catalog for Glue
# Glue 4.0 has native Iceberg support, but we still need to configure the catalog
# Note: In Glue, we can set catalog configurations after SparkContext is created
# The spark.sql.extensions must be set via job arguments (--conf) in the job definition
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", s3_warehouse_path)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Verify Iceberg extensions are available (they should be if configured via --conf)
print("Iceberg catalog configuration:")
print(f"  Catalog: glue_catalog")
print(f"  Warehouse: {s3_warehouse_path}")

# Table schemas for all 24 TPC-DS tables
table_schemas = {
    'call_center': StructType([
        StructField("cc_call_center_sk", IntegerType(), False),
        StructField("cc_call_center_id", StringType(), True),
        StructField("cc_rec_start_date", DateType(), True),
        StructField("cc_rec_end_date", DateType(), True),
        StructField("cc_closed_date_sk", IntegerType(), True),
        StructField("cc_open_date_sk", IntegerType(), True),
        StructField("cc_name", StringType(), True),
        StructField("cc_class", StringType(), True),
        StructField("cc_employees", IntegerType(), True),
        StructField("cc_sq_ft", IntegerType(), True),
        StructField("cc_hours", StringType(), True),
        StructField("cc_manager", StringType(), True),
        StructField("cc_mkt_id", IntegerType(), True),
        StructField("cc_mkt_class", StringType(), True),
        StructField("cc_mkt_desc", StringType(), True),
        StructField("cc_market_manager", StringType(), True),
        StructField("cc_division", IntegerType(), True),
        StructField("cc_division_name", StringType(), True),
        StructField("cc_company", IntegerType(), True),
        StructField("cc_company_name", StringType(), True),
        StructField("cc_street_number", StringType(), True),
        StructField("cc_street_name", StringType(), True),
        StructField("cc_street_type", StringType(), True),
        StructField("cc_suite_number", StringType(), True),
        StructField("cc_city", StringType(), True),
        StructField("cc_county", StringType(), True),
        StructField("cc_state", StringType(), True),
        StructField("cc_zip", StringType(), True),
        StructField("cc_country", StringType(), True),
        StructField("cc_gmt_offset", DoubleType(), True),
        StructField("cc_tax_percentage", DoubleType(), True),
        StructField("c_customer_sk", IntegerType(), False),
        StructField("c_customer_id", StringType(), True),
        StructField("c_current_cdemo_sk", IntegerType(), True),
        StructField("c_current_hdemo_sk", IntegerType(), True),
        StructField("c_current_addr_sk", IntegerType(), True)
    ]),
    'catalog_page': StructType([
        StructField("cp_catalog_page_sk", IntegerType(), False),
        StructField("cp_catalog_page_id", StringType(), True),
        StructField("cp_start_date_sk", IntegerType(), True),
        StructField("cp_end_date_sk", IntegerType(), True),
        StructField("cp_department", StringType(), True),
        StructField("cp_catalog_number", IntegerType(), True),
        StructField("cp_catalog_page_number", IntegerType(), True),
        StructField("cp_description", StringType(), True),
        StructField("cp_type", StringType(), True),
        StructField("ca_address_sk", IntegerType(), False),
        StructField("ca_address_id", StringType(), True),
        StructField("ca_street_number", StringType(), True),
        StructField("ca_street_name", StringType(), True),
        StructField("ca_street_type", StringType(), True),
        StructField("ca_suite_number", StringType(), True),
        StructField("ca_city", StringType(), True),
        StructField("ca_county", StringType(), True),
        StructField("ca_state", StringType(), True),
        StructField("ca_zip", StringType(), True),
        StructField("ca_country", StringType(), True),
        StructField("ca_gmt_offset", DoubleType(), True),
        StructField("ca_location_type", StringType(), True),
        StructField("cd_demo_sk", IntegerType(), False),
        StructField("cd_gender", StringType(), True),
        StructField("cd_marital_status", StringType(), True),
        StructField("cd_education_status", StringType(), True)
    ]),
    'catalog_returns': StructType([
        StructField("cr_returned_date_sk", IntegerType(), True),
        StructField("cr_returned_time_sk", IntegerType(), True),
        StructField("cr_item_sk", IntegerType(), True),
        StructField("cr_refunded_customer_sk", IntegerType(), True),
        StructField("cr_refunded_cdemo_sk", IntegerType(), True),
        StructField("cr_refunded_hdemo_sk", IntegerType(), True),
        StructField("cr_refunded_addr_sk", IntegerType(), True),
        StructField("cr_returning_customer_sk", IntegerType(), True),
        StructField("cr_returning_cdemo_sk", IntegerType(), True),
        StructField("cr_returning_hdemo_sk", IntegerType(), True),
        StructField("cr_returning_addr_sk", IntegerType(), True),
        StructField("cr_call_center_sk", IntegerType(), True),
        StructField("cr_catalog_page_sk", IntegerType(), True),
        StructField("cr_ship_mode_sk", IntegerType(), True),
        StructField("cr_warehouse_sk", IntegerType(), True),
        StructField("cr_reason_sk", IntegerType(), True),
        StructField("cr_order_number", LongType(), True),
        StructField("cr_return_quantity", IntegerType(), True),
        StructField("cr_return_amount", DecimalType(7, 2), True),
        StructField("cr_return_tax", DecimalType(7, 2), True),
        StructField("cr_return_amt_inc_tax", DecimalType(7, 2), True),
        StructField("cr_fee", DecimalType(7, 2), True),
        StructField("cr_return_ship_cost", DecimalType(7, 2), True),
        StructField("cr_refunded_cash", DecimalType(7, 2), True),
        StructField("cr_reversed_charge", DecimalType(7, 2), True),
        StructField("cr_store_credit", DecimalType(7, 2), True),
        StructField("cr_net_loss", DecimalType(7, 2), True),
        StructField("cs_sold_date_sk", IntegerType(), True),
        StructField("cs_sold_time_sk", IntegerType(), True),
        StructField("cs_ship_date_sk", IntegerType(), True),
        StructField("cs_bill_customer_sk", IntegerType(), True),
        StructField("cs_bill_cdemo_sk", IntegerType(), True),
        StructField("cs_bill_hdemo_sk", IntegerType(), True),
        StructField("cs_bill_addr_sk", IntegerType(), True)
    ]),
    'catalog_sales': StructType([
        StructField("cs_sold_date_sk", IntegerType(), True),
        StructField("cs_sold_time_sk", IntegerType(), True),
        StructField("cs_ship_date_sk", IntegerType(), True),
        StructField("cs_bill_customer_sk", IntegerType(), True),
        StructField("cs_bill_cdemo_sk", IntegerType(), True),
        StructField("cs_bill_hdemo_sk", IntegerType(), True),
        StructField("cs_bill_addr_sk", IntegerType(), True),
        StructField("cs_ship_customer_sk", IntegerType(), True),
        StructField("cs_ship_cdemo_sk", IntegerType(), True),
        StructField("cs_ship_hdemo_sk", IntegerType(), True),
        StructField("cs_ship_addr_sk", IntegerType(), True),
        StructField("cs_call_center_sk", IntegerType(), True),
        StructField("cs_catalog_page_sk", IntegerType(), True),
        StructField("cs_ship_mode_sk", IntegerType(), True),
        StructField("cs_warehouse_sk", IntegerType(), True),
        StructField("cs_item_sk", IntegerType(), True),
        StructField("cs_promo_sk", IntegerType(), True),
        StructField("cs_order_number", LongType(), True),
        StructField("cs_quantity", IntegerType(), True),
        StructField("cs_wholesale_cost", DecimalType(7, 2), True),
        StructField("cs_list_price", DecimalType(7, 2), True),
        StructField("cs_sales_price", DecimalType(7, 2), True),
        StructField("cs_ext_discount_amt", DecimalType(7, 2), True),
        StructField("cs_ext_sales_price", DecimalType(7, 2), True),
        StructField("cs_ext_wholesale_cost", DecimalType(7, 2), True),
        StructField("cs_ext_list_price", DecimalType(7, 2), True),
        StructField("cs_ext_tax", DecimalType(7, 2), True),
        StructField("cs_coupon_amt", DecimalType(7, 2), True),
        StructField("cs_ext_ship_cost", DecimalType(7, 2), True),
        StructField("cs_net_paid", DecimalType(7, 2), True),
        StructField("cs_net_paid_inc_tax", DecimalType(7, 2), True),
        StructField("cs_net_paid_inc_ship", DecimalType(7, 2), True),
        StructField("cs_net_paid_inc_ship_tax", DecimalType(7, 2), True),
        StructField("cs_net_profit", DecimalType(7, 2), True)
    ]),
    'customer': StructType([
        StructField("c_customer_sk", IntegerType(), False),
        StructField("c_customer_id", StringType(), True),
        StructField("c_current_cdemo_sk", IntegerType(), True),
        StructField("c_current_hdemo_sk", IntegerType(), True),
        StructField("c_current_addr_sk", IntegerType(), True),
        StructField("c_first_shipto_date_sk", IntegerType(), True),
        StructField("c_first_sales_date_sk", IntegerType(), True),
        StructField("c_salutation", StringType(), True),
        StructField("c_first_name", StringType(), True),
        StructField("c_last_name", StringType(), True),
        StructField("c_preferred_cust_flag", StringType(), True),
        StructField("c_birth_day", IntegerType(), True),
        StructField("c_birth_month", IntegerType(), True),
        StructField("c_birth_year", IntegerType(), True),
        StructField("c_birth_country", StringType(), True),
        StructField("c_login", StringType(), True),
        StructField("c_email_address", StringType(), True),
        StructField("c_last_review_date", StringType(), True),
        StructField("i_item_sk", IntegerType(), False),
        StructField("i_item_id", StringType(), True),
        StructField("i_rec_start_date", DateType(), True),
        StructField("i_rec_end_date", DateType(), True),
        StructField("i_item_desc", StringType(), True),
        StructField("i_current_price", DecimalType(7, 2), True),
        StructField("i_wholesale_cost", DecimalType(7, 2), True),
        StructField("i_brand_id", IntegerType(), True),
        StructField("i_brand", StringType(), True),
        StructField("i_class_id", IntegerType(), True),
        StructField("i_class", StringType(), True),
        StructField("i_category_id", IntegerType(), True),
        StructField("i_category", StringType(), True),
        StructField("i_manufact_id", IntegerType(), True),
        StructField("i_manufact", StringType(), True),
        StructField("i_size", StringType(), True),
        StructField("i_formulation", StringType(), True),
        StructField("i_color", StringType(), True)
    ]),
    'customer_address': StructType([
        StructField("ca_address_sk", IntegerType(), False),
        StructField("ca_address_id", StringType(), True),
        StructField("ca_street_number", StringType(), True),
        StructField("ca_street_name", StringType(), True),
        StructField("ca_street_type", StringType(), True),
        StructField("ca_suite_number", StringType(), True),
        StructField("ca_city", StringType(), True),
        StructField("ca_county", StringType(), True),
        StructField("ca_state", StringType(), True),
        StructField("ca_zip", StringType(), True),
        StructField("ca_country", StringType(), True),
        StructField("ca_gmt_offset", DoubleType(), True),
        StructField("ca_location_type", StringType(), True),
        StructField("cd_demo_sk", IntegerType(), False),
        StructField("cd_gender", StringType(), True),
        StructField("cd_marital_status", StringType(), True),
        StructField("cd_education_status", StringType(), True),
        StructField("cd_purchase_estimate", IntegerType(), True),
        StructField("cd_credit_rating", StringType(), True),
        StructField("cd_dep_count", IntegerType(), True),
        StructField("cd_dep_employed_count", IntegerType(), True),
        StructField("cd_dep_college_count", IntegerType(), True),
        StructField("d_date_sk", IntegerType(), False),
        StructField("d_date_id", StringType(), True),
        StructField("d_date", DateType(), True),
        StructField("d_month_seq", IntegerType(), True),
        StructField("d_week_seq", IntegerType(), True)
    ]),
    'customer_demographics': StructType([
        StructField("cd_demo_sk", IntegerType(), False),
        StructField("cd_gender", StringType(), True),
        StructField("cd_marital_status", StringType(), True),
        StructField("cd_education_status", StringType(), True),
        StructField("cd_purchase_estimate", IntegerType(), True),
        StructField("cd_credit_rating", StringType(), True),
        StructField("cd_dep_count", IntegerType(), True),
        StructField("cd_dep_employed_count", IntegerType(), True),
        StructField("cd_dep_college_count", IntegerType(), True),
        StructField("d_date_sk", IntegerType(), False),
        StructField("d_date_id", StringType(), True),
        StructField("d_date", DateType(), True),
        StructField("d_month_seq", IntegerType(), True),
        StructField("d_week_seq", IntegerType(), True),
        StructField("d_quarter_seq", IntegerType(), True),
        StructField("d_year", IntegerType(), True),
        StructField("d_dow", IntegerType(), True),
        StructField("d_moy", IntegerType(), True),
        StructField("d_dom", IntegerType(), True),
        StructField("d_qoy", IntegerType(), True),
        StructField("d_fy_year", IntegerType(), True),
        StructField("d_fy_quarter_seq", IntegerType(), True),
        StructField("d_fy_week_seq", IntegerType(), True),
        StructField("d_day_name", StringType(), True),
        StructField("d_quarter_name", StringType(), True),
        StructField("d_holiday", StringType(), True),
        StructField("d_weekend", StringType(), True),
        StructField("d_following_holiday", StringType(), True),
        StructField("d_first_dom", IntegerType(), True),
        StructField("d_last_dom", IntegerType(), True),
        StructField("d_same_day_ly", IntegerType(), True),
        StructField("d_same_day_lq", IntegerType(), True),
        StructField("d_current_day", StringType(), True),
        StructField("d_current_week", StringType(), True),
        StructField("d_current_month", StringType(), True),
        StructField("d_current_quarter", StringType(), True),
        StructField("d_current_year", StringType(), True)
    ]),
    'date_dim': StructType([
        StructField("d_date_sk", IntegerType(), False),
        StructField("d_date_id", StringType(), True),
        StructField("d_date", DateType(), True),
        StructField("d_month_seq", IntegerType(), True),
        StructField("d_week_seq", IntegerType(), True),
        StructField("d_quarter_seq", IntegerType(), True),
        StructField("d_year", IntegerType(), True),
        StructField("d_dow", IntegerType(), True),
        StructField("d_moy", IntegerType(), True),
        StructField("d_dom", IntegerType(), True),
        StructField("d_qoy", IntegerType(), True),
        StructField("d_fy_year", IntegerType(), True),
        StructField("d_fy_quarter_seq", IntegerType(), True),
        StructField("d_fy_week_seq", IntegerType(), True),
        StructField("d_day_name", StringType(), True),
        StructField("d_quarter_name", StringType(), True),
        StructField("d_holiday", StringType(), True),
        StructField("d_weekend", StringType(), True),
        StructField("d_following_holiday", StringType(), True),
        StructField("d_first_dom", IntegerType(), True),
        StructField("d_last_dom", IntegerType(), True),
        StructField("d_same_day_ly", IntegerType(), True),
        StructField("d_same_day_lq", IntegerType(), True),
        StructField("d_current_day", StringType(), True),
        StructField("d_current_week", StringType(), True),
        StructField("d_current_month", StringType(), True),
        StructField("d_current_quarter", StringType(), True),
        StructField("d_current_year", StringType(), True),
        StructField("hd_demo_sk", IntegerType(), False),
        StructField("hd_income_band_sk", IntegerType(), True),
        StructField("hd_buy_potential", StringType(), True),
        StructField("hd_dep_count", IntegerType(), True),
        StructField("hd_vehicle_count", IntegerType(), True)
    ]),
    'household_demographics': StructType([
        StructField("hd_demo_sk", IntegerType(), False),
        StructField("hd_income_band_sk", IntegerType(), True),
        StructField("hd_buy_potential", StringType(), True),
        StructField("hd_dep_count", IntegerType(), True),
        StructField("hd_vehicle_count", IntegerType(), True),
        StructField("ib_income_band_sk", IntegerType(), False),
        StructField("ib_lower_bound", IntegerType(), True),
        StructField("ib_upper_bound", IntegerType(), True),
        StructField("p_promo_sk", IntegerType(), False),
        StructField("p_promo_id", StringType(), True),
        StructField("p_start_date_sk", IntegerType(), True),
        StructField("p_end_date_sk", IntegerType(), True),
        StructField("p_item_sk", IntegerType(), True),
        StructField("p_cost", DecimalType(15, 2), True),
        StructField("p_response_target", IntegerType(), True),
        StructField("p_promo_name", StringType(), True),
        StructField("p_channel_dmail", StringType(), True),
        StructField("p_channel_email", StringType(), True),
        StructField("p_channel_catalog", StringType(), True),
        StructField("p_channel_tv", StringType(), True),
        StructField("p_channel_radio", StringType(), True),
        StructField("p_channel_press", StringType(), True),
        StructField("p_channel_event", StringType(), True),
        StructField("p_channel_demo", StringType(), True),
        StructField("p_channel_details", StringType(), True),
        StructField("p_purpose", StringType(), True),
        StructField("p_discount_active", StringType(), True)
    ]),
    'income_band': StructType([
        StructField("ib_income_band_sk", IntegerType(), False),
        StructField("ib_lower_bound", IntegerType(), True),
        StructField("ib_upper_bound", IntegerType(), True),
        StructField("p_promo_sk", IntegerType(), False),
        StructField("p_promo_id", StringType(), True),
        StructField("p_start_date_sk", IntegerType(), True),
        StructField("p_end_date_sk", IntegerType(), True),
        StructField("p_item_sk", IntegerType(), True),
        StructField("p_cost", DecimalType(15, 2), True),
        StructField("p_response_target", IntegerType(), True),
        StructField("p_promo_name", StringType(), True),
        StructField("p_channel_dmail", StringType(), True),
        StructField("p_channel_email", StringType(), True),
        StructField("p_channel_catalog", StringType(), True),
        StructField("p_channel_tv", StringType(), True),
        StructField("p_channel_radio", StringType(), True),
        StructField("p_channel_press", StringType(), True),
        StructField("p_channel_event", StringType(), True),
        StructField("p_channel_demo", StringType(), True),
        StructField("p_channel_details", StringType(), True),
        StructField("p_purpose", StringType(), True),
        StructField("p_discount_active", StringType(), True),
        StructField("r_reason_sk", IntegerType(), False),
        StructField("r_reason_id", StringType(), True),
        StructField("r_reason_desc", StringType(), True)
    ]),
    'inventory': StructType([
        StructField("inv_date_sk", IntegerType(), True),
        StructField("inv_item_sk", IntegerType(), True),
        StructField("inv_warehouse_sk", IntegerType(), True),
        StructField("inv_quantity_on_hand", IntegerType(), True),
        StructField("sr_returned_date_sk", IntegerType(), True),
        StructField("sr_return_time_sk", IntegerType(), True),
        StructField("sr_item_sk", IntegerType(), True),
        StructField("sr_customer_sk", IntegerType(), True),
        StructField("sr_cdemo_sk", IntegerType(), True),
        StructField("sr_hdemo_sk", IntegerType(), True),
        StructField("sr_addr_sk", IntegerType(), True),
        StructField("sr_store_sk", IntegerType(), True),
        StructField("sr_reason_sk", IntegerType(), True),
        StructField("sr_ticket_number", LongType(), True),
        StructField("sr_return_quantity", IntegerType(), True),
        StructField("sr_return_amt", DecimalType(7, 2), True),
        StructField("sr_return_tax", DecimalType(7, 2), True),
        StructField("sr_return_amt_inc_tax", DecimalType(7, 2), True),
        StructField("sr_fee", DecimalType(7, 2), True),
        StructField("sr_return_ship_cost", DecimalType(7, 2), True),
        StructField("sr_refunded_cash", DecimalType(7, 2), True),
        StructField("sr_reversed_charge", DecimalType(7, 2), True),
        StructField("sr_store_credit", DecimalType(7, 2), True),
        StructField("sr_net_loss", DecimalType(7, 2), True),
        StructField("wr_returned_date_sk", IntegerType(), True),
        StructField("wr_returned_time_sk", IntegerType(), True)
    ]),
    'item': StructType([
        StructField("i_item_sk", IntegerType(), False),
        StructField("i_item_id", StringType(), True),
        StructField("i_rec_start_date", DateType(), True),
        StructField("i_rec_end_date", DateType(), True),
        StructField("i_item_desc", StringType(), True),
        StructField("i_current_price", DecimalType(7, 2), True),
        StructField("i_wholesale_cost", DecimalType(7, 2), True),
        StructField("i_brand_id", IntegerType(), True),
        StructField("i_brand", StringType(), True),
        StructField("i_class_id", IntegerType(), True),
        StructField("i_class", StringType(), True),
        StructField("i_category_id", IntegerType(), True),
        StructField("i_category", StringType(), True),
        StructField("i_manufact_id", IntegerType(), True),
        StructField("i_manufact", StringType(), True),
        StructField("i_size", StringType(), True),
        StructField("i_formulation", StringType(), True),
        StructField("i_color", StringType(), True),
        StructField("i_units", StringType(), True),
        StructField("i_container", StringType(), True),
        StructField("i_manager_id", IntegerType(), True),
        StructField("i_product_name", StringType(), True),
        StructField("ss_sold_date_sk", IntegerType(), True),
        StructField("ss_sold_time_sk", IntegerType(), True),
        StructField("ss_item_sk", IntegerType(), True),
        StructField("ss_customer_sk", IntegerType(), True),
        StructField("ss_cdemo_sk", IntegerType(), True),
        StructField("ss_hdemo_sk", IntegerType(), True),
        StructField("ss_addr_sk", IntegerType(), True),
        StructField("ss_store_sk", IntegerType(), True),
        StructField("ss_promo_sk", IntegerType(), True),
        StructField("ss_ticket_number", LongType(), True),
        StructField("ss_quantity", IntegerType(), True),
        StructField("ss_wholesale_cost", DecimalType(7, 2), True),
        StructField("ss_list_price", DecimalType(7, 2), True),
        StructField("ss_sales_price", DecimalType(7, 2), True),
        StructField("ss_ext_discount_amt", DecimalType(7, 2), True)
    ]),
    'promotion': StructType([
        StructField("p_promo_sk", IntegerType(), False),
        StructField("p_promo_id", StringType(), True),
        StructField("p_start_date_sk", IntegerType(), True),
        StructField("p_end_date_sk", IntegerType(), True),
        StructField("p_item_sk", IntegerType(), True),
        StructField("p_cost", DecimalType(15, 2), True),
        StructField("p_response_target", IntegerType(), True),
        StructField("p_promo_name", StringType(), True),
        StructField("p_channel_dmail", StringType(), True),
        StructField("p_channel_email", StringType(), True),
        StructField("p_channel_catalog", StringType(), True),
        StructField("p_channel_tv", StringType(), True),
        StructField("p_channel_radio", StringType(), True),
        StructField("p_channel_press", StringType(), True),
        StructField("p_channel_event", StringType(), True),
        StructField("p_channel_demo", StringType(), True),
        StructField("p_channel_details", StringType(), True),
        StructField("p_purpose", StringType(), True),
        StructField("p_discount_active", StringType(), True),
        StructField("r_reason_sk", IntegerType(), False),
        StructField("r_reason_id", StringType(), True),
        StructField("r_reason_desc", StringType(), True),
        StructField("sm_ship_mode_sk", IntegerType(), False),
        StructField("sm_ship_mode_id", StringType(), True),
        StructField("sm_type", StringType(), True),
        StructField("sm_code", StringType(), True),
        StructField("sm_carrier", StringType(), True)
    ]),
    'reason': StructType([
        StructField("r_reason_sk", IntegerType(), False),
        StructField("r_reason_id", StringType(), True),
        StructField("r_reason_desc", StringType(), True),
        StructField("sm_ship_mode_sk", IntegerType(), False),
        StructField("sm_ship_mode_id", StringType(), True),
        StructField("sm_type", StringType(), True),
        StructField("sm_code", StringType(), True),
        StructField("sm_carrier", StringType(), True),
        StructField("sm_contract", StringType(), True),
        StructField("s_store_sk", IntegerType(), False),
        StructField("s_store_id", StringType(), True),
        StructField("s_rec_start_date", DateType(), True),
        StructField("s_rec_end_date", DateType(), True),
        StructField("s_closed_date_sk", IntegerType(), True),
        StructField("s_store_name", StringType(), True),
        StructField("s_number_employees", IntegerType(), True),
        StructField("s_floor_space", IntegerType(), True),
        StructField("s_hours", StringType(), True),
        StructField("s_manager", StringType(), True),
        StructField("s_market_id", IntegerType(), True),
        StructField("s_geography_class", StringType(), True),
        StructField("s_market_desc", StringType(), True),
        StructField("s_market_manager", StringType(), True),
        StructField("s_division_id", IntegerType(), True),
        StructField("s_division_name", StringType(), True),
        StructField("s_company_id", IntegerType(), True),
        StructField("s_company_name", StringType(), True)
    ]),
    'ship_mode': StructType([
        StructField("sm_ship_mode_sk", IntegerType(), False),
        StructField("sm_ship_mode_id", StringType(), True),
        StructField("sm_type", StringType(), True),
        StructField("sm_code", StringType(), True),
        StructField("sm_carrier", StringType(), True),
        StructField("sm_contract", StringType(), True),
        StructField("s_store_sk", IntegerType(), False),
        StructField("s_store_id", StringType(), True),
        StructField("s_rec_start_date", DateType(), True),
        StructField("s_rec_end_date", DateType(), True),
        StructField("s_closed_date_sk", IntegerType(), True),
        StructField("s_store_name", StringType(), True),
        StructField("s_number_employees", IntegerType(), True),
        StructField("s_floor_space", IntegerType(), True),
        StructField("s_hours", StringType(), True),
        StructField("s_manager", StringType(), True),
        StructField("s_market_id", IntegerType(), True),
        StructField("s_geography_class", StringType(), True),
        StructField("s_market_desc", StringType(), True),
        StructField("s_market_manager", StringType(), True),
        StructField("s_division_id", IntegerType(), True),
        StructField("s_division_name", StringType(), True),
        StructField("s_company_id", IntegerType(), True),
        StructField("s_company_name", StringType(), True),
        StructField("s_street_number", StringType(), True),
        StructField("s_street_name", StringType(), True),
        StructField("s_street_type", StringType(), True),
        StructField("s_suite_number", StringType(), True),
        StructField("s_city", StringType(), True),
        StructField("s_county", StringType(), True),
        StructField("s_state", StringType(), True),
        StructField("s_zip", StringType(), True),
        StructField("s_country", StringType(), True),
        StructField("s_gmt_offset", DoubleType(), True),
        StructField("s_tax_precentage", DoubleType(), True)
    ]),
    'store': StructType([
        StructField("s_store_sk", IntegerType(), False),
        StructField("s_store_id", StringType(), True),
        StructField("s_rec_start_date", DateType(), True),
        StructField("s_rec_end_date", DateType(), True),
        StructField("s_closed_date_sk", IntegerType(), True),
        StructField("s_store_name", StringType(), True),
        StructField("s_number_employees", IntegerType(), True),
        StructField("s_floor_space", IntegerType(), True),
        StructField("s_hours", StringType(), True),
        StructField("s_manager", StringType(), True),
        StructField("s_market_id", IntegerType(), True),
        StructField("s_geography_class", StringType(), True),
        StructField("s_market_desc", StringType(), True),
        StructField("s_market_manager", StringType(), True),
        StructField("s_division_id", IntegerType(), True),
        StructField("s_division_name", StringType(), True),
        StructField("s_company_id", IntegerType(), True),
        StructField("s_company_name", StringType(), True),
        StructField("s_street_number", StringType(), True),
        StructField("s_street_name", StringType(), True),
        StructField("s_street_type", StringType(), True),
        StructField("s_suite_number", StringType(), True),
        StructField("s_city", StringType(), True),
        StructField("s_county", StringType(), True),
        StructField("s_state", StringType(), True),
        StructField("s_zip", StringType(), True),
        StructField("s_country", StringType(), True),
        StructField("s_gmt_offset", DoubleType(), True),
        StructField("s_tax_precentage", DoubleType(), True),
        StructField("t_time_sk", IntegerType(), False),
        StructField("t_time_id", StringType(), True),
        StructField("t_time", IntegerType(), True),
        StructField("t_hour", IntegerType(), True),
        StructField("t_minute", IntegerType(), True),
        StructField("t_second", IntegerType(), True),
        StructField("t_am_pm", StringType(), True),
        StructField("t_shift", StringType(), True),
        StructField("t_sub_shift", StringType(), True)
    ]),
    'store_returns': StructType([
        StructField("sr_returned_date_sk", IntegerType(), True),
        StructField("sr_return_time_sk", IntegerType(), True),
        StructField("sr_item_sk", IntegerType(), True),
        StructField("sr_customer_sk", IntegerType(), True),
        StructField("sr_cdemo_sk", IntegerType(), True),
        StructField("sr_hdemo_sk", IntegerType(), True),
        StructField("sr_addr_sk", IntegerType(), True),
        StructField("sr_store_sk", IntegerType(), True),
        StructField("sr_reason_sk", IntegerType(), True),
        StructField("sr_ticket_number", LongType(), True),
        StructField("sr_return_quantity", IntegerType(), True),
        StructField("sr_return_amt", DecimalType(7, 2), True),
        StructField("sr_return_tax", DecimalType(7, 2), True),
        StructField("sr_return_amt_inc_tax", DecimalType(7, 2), True),
        StructField("sr_fee", DecimalType(7, 2), True),
        StructField("sr_return_ship_cost", DecimalType(7, 2), True),
        StructField("sr_refunded_cash", DecimalType(7, 2), True),
        StructField("sr_reversed_charge", DecimalType(7, 2), True),
        StructField("sr_store_credit", DecimalType(7, 2), True),
        StructField("sr_net_loss", DecimalType(7, 2), True),
        StructField("wr_returned_date_sk", IntegerType(), True),
        StructField("wr_returned_time_sk", IntegerType(), True),
        StructField("wr_item_sk", IntegerType(), True),
        StructField("wr_refunded_customer_sk", IntegerType(), True),
        StructField("wr_refunded_cdemo_sk", IntegerType(), True),
        StructField("wr_refunded_hdemo_sk", IntegerType(), True),
        StructField("wr_refunded_addr_sk", IntegerType(), True),
        StructField("wr_returning_customer_sk", IntegerType(), True),
        StructField("wr_returning_cdemo_sk", IntegerType(), True),
        StructField("wr_returning_hdemo_sk", IntegerType(), True),
        StructField("wr_returning_addr_sk", IntegerType(), True),
        StructField("wr_web_page_sk", IntegerType(), True),
        StructField("wr_reason_sk", IntegerType(), True),
        StructField("wr_order_number", LongType(), True)
    ]),
    'store_sales': StructType([
        StructField("ss_sold_date_sk", IntegerType(), True),
        StructField("ss_sold_time_sk", IntegerType(), True),
        StructField("ss_item_sk", IntegerType(), True),
        StructField("ss_customer_sk", IntegerType(), True),
        StructField("ss_cdemo_sk", IntegerType(), True),
        StructField("ss_hdemo_sk", IntegerType(), True),
        StructField("ss_addr_sk", IntegerType(), True),
        StructField("ss_store_sk", IntegerType(), True),
        StructField("ss_promo_sk", IntegerType(), True),
        StructField("ss_ticket_number", LongType(), True),
        StructField("ss_quantity", IntegerType(), True),
        StructField("ss_wholesale_cost", DecimalType(7, 2), True),
        StructField("ss_list_price", DecimalType(7, 2), True),
        StructField("ss_sales_price", DecimalType(7, 2), True),
        StructField("ss_ext_discount_amt", DecimalType(7, 2), True),
        StructField("ss_ext_sales_price", DecimalType(7, 2), True),
        StructField("ss_ext_wholesale_cost", DecimalType(7, 2), True),
        StructField("ss_ext_list_price", DecimalType(7, 2), True),
        StructField("ss_ext_tax", DecimalType(7, 2), True),
        StructField("ss_coupon_amt", DecimalType(7, 2), True),
        StructField("ss_net_paid", DecimalType(7, 2), True),
        StructField("ss_net_paid_inc_tax", DecimalType(7, 2), True),
        StructField("ss_net_profit", DecimalType(7, 2), True)
    ]),
    'time_dim': StructType([
        StructField("t_time_sk", IntegerType(), False),
        StructField("t_time_id", StringType(), True),
        StructField("t_time", IntegerType(), True),
        StructField("t_hour", IntegerType(), True),
        StructField("t_minute", IntegerType(), True),
        StructField("t_second", IntegerType(), True),
        StructField("t_am_pm", StringType(), True),
        StructField("t_shift", StringType(), True),
        StructField("t_sub_shift", StringType(), True),
        StructField("t_meal_time", StringType(), True),
        StructField("w_warehouse_sk", IntegerType(), False),
        StructField("w_warehouse_id", StringType(), True),
        StructField("w_warehouse_name", StringType(), True),
        StructField("w_warehouse_sq_ft", IntegerType(), True),
        StructField("w_street_number", StringType(), True),
        StructField("w_street_name", StringType(), True),
        StructField("w_street_type", StringType(), True),
        StructField("w_suite_number", StringType(), True),
        StructField("w_city", StringType(), True),
        StructField("w_county", StringType(), True),
        StructField("w_state", StringType(), True),
        StructField("w_zip", StringType(), True),
        StructField("w_country", StringType(), True),
        StructField("w_gmt_offset", DoubleType(), True),
        StructField("wp_web_page_sk", IntegerType(), False),
        StructField("wp_web_page_id", StringType(), True),
        StructField("wp_rec_start_date", DateType(), True),
        StructField("wp_rec_end_date", DateType(), True)
    ]),
    'warehouse': StructType([
        StructField("w_warehouse_sk", IntegerType(), False),
        StructField("w_warehouse_id", StringType(), True),
        StructField("w_warehouse_name", StringType(), True),
        StructField("w_warehouse_sq_ft", IntegerType(), True),
        StructField("w_street_number", StringType(), True),
        StructField("w_street_name", StringType(), True),
        StructField("w_street_type", StringType(), True),
        StructField("w_suite_number", StringType(), True),
        StructField("w_city", StringType(), True),
        StructField("w_county", StringType(), True),
        StructField("w_state", StringType(), True),
        StructField("w_zip", StringType(), True),
        StructField("w_country", StringType(), True),
        StructField("w_gmt_offset", DoubleType(), True),
        StructField("wp_web_page_sk", IntegerType(), False),
        StructField("wp_web_page_id", StringType(), True),
        StructField("wp_rec_start_date", DateType(), True),
        StructField("wp_rec_end_date", DateType(), True),
        StructField("wp_creation_date_sk", IntegerType(), True),
        StructField("wp_access_date_sk", IntegerType(), True),
        StructField("wp_autogen_flag", StringType(), True),
        StructField("wp_customer_sk", IntegerType(), True),
        StructField("wp_url", StringType(), True),
        StructField("wp_type", StringType(), True),
        StructField("wp_char_count", IntegerType(), True),
        StructField("wp_link_count", IntegerType(), True),
        StructField("wp_image_count", IntegerType(), True),
        StructField("wp_max_ad_count", IntegerType(), True)
    ]),
    'web_page': StructType([
        StructField("wp_web_page_sk", IntegerType(), False),
        StructField("wp_web_page_id", StringType(), True),
        StructField("wp_rec_start_date", DateType(), True),
        StructField("wp_rec_end_date", DateType(), True),
        StructField("wp_creation_date_sk", IntegerType(), True),
        StructField("wp_access_date_sk", IntegerType(), True),
        StructField("wp_autogen_flag", StringType(), True),
        StructField("wp_customer_sk", IntegerType(), True),
        StructField("wp_url", StringType(), True),
        StructField("wp_type", StringType(), True),
        StructField("wp_char_count", IntegerType(), True),
        StructField("wp_link_count", IntegerType(), True),
        StructField("wp_image_count", IntegerType(), True),
        StructField("wp_max_ad_count", IntegerType(), True),
        StructField("web_site_sk", IntegerType(), False),
        StructField("web_site_id", StringType(), True),
        StructField("web_rec_start_date", DateType(), True),
        StructField("web_rec_end_date", DateType(), True),
        StructField("web_name", StringType(), True),
        StructField("web_open_date_sk", IntegerType(), True),
        StructField("web_close_date_sk", IntegerType(), True),
        StructField("web_class", StringType(), True),
        StructField("web_manager", StringType(), True),
        StructField("web_mkt_id", IntegerType(), True),
        StructField("web_mkt_class", StringType(), True),
        StructField("web_mkt_desc", StringType(), True),
        StructField("web_market_manager", StringType(), True),
        StructField("web_company_id", IntegerType(), True),
        StructField("web_company_name", StringType(), True),
        StructField("web_street_number", StringType(), True),
        StructField("web_street_name", StringType(), True),
        StructField("web_street_type", StringType(), True),
        StructField("web_suite_number", StringType(), True),
        StructField("web_city", StringType(), True),
        StructField("web_county", StringType(), True),
        StructField("web_state", StringType(), True),
        StructField("web_zip", StringType(), True)
    ]),
    'web_returns': StructType([
        StructField("wr_returned_date_sk", IntegerType(), True),
        StructField("wr_returned_time_sk", IntegerType(), True),
        StructField("wr_item_sk", IntegerType(), True),
        StructField("wr_refunded_customer_sk", IntegerType(), True),
        StructField("wr_refunded_cdemo_sk", IntegerType(), True),
        StructField("wr_refunded_hdemo_sk", IntegerType(), True),
        StructField("wr_refunded_addr_sk", IntegerType(), True),
        StructField("wr_returning_customer_sk", IntegerType(), True),
        StructField("wr_returning_cdemo_sk", IntegerType(), True),
        StructField("wr_returning_hdemo_sk", IntegerType(), True),
        StructField("wr_returning_addr_sk", IntegerType(), True),
        StructField("wr_web_page_sk", IntegerType(), True),
        StructField("wr_reason_sk", IntegerType(), True),
        StructField("wr_order_number", LongType(), True),
        StructField("wr_return_quantity", IntegerType(), True),
        StructField("wr_return_amt", DecimalType(7, 2), True),
        StructField("wr_return_tax", DecimalType(7, 2), True),
        StructField("wr_return_amt_inc_tax", DecimalType(7, 2), True),
        StructField("wr_fee", DecimalType(7, 2), True),
        StructField("wr_return_ship_cost", DecimalType(7, 2), True),
        StructField("wr_refunded_cash", DecimalType(7, 2), True),
        StructField("wr_reversed_charge", DecimalType(7, 2), True),
        StructField("wr_account_credit", DecimalType(7, 2), True),
        StructField("wr_net_loss", DecimalType(7, 2), True),
        StructField("ws_sold_date_sk", IntegerType(), True),
        StructField("ws_sold_time_sk", IntegerType(), True),
        StructField("ws_ship_date_sk", IntegerType(), True),
        StructField("ws_item_sk", IntegerType(), True),
        StructField("ws_bill_customer_sk", IntegerType(), True),
        StructField("ws_bill_cdemo_sk", IntegerType(), True),
        StructField("ws_bill_hdemo_sk", IntegerType(), True),
        StructField("ws_bill_addr_sk", IntegerType(), True),
        StructField("ws_ship_customer_sk", IntegerType(), True),
        StructField("ws_ship_cdemo_sk", IntegerType(), True)
    ]),
    'web_sales': StructType([
        StructField("ws_sold_date_sk", IntegerType(), True),
        StructField("ws_sold_time_sk", IntegerType(), True),
        StructField("ws_ship_date_sk", IntegerType(), True),
        StructField("ws_item_sk", IntegerType(), True),
        StructField("ws_bill_customer_sk", IntegerType(), True),
        StructField("ws_bill_cdemo_sk", IntegerType(), True),
        StructField("ws_bill_hdemo_sk", IntegerType(), True),
        StructField("ws_bill_addr_sk", IntegerType(), True),
        StructField("ws_ship_customer_sk", IntegerType(), True),
        StructField("ws_ship_cdemo_sk", IntegerType(), True),
        StructField("ws_ship_hdemo_sk", IntegerType(), True),
        StructField("ws_ship_addr_sk", IntegerType(), True),
        StructField("ws_web_page_sk", IntegerType(), True),
        StructField("ws_web_site_sk", IntegerType(), True),
        StructField("ws_ship_mode_sk", IntegerType(), True),
        StructField("ws_warehouse_sk", IntegerType(), True),
        StructField("ws_promo_sk", IntegerType(), True),
        StructField("ws_order_number", LongType(), True),
        StructField("ws_quantity", IntegerType(), True),
        StructField("ws_wholesale_cost", DecimalType(7, 2), True),
        StructField("ws_list_price", DecimalType(7, 2), True),
        StructField("ws_sales_price", DecimalType(7, 2), True),
        StructField("ws_ext_discount_amt", DecimalType(7, 2), True),
        StructField("ws_ext_sales_price", DecimalType(7, 2), True),
        StructField("ws_ext_wholesale_cost", DecimalType(7, 2), True),
        StructField("ws_ext_list_price", DecimalType(7, 2), True),
        StructField("ws_ext_tax", DecimalType(7, 2), True),
        StructField("ws_coupon_amt", DecimalType(7, 2), True),
        StructField("ws_ext_ship_cost", DecimalType(7, 2), True),
        StructField("ws_net_paid", DecimalType(7, 2), True),
        StructField("ws_net_paid_inc_tax", DecimalType(7, 2), True),
        StructField("ws_net_paid_inc_ship", DecimalType(7, 2), True),
        StructField("ws_net_paid_inc_ship_tax", DecimalType(7, 2), True),
        StructField("ws_net_profit", DecimalType(7, 2), True)
    ]),
    'web_site': StructType([
        StructField("web_site_sk", IntegerType(), False),
        StructField("web_site_id", StringType(), True),
        StructField("web_rec_start_date", DateType(), True),
        StructField("web_rec_end_date", DateType(), True),
        StructField("web_name", StringType(), True),
        StructField("web_open_date_sk", IntegerType(), True),
        StructField("web_close_date_sk", IntegerType(), True),
        StructField("web_class", StringType(), True),
        StructField("web_manager", StringType(), True),
        StructField("web_mkt_id", IntegerType(), True),
        StructField("web_mkt_class", StringType(), True),
        StructField("web_mkt_desc", StringType(), True),
        StructField("web_market_manager", StringType(), True),
        StructField("web_company_id", IntegerType(), True),
        StructField("web_company_name", StringType(), True),
        StructField("web_street_number", StringType(), True),
        StructField("web_street_name", StringType(), True),
        StructField("web_street_type", StringType(), True),
        StructField("web_suite_number", StringType(), True),
        StructField("web_city", StringType(), True),
        StructField("web_county", StringType(), True),
        StructField("web_state", StringType(), True),
        StructField("web_zip", StringType(), True),
        StructField("web_country", StringType(), True),
        StructField("web_gmt_offset", DoubleType(), True),
        StructField("web_tax_percentage", DoubleType(), True),
        StructField("cr_returned_date_sk", IntegerType(), True),
        StructField("cr_returned_time_sk", IntegerType(), True),
        StructField("cr_item_sk", IntegerType(), True),
        StructField("cr_refunded_customer_sk", IntegerType(), True),
        StructField("cr_refunded_cdemo_sk", IntegerType(), True),
        StructField("cr_refunded_hdemo_sk", IntegerType(), True),
        StructField("cr_refunded_addr_sk", IntegerType(), True),
        StructField("cr_returning_customer_sk", IntegerType(), True)
    ]),
}

def create_iceberg_table(table_name, schema):
    """Create an Iceberg table using Glue catalog"""
    print(f"\nCreating Iceberg table: {table_name}")
    
    try:
        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], schema)
        
        # Full table name in Glue catalog
        full_table_name = f"glue_catalog.{glue_database}.{table_name}"
        
        print(f"  Table name: {full_table_name}")
        print(f"  Schema: {len(schema.fields)} columns")
        
        # First, try to drop the table if it exists (from Terraform, lacks Iceberg metadata)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            print(f"  Dropped existing table (if any)")
        except Exception as drop_error:
            print(f"  ⚠️  Could not drop table (may not exist): {drop_error}")
        
        # Create Iceberg table with properties
        writer = empty_df.writeTo(full_table_name)
        writer = writer.tableProperty("write.format.default", "parquet")
        writer = writer.tableProperty("write.parquet.compression-codec", "snappy")
        writer = writer.tableProperty("write.target-file-size-bytes", "134217728")
        writer.create()
        
        # Get metadata location
        try:
            metadata_result = spark.sql(
                f"SHOW TBLPROPERTIES {full_table_name} ('metadata_location')"
            ).collect()
            if metadata_result:
                metadata_location = metadata_result[0][0]
                print(f"  ✅ Metadata location: {metadata_location}")
        except Exception as e:
            print(f"  ⚠️  Could not retrieve metadata location: {e}")
        
        print(f"  ✅ Successfully created Iceberg table: {table_name}")
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower() or "TableExistsException" in error_msg or "TABLE_ALREADY_EXISTS" in error_msg:
            print(f"  ⚠️  Table already exists, dropping and recreating with Iceberg metadata...")
            try:
                # Drop existing table (created by Terraform, lacks Iceberg metadata)
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                print(f"  Dropped existing table")
                # Recreate with proper Iceberg metadata
                writer = empty_df.writeTo(full_table_name)
                writer = writer.tableProperty("write.format.default", "parquet")
                writer = writer.tableProperty("write.parquet.compression-codec", "snappy")
                writer = writer.tableProperty("write.target-file-size-bytes", "134217728")
                writer.create()
                
                # Get metadata location
                try:
                    metadata_result = spark.sql(
                        f"SHOW TBLPROPERTIES {full_table_name} ('metadata_location')"
                    ).collect()
                    if metadata_result:
                        metadata_location = metadata_result[0][0]
                        print(f"  ✅ Metadata location: {metadata_location}")
                except Exception as e2:
                    print(f"  ⚠️  Could not retrieve metadata location: {e2}")
                
                print(f"  ✅ Successfully recreated table with Iceberg metadata: {table_name}")
                return True
            except Exception as e2:
                print(f"  ❌ Failed to recreate table {table_name}: {e2}")
                import traceback
                traceback.print_exc()
                return False
        else:
            print(f"  ❌ Failed to create table {table_name}: {e}")
            import traceback
            traceback.print_exc()
            return False

# Create all tables
print(f"\nCreating {len(table_schemas)} Iceberg tables...")
print("=" * 70)

success_count = 0
failed_tables = []

for table_name, schema in sorted(table_schemas.items()):
    print(f"\n[{success_count + len(failed_tables) + 1}/{len(table_schemas)}] Processing: {table_name}")
    
    if create_iceberg_table(table_name, schema):
        success_count += 1
    else:
        failed_tables.append(table_name)

# Summary
print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"✅ Successfully created: {success_count}/{len(table_schemas)} tables")

if failed_tables:
    print(f"❌ Failed tables: {len(failed_tables)}")
    for table in failed_tables:
        print(f"   - {table}")
    
if success_count == len(table_schemas):
    print("\n🎉 All Iceberg tables created successfully!")
    print("\nNext steps:")
    print("1. Verify tables in AWS Glue Console")
    print("2. Check S3 for metadata files")
    print("3. Test from Snowflake:")
    print(f"   SELECT * FROM AWS_GLUE_CATALOG.{glue_database}.call_center LIMIT 1;")

# Commit job
job.commit()

print("\n" + "=" * 70)
