"""
Mock TPC-DS Data Generator
Generates synthetic TPC-DS data for testing without requiring the official toolkit
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import time
import os
from pathlib import Path
import random
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class MockTPCDSDataGenerator:
    """Generates synthetic TPC-DS data for testing purposes"""
    
    def __init__(self, scale_factor: float = 1.0, data_dir: str = None):
        """
        Initialize mock TPC-DS data generator
        
        Args:
            scale_factor: Scale factor for data generation (1.0 = 1GB)
            data_dir: Directory to store generated data
        """
        self.scale_factor = scale_factor
        self.data_dir = data_dir or f"data/tpcds_data_sf{scale_factor}"
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Calculate row counts based on scale factor
        self.row_counts = self._calculate_row_counts(scale_factor)
        
        logger.info(f"Mock TPC-DS Data Generator initialized with scale factor {scale_factor}")
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Estimated total rows: {sum(self.row_counts.values()):,}")
    
    def _calculate_row_counts(self, scale_factor: float) -> Dict[str, int]:
        """Calculate row counts for each table based on scale factor"""
        # Base row counts for SF=1 (approximately 1GB)
        base_counts = {
            # Dimension tables (smaller)
            'call_center': 6,
            'catalog_page': 11718,
            'customer': 100000,
            'customer_address': 50000,
            'customer_demographics': 1920800,
            'date_dim': 73049,
            'household_demographics': 7200,
            'income_band': 20,
            'item': 18000,
            'promotion': 300,
            'reason': 35,
            'ship_mode': 20,
            'store': 12,
            'time_dim': 86400,
            'warehouse': 5,
            'web_page': 60,
            'web_site': 30,
            
            # Fact tables (larger)
            'catalog_returns': int(144067 * scale_factor),
            'catalog_sales': int(1441548 * scale_factor),
            'inventory': int(11745000 * scale_factor),
            'store_returns': int(287514 * scale_factor),
            'store_sales': int(2880404 * scale_factor),
            'web_returns': int(71763 * scale_factor),
            'web_sales': int(719384 * scale_factor)
        }
        
        # Scale all counts
        scaled_counts = {}
        for table, count in base_counts.items():
            scaled_counts[table] = max(1, int(count * scale_factor))
        
        return scaled_counts
    
    def generate_data(self) -> Dict[str, Any]:
        """Generate synthetic TPC-DS data"""
        logger.info(f"Generating synthetic TPC-DS data with scale factor {self.scale_factor}")
        
        start_time = time.time()
        generated_files = []
        generated_tables = {}
        total_rows_generated = 0
        
        try:
            # Generate dimension tables first
            dimension_tables = [
                'call_center', 'catalog_page', 'customer', 'customer_address',
                'customer_demographics', 'date_dim', 'household_demographics',
                'income_band', 'item', 'promotion', 'reason', 'ship_mode',
                'store', 'time_dim', 'warehouse', 'web_page', 'web_site'
            ]
            
            logger.info("Generating dimension tables...")
            for table_name in dimension_tables:
                expected_rows = self.row_counts[table_name]
                logger.info(f"Generating {table_name} (expected: {expected_rows:,} rows)...")
                df = self._generate_table_data(table_name)
                actual_rows = len(df)
                generated_tables[table_name] = actual_rows
                total_rows_generated += actual_rows
                
                file_path = os.path.join(self.data_dir, f"{table_name}.dat")
                self._save_dat_file(df, file_path)
                generated_files.append(file_path)
                
                logger.info(f"✓ {table_name}: {actual_rows:,} rows generated")
            
            # Generate fact tables
            fact_tables = [
                'catalog_returns', 'catalog_sales', 'inventory',
                'store_returns', 'store_sales', 'web_returns', 'web_sales'
            ]
            
            logger.info("Generating fact tables...")
            for table_name in fact_tables:
                expected_rows = self.row_counts[table_name]
                logger.info(f"Generating {table_name} (expected: {expected_rows:,} rows)...")
                df = self._generate_table_data(table_name)
                actual_rows = len(df)
                generated_tables[table_name] = actual_rows
                total_rows_generated += actual_rows
                
                file_path = os.path.join(self.data_dir, f"{table_name}.dat")
                self._save_dat_file(df, file_path)
                generated_files.append(file_path)
                
                logger.info(f"✓ {table_name}: {actual_rows:,} rows generated")
            
            generation_time = time.time() - start_time
            
            logger.info(f"Data generation completed in {generation_time:.2f} seconds")
            logger.info(f"Generated {len(generated_files)} files")
            logger.info(f"Total rows generated: {total_rows_generated:,}")
            
            return {
                'status': 'success',
                'generation_time': generation_time,
                'scale_factor': self.scale_factor,
                'data_directory': self.data_dir,
                'generated_files': generated_files,
                'total_files': len(generated_files),
                'generated_tables': generated_tables,
                'total_rows_generated': total_rows_generated
            }
            
        except Exception as e:
            logger.error(f"Data generation failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'generation_time': time.time() - start_time,
                'generated_files': generated_files
            }
    
    def _generate_table_data(self, table_name: str) -> pd.DataFrame:
        """Generate data for a specific table"""
        row_count = self.row_counts[table_name]
        
        if table_name == 'call_center':
            return self._generate_call_center_data(row_count)
        elif table_name == 'catalog_page':
            return self._generate_catalog_page_data(row_count)
        elif table_name == 'customer':
            return self._generate_customer_data(row_count)
        elif table_name == 'customer_address':
            return self._generate_customer_address_data(row_count)
        elif table_name == 'customer_demographics':
            return self._generate_customer_demographics_data(row_count)
        elif table_name == 'date_dim':
            return self._generate_date_dim_data(row_count)
        elif table_name == 'household_demographics':
            return self._generate_household_demographics_data(row_count)
        elif table_name == 'income_band':
            return self._generate_income_band_data(row_count)
        elif table_name == 'item':
            return self._generate_item_data(row_count)
        elif table_name == 'promotion':
            return self._generate_promotion_data(row_count)
        elif table_name == 'reason':
            return self._generate_reason_data(row_count)
        elif table_name == 'ship_mode':
            return self._generate_ship_mode_data(row_count)
        elif table_name == 'store':
            return self._generate_store_data(row_count)
        elif table_name == 'time_dim':
            return self._generate_time_dim_data(row_count)
        elif table_name == 'warehouse':
            return self._generate_warehouse_data(row_count)
        elif table_name == 'web_page':
            return self._generate_web_page_data(row_count)
        elif table_name == 'web_site':
            return self._generate_web_site_data(row_count)
        elif table_name == 'catalog_returns':
            return self._generate_catalog_returns_data(row_count)
        elif table_name == 'catalog_sales':
            return self._generate_catalog_sales_data(row_count)
        elif table_name == 'inventory':
            return self._generate_inventory_data(row_count)
        elif table_name == 'store_returns':
            return self._generate_store_returns_data(row_count)
        elif table_name == 'store_sales':
            return self._generate_store_sales_data(row_count)
        elif table_name == 'web_returns':
            return self._generate_web_returns_data(row_count)
        elif table_name == 'web_sales':
            return self._generate_web_sales_data(row_count)
        else:
            raise ValueError(f"Unknown table: {table_name}")
    
    def _generate_call_center_data(self, row_count: int) -> pd.DataFrame:
        """Generate call center data"""
        data = {
            'cc_call_center_sk': range(1, row_count + 1),
            'cc_call_center_id': [f"CC{i:04d}" for i in range(1, row_count + 1)],
            'cc_rec_start_date': [self._random_date() for _ in range(row_count)],
            'cc_rec_end_date': [self._random_date() for _ in range(row_count)],
            'cc_closed_date_sk': [self._random_sk() for _ in range(row_count)],
            'cc_open_date_sk': [self._random_sk() for _ in range(row_count)],
            'cc_name': [f"Call Center {i}" for i in range(1, row_count + 1)],
            'cc_class': np.random.choice(['Small', 'Medium', 'Large'], row_count),
            'cc_employees': np.random.randint(10, 100, row_count),
            'cc_sq_ft': np.random.randint(1000, 10000, row_count),
            'cc_hours': np.random.choice(['8AM-5PM', '9AM-6PM', '24/7'], row_count),
            'cc_manager': [f"Manager {i}" for i in range(1, row_count + 1)],
            'cc_mkt_id': np.random.randint(1, 10, row_count),
            'cc_mkt_class': np.random.choice(['A', 'B', 'C'], row_count),
            'cc_mkt_desc': [f"Market {i}" for i in range(1, row_count + 1)],
            'cc_market_manager': [f"Market Manager {i}" for i in range(1, row_count + 1)],
            'cc_division': np.random.randint(1, 5, row_count),
            'cc_division_name': [f"Division {i}" for i in range(1, row_count + 1)],
            'cc_company': np.random.randint(1, 3, row_count),
            'cc_company_name': [f"Company {i}" for i in range(1, row_count + 1)],
            'cc_street_number': [str(np.random.randint(1, 9999)) for _ in range(row_count)],
            'cc_street_name': [f"Street {i}" for i in range(1, row_count + 1)],
            'cc_street_type': np.random.choice(['St', 'Ave', 'Blvd', 'Rd'], row_count),
            'cc_suite_number': [str(np.random.randint(100, 999)) for _ in range(row_count)],
            'cc_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], row_count),
            'cc_county': [f"County {i}" for i in range(1, row_count + 1)],
            'cc_state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], row_count),
            'cc_zip': [str(np.random.randint(10000, 99999)) for _ in range(row_count)],
            'cc_country': ['USA'] * row_count,
            'cc_gmt_offset': np.random.uniform(-8, -5, row_count).round(2),
            'cc_tax_percentage': np.random.uniform(5, 10, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_catalog_page_data(self, row_count: int) -> pd.DataFrame:
        """Generate catalog page data"""
        data = {
            'cp_catalog_page_sk': range(1, row_count + 1),
            'cp_catalog_page_id': [f"CP{i:06d}" for i in range(1, row_count + 1)],
            'cp_start_date_sk': [self._random_sk() for _ in range(row_count)],
            'cp_end_date_sk': [self._random_sk() for _ in range(row_count)],
            'cp_department': np.random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books'], row_count),
            'cp_catalog_number': np.random.randint(1, 100, row_count),
            'cp_catalog_page_number': np.random.randint(1, 1000, row_count),
            'cp_description': [f"Catalog page {i} description" for i in range(1, row_count + 1)],
            'cp_type': np.random.choice(['Type A', 'Type B', 'Type C'], row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_customer_data(self, row_count: int) -> pd.DataFrame:
        """Generate customer data"""
        data = {
            'c_customer_sk': range(1, row_count + 1),
            'c_customer_id': [f"C{i:08d}" for i in range(1, row_count + 1)],
            'c_current_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'c_current_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'c_current_addr_sk': [self._random_sk() for _ in range(row_count)],
            'c_first_shipto_date_sk': [self._random_sk() for _ in range(row_count)],
            'c_first_sales_date_sk': [self._random_sk() for _ in range(row_count)],
            'c_salutation': np.random.choice(['Mr.', 'Ms.', 'Mrs.', 'Dr.'], row_count),
            'c_first_name': [f"FirstName{i}" for i in range(1, row_count + 1)],
            'c_last_name': [f"LastName{i}" for i in range(1, row_count + 1)],
            'c_preferred_cust_flag': np.random.choice(['Y', 'N'], row_count),
            'c_birth_day': np.random.randint(1, 29, row_count),
            'c_birth_month': np.random.randint(1, 13, row_count),
            'c_birth_year': np.random.randint(1950, 2000, row_count),
            'c_birth_country': np.random.choice(['USA', 'Canada', 'Mexico'], row_count),
            'c_login': [f"login{i}" for i in range(1, row_count + 1)],
            'c_email_address': [f"customer{i}@email.com" for i in range(1, row_count + 1)],
            'c_last_review_date_sk': [self._random_sk() for _ in range(row_count)]
        }
        return pd.DataFrame(data)
    
    def _generate_customer_address_data(self, row_count: int) -> pd.DataFrame:
        """Generate customer address data"""
        data = {
            'ca_address_sk': range(1, row_count + 1),
            'ca_address_id': [f"CA{i:08d}" for i in range(1, row_count + 1)],
            'ca_street_number': [str(np.random.randint(1, 9999)) for _ in range(row_count)],
            'ca_street_name': [f"Street {i}" for i in range(1, row_count + 1)],
            'ca_street_type': np.random.choice(['St', 'Ave', 'Blvd', 'Rd'], row_count),
            'ca_suite_number': [str(np.random.randint(100, 999)) for _ in range(row_count)],
            'ca_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], row_count),
            'ca_county': [f"County {i}" for i in range(1, row_count + 1)],
            'ca_state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], row_count),
            'ca_zip': [str(np.random.randint(10000, 99999)) for _ in range(row_count)],
            'ca_country': ['USA'] * row_count,
            'ca_gmt_offset': np.random.uniform(-8, -5, row_count).round(2),
            'ca_location_type': np.random.choice(['Residential', 'Commercial', 'Industrial'], row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_customer_demographics_data(self, row_count: int) -> pd.DataFrame:
        """Generate customer demographics data"""
        data = {
            'cd_demo_sk': range(1, row_count + 1),
            'cd_gender': np.random.choice(['M', 'F'], row_count),
            'cd_marital_status': np.random.choice(['S', 'M', 'D', 'W'], row_count),
            'cd_education_status': np.random.choice(['High School', 'College', 'Graduate', 'PhD'], row_count),
            'cd_purchase_estimate': np.random.randint(1000, 50000, row_count),
            'cd_credit_rating': np.random.choice(['Excellent', 'Good', 'Fair', 'Poor'], row_count),
            'cd_dep_count': np.random.randint(0, 5, row_count),
            'cd_dep_employed_count': np.random.randint(0, 3, row_count),
            'cd_dep_college_count': np.random.randint(0, 2, row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_date_dim_data(self, row_count: int) -> pd.DataFrame:
        """Generate date dimension data"""
        start_date = datetime(2000, 1, 1)
        dates = [start_date + timedelta(days=i) for i in range(row_count)]
        
        data = {
            'd_date_sk': range(1, row_count + 1),
            'd_date_id': [f"D{i:08d}" for i in range(1, row_count + 1)],
            'd_date': dates,
            'd_month_seq': [(d.year - 2000) * 12 + d.month for d in dates],
            'd_week_seq': [(d - datetime(2000, 1, 1)).days // 7 for d in dates],
            'd_quarter_seq': [(d.year - 2000) * 4 + (d.month - 1) // 3 for d in dates],
            'd_year': [d.year for d in dates],
            'd_dow': [d.weekday() for d in dates],
            'd_moy': [d.month for d in dates],
            'd_dom': [d.day for d in dates],
            'd_qoy': [(d.month - 1) // 3 + 1 for d in dates],
            'd_fy_year': [d.year for d in dates],
            'd_fy_quarter_seq': [(d.year - 2000) * 4 + (d.month - 1) // 3 for d in dates],
            'd_fy_week_seq': [(d - datetime(2000, 1, 1)).days // 7 for d in dates],
            'd_day_name': [d.strftime('%A') for d in dates],
            'd_quarter_name': [f"Q{(d.month - 1) // 3 + 1}" for d in dates],
            'd_holiday': ['N'] * row_count,
            'd_weekend': ['Y' if d.weekday() >= 5 else 'N' for d in dates],
            'd_following_holiday': ['N'] * row_count,
            'd_first_dom': [1] * row_count,
            'd_last_dom': [d.day for d in dates],
            'd_same_day_ly': [0] * row_count,
            'd_same_day_lq': [0] * row_count,
            'd_current_day': ['N'] * row_count,
            'd_current_week': ['N'] * row_count,
            'd_current_month': ['N'] * row_count,
            'd_current_quarter': ['N'] * row_count,
            'd_current_year': ['N'] * row_count
        }
        return pd.DataFrame(data)
    
    def _generate_household_demographics_data(self, row_count: int) -> pd.DataFrame:
        """Generate household demographics data"""
        data = {
            'hd_demo_sk': range(1, row_count + 1),
            'hd_income_band_sk': [self._random_sk() for _ in range(row_count)],
            'hd_buy_potential': np.random.choice(['Low', 'Medium', 'High'], row_count),
            'hd_dep_count': np.random.randint(0, 5, row_count),
            'hd_vehicle_count': np.random.randint(0, 3, row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_income_band_data(self, row_count: int) -> pd.DataFrame:
        """Generate income band data"""
        data = {
            'ib_income_band_sk': range(1, row_count + 1),
            'ib_lower_bound': [i * 10000 for i in range(row_count)],
            'ib_upper_bound': [(i + 1) * 10000 for i in range(row_count)]
        }
        return pd.DataFrame(data)
    
    def _generate_item_data(self, row_count: int) -> pd.DataFrame:
        """Generate item data"""
        data = {
            'i_item_sk': range(1, row_count + 1),
            'i_item_id': [f"I{i:08d}" for i in range(1, row_count + 1)],
            'i_rec_start_date': [self._random_date() for _ in range(row_count)],
            'i_rec_end_date': [self._random_date() for _ in range(row_count)],
            'i_item_desc': [f"Item description {i}" for i in range(1, row_count + 1)],
            'i_current_price': np.random.uniform(10, 1000, row_count).round(2),
            'i_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'i_brand_id': np.random.randint(1, 100, row_count),
            'i_brand': [f"Brand {i}" for i in range(1, row_count + 1)],
            'i_class_id': np.random.randint(1, 50, row_count),
            'i_class': [f"Class {i}" for i in range(1, row_count + 1)],
            'i_category_id': np.random.randint(1, 20, row_count),
            'i_category': [f"Category {i}" for i in range(1, row_count + 1)],
            'i_manufact_id': np.random.randint(1, 200, row_count),
            'i_manufact': [f"Manufacturer {i}" for i in range(1, row_count + 1)],
            'i_size': np.random.choice(['S', 'M', 'L', 'XL'], row_count),
            'i_formulation': [f"Formulation {i}" for i in range(1, row_count + 1)],
            'i_color': np.random.choice(['Red', 'Blue', 'Green', 'Black', 'White'], row_count),
            'i_units': np.random.choice(['Each', 'Box', 'Case'], row_count),
            'i_container': np.random.choice(['Box', 'Bag', 'Bottle'], row_count),
            'i_manager_id': np.random.randint(1, 100, row_count),
            'i_product_name': [f"Product {i}" for i in range(1, row_count + 1)]
        }
        return pd.DataFrame(data)
    
    def _generate_promotion_data(self, row_count: int) -> pd.DataFrame:
        """Generate promotion data"""
        data = {
            'p_promo_sk': range(1, row_count + 1),
            'p_promo_id': [f"P{i:08d}" for i in range(1, row_count + 1)],
            'p_start_date_sk': [self._random_sk() for _ in range(row_count)],
            'p_end_date_sk': [self._random_sk() for _ in range(row_count)],
            'p_item_sk': [self._random_sk() for _ in range(row_count)],
            'p_cost': np.random.uniform(100, 10000, row_count).round(2),
            'p_response_target': np.random.randint(1000, 100000, row_count),
            'p_promo_name': [f"Promotion {i}" for i in range(1, row_count + 1)],
            'p_channel_dmail': np.random.choice(['Y', 'N'], row_count),
            'p_channel_email': np.random.choice(['Y', 'N'], row_count),
            'p_channel_catalog': np.random.choice(['Y', 'N'], row_count),
            'p_channel_tv': np.random.choice(['Y', 'N'], row_count),
            'p_channel_radio': np.random.choice(['Y', 'N'], row_count),
            'p_channel_press': np.random.choice(['Y', 'N'], row_count),
            'p_channel_event': np.random.choice(['Y', 'N'], row_count),
            'p_channel_demo': np.random.choice(['Y', 'N'], row_count),
            'p_channel_details': [f"Channel details {i}" for i in range(1, row_count + 1)],
            'p_purpose': np.random.choice(['Purpose A', 'Purpose B', 'Purpose C'], row_count),
            'p_discount_active': np.random.choice(['Y', 'N'], row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_reason_data(self, row_count: int) -> pd.DataFrame:
        """Generate reason data"""
        data = {
            'r_reason_sk': range(1, row_count + 1),
            'r_reason_id': [f"R{i:08d}" for i in range(1, row_count + 1)],
            'r_reason_desc': [f"Reason description {i}" for i in range(1, row_count + 1)]
        }
        return pd.DataFrame(data)
    
    def _generate_ship_mode_data(self, row_count: int) -> pd.DataFrame:
        """Generate ship mode data"""
        data = {
            'sm_ship_mode_sk': range(1, row_count + 1),
            'sm_ship_mode_id': [f"SM{i:08d}" for i in range(1, row_count + 1)],
            'sm_type': np.random.choice(['Standard', 'Express', 'Overnight'], row_count),
            'sm_code': [f"SM{i:02d}" for i in range(1, row_count + 1)],
            'sm_carrier': np.random.choice(['UPS', 'FedEx', 'USPS'], row_count),
            'sm_contract': [f"Contract {i}" for i in range(1, row_count + 1)]
        }
        return pd.DataFrame(data)
    
    def _generate_store_data(self, row_count: int) -> pd.DataFrame:
        """Generate store data"""
        data = {
            's_store_sk': range(1, row_count + 1),
            's_store_id': [f"S{i:08d}" for i in range(1, row_count + 1)],
            's_rec_start_date': [self._random_date() for _ in range(row_count)],
            's_rec_end_date': [self._random_date() for _ in range(row_count)],
            's_closed_date_sk': [self._random_sk() for _ in range(row_count)],
            's_store_name': [f"Store {i}" for i in range(1, row_count + 1)],
            's_number_employees': np.random.randint(5, 50, row_count),
            's_floor_space': np.random.randint(1000, 10000, row_count),
            's_hours': np.random.choice(['8AM-9PM', '9AM-10PM', '24/7'], row_count),
            's_manager': [f"Manager {i}" for i in range(1, row_count + 1)],
            's_market_id': np.random.randint(1, 10, row_count),
            's_geography_class': [f"Geography {i}" for i in range(1, row_count + 1)],
            's_market_desc': [f"Market {i}" for i in range(1, row_count + 1)],
            's_market_manager': [f"Market Manager {i}" for i in range(1, row_count + 1)],
            's_division_id': np.random.randint(1, 5, row_count),
            's_division_name': [f"Division {i}" for i in range(1, row_count + 1)],
            's_company_id': np.random.randint(1, 3, row_count),
            's_company_name': [f"Company {i}" for i in range(1, row_count + 1)],
            's_street_number': [str(np.random.randint(1, 9999)) for _ in range(row_count)],
            's_street_name': [f"Street {i}" for i in range(1, row_count + 1)],
            's_street_type': np.random.choice(['St', 'Ave', 'Blvd', 'Rd'], row_count),
            's_suite_number': [str(np.random.randint(100, 999)) for _ in range(row_count)],
            's_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], row_count),
            's_county': [f"County {i}" for i in range(1, row_count + 1)],
            's_state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], row_count),
            's_zip': [str(np.random.randint(10000, 99999)) for _ in range(row_count)],
            's_country': ['USA'] * row_count,
            's_gmt_offset': np.random.uniform(-8, -5, row_count).round(2),
            's_tax_precentage': np.random.uniform(5, 10, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_time_dim_data(self, row_count: int) -> pd.DataFrame:
        """Generate time dimension data"""
        data = {
            't_time_sk': range(1, row_count + 1),
            't_time_id': [f"T{i:08d}" for i in range(1, row_count + 1)],
            't_time': [i for i in range(row_count)],
            't_hour': [i % 24 for i in range(row_count)],
            't_minute': [i % 60 for i in range(row_count)],
            't_second': [i % 60 for i in range(row_count)],
            't_am_pm': ['AM' if i % 24 < 12 else 'PM' for i in range(row_count)],
            't_shift': ['Morning' if i % 24 < 8 else 'Afternoon' if i % 24 < 16 else 'Evening' for i in range(row_count)],
            't_sub_shift': [f"SubShift {i % 4}" for i in range(row_count)],
            't_meal_time': ['Breakfast' if i % 24 < 8 else 'Lunch' if i % 24 < 14 else 'Dinner' for i in range(row_count)]
        }
        return pd.DataFrame(data)
    
    def _generate_warehouse_data(self, row_count: int) -> pd.DataFrame:
        """Generate warehouse data"""
        data = {
            'w_warehouse_sk': range(1, row_count + 1),
            'w_warehouse_id': [f"W{i:08d}" for i in range(1, row_count + 1)],
            'w_warehouse_name': [f"Warehouse {i}" for i in range(1, row_count + 1)],
            'w_warehouse_sq_ft': np.random.randint(10000, 100000, row_count),
            'w_street_number': [str(np.random.randint(1, 9999)) for _ in range(row_count)],
            'w_street_name': [f"Street {i}" for i in range(1, row_count + 1)],
            'w_street_type': np.random.choice(['St', 'Ave', 'Blvd', 'Rd'], row_count),
            'w_suite_number': [str(np.random.randint(100, 999)) for _ in range(row_count)],
            'w_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], row_count),
            'w_county': [f"County {i}" for i in range(1, row_count + 1)],
            'w_state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], row_count),
            'w_zip': [str(np.random.randint(10000, 99999)) for _ in range(row_count)],
            'w_country': ['USA'] * row_count,
            'w_gmt_offset': np.random.uniform(-8, -5, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_web_page_data(self, row_count: int) -> pd.DataFrame:
        """Generate web page data"""
        data = {
            'wp_web_page_sk': range(1, row_count + 1),
            'wp_web_page_id': [f"WP{i:08d}" for i in range(1, row_count + 1)],
            'wp_rec_start_date': [self._random_date() for _ in range(row_count)],
            'wp_rec_end_date': [self._random_date() for _ in range(row_count)],
            'wp_creation_date_sk': [self._random_sk() for _ in range(row_count)],
            'wp_access_date_sk': [self._random_sk() for _ in range(row_count)],
            'wp_autogen_flag': np.random.choice(['Y', 'N'], row_count),
            'wp_customer_sk': [self._random_sk() for _ in range(row_count)],
            'wp_url': [f"https://example.com/page{i}" for i in range(1, row_count + 1)],
            'wp_type': np.random.choice(['Type A', 'Type B', 'Type C'], row_count),
            'wp_char_count': np.random.randint(100, 10000, row_count),
            'wp_link_count': np.random.randint(0, 100, row_count),
            'wp_image_count': np.random.randint(0, 50, row_count),
            'wp_max_ad_count': np.random.randint(0, 10, row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_web_site_data(self, row_count: int) -> pd.DataFrame:
        """Generate web site data"""
        data = {
            'web_site_sk': range(1, row_count + 1),
            'web_site_id': [f"WS{i:08d}" for i in range(1, row_count + 1)],
            'web_rec_start_date': [self._random_date() for _ in range(row_count)],
            'web_rec_end_date': [self._random_date() for _ in range(row_count)],
            'web_name': [f"Website {i}" for i in range(1, row_count + 1)],
            'web_open_date_sk': [self._random_sk() for _ in range(row_count)],
            'web_close_date_sk': [self._random_sk() for _ in range(row_count)],
            'web_class': np.random.choice(['Class A', 'Class B', 'Class C'], row_count),
            'web_manager': [f"Manager {i}" for i in range(1, row_count + 1)],
            'web_mkt_id': np.random.randint(1, 10, row_count),
            'web_mkt_class': np.random.choice(['A', 'B', 'C'], row_count),
            'web_mkt_desc': [f"Market {i}" for i in range(1, row_count + 1)],
            'web_market_manager': [f"Market Manager {i}" for i in range(1, row_count + 1)],
            'web_company_id': np.random.randint(1, 3, row_count),
            'web_company_name': [f"Company {i}" for i in range(1, row_count + 1)],
            'web_street_number': [str(np.random.randint(1, 9999)) for _ in range(row_count)],
            'web_street_name': [f"Street {i}" for i in range(1, row_count + 1)],
            'web_street_type': np.random.choice(['St', 'Ave', 'Blvd', 'Rd'], row_count),
            'web_suite_number': [str(np.random.randint(100, 999)) for _ in range(row_count)],
            'web_city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'], row_count),
            'web_county': [f"County {i}" for i in range(1, row_count + 1)],
            'web_state': np.random.choice(['NY', 'CA', 'IL', 'TX', 'AZ'], row_count),
            'web_zip': [str(np.random.randint(10000, 99999)) for _ in range(row_count)],
            'web_country': ['USA'] * row_count,
            'web_gmt_offset': np.random.uniform(-8, -5, row_count).round(2),
            'web_tax_percentage': np.random.uniform(5, 10, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_catalog_returns_data(self, row_count: int) -> pd.DataFrame:
        """Generate catalog returns data"""
        data = {
            'cr_returned_date_sk': [self._random_sk() for _ in range(row_count)],
            'cr_returned_time_sk': [self._random_sk() for _ in range(row_count)],
            'cr_item_sk': [self._random_sk() for _ in range(row_count)],
            'cr_refunded_customer_sk': [self._random_sk() for _ in range(row_count)],
            'cr_refunded_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cr_refunded_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cr_refunded_addr_sk': [self._random_sk() for _ in range(row_count)],
            'cr_returning_customer_sk': [self._random_sk() for _ in range(row_count)],
            'cr_returning_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cr_returning_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cr_returning_addr_sk': [self._random_sk() for _ in range(row_count)],
            'cr_call_center_sk': [self._random_sk() for _ in range(row_count)],
            'cr_catalog_page_sk': [self._random_sk() for _ in range(row_count)],
            'cr_ship_mode_sk': [self._random_sk() for _ in range(row_count)],
            'cr_warehouse_sk': [self._random_sk() for _ in range(row_count)],
            'cr_reason_sk': [self._random_sk() for _ in range(row_count)],
            'cr_order_number': np.random.randint(1000000, 9999999, row_count),
            'cr_return_quantity': np.random.randint(1, 10, row_count),
            'cr_return_amount': np.random.uniform(10, 1000, row_count).round(2),
            'cr_return_tax': np.random.uniform(1, 100, row_count).round(2),
            'cr_return_amt_inc_tax': np.random.uniform(11, 1100, row_count).round(2),
            'cr_fee': np.random.uniform(0, 50, row_count).round(2),
            'cr_return_ship_cost': np.random.uniform(0, 100, row_count).round(2),
            'cr_refunded_cash': np.random.uniform(0, 1000, row_count).round(2),
            'cr_reversed_charge': np.random.uniform(0, 1000, row_count).round(2),
            'cr_store_credit': np.random.uniform(0, 1000, row_count).round(2),
            'cr_net_loss': np.random.uniform(0, 100, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_catalog_sales_data(self, row_count: int) -> pd.DataFrame:
        """Generate catalog sales data"""
        data = {
            'cs_sold_date_sk': [self._random_sk() for _ in range(row_count)],
            'cs_sold_time_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_date_sk': [self._random_sk() for _ in range(row_count)],
            'cs_bill_customer_sk': [self._random_sk() for _ in range(row_count)],
            'cs_bill_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cs_bill_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cs_bill_addr_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_customer_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_addr_sk': [self._random_sk() for _ in range(row_count)],
            'cs_call_center_sk': [self._random_sk() for _ in range(row_count)],
            'cs_catalog_page_sk': [self._random_sk() for _ in range(row_count)],
            'cs_ship_mode_sk': [self._random_sk() for _ in range(row_count)],
            'cs_warehouse_sk': [self._random_sk() for _ in range(row_count)],
            'cs_item_sk': [self._random_sk() for _ in range(row_count)],
            'cs_promo_sk': [self._random_sk() for _ in range(row_count)],
            'cs_order_number': np.random.randint(1000000, 9999999, row_count),
            'cs_quantity': np.random.randint(1, 10, row_count),
            'cs_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'cs_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'cs_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'cs_ext_discount_amt': np.random.uniform(0, 100, row_count).round(2),
            'cs_ext_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'cs_ext_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'cs_ext_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'cs_ext_tax': np.random.uniform(0, 100, row_count).round(2),
            'cs_coupon_amt': np.random.uniform(0, 50, row_count).round(2),
            'cs_ext_ship_cost': np.random.uniform(0, 100, row_count).round(2),
            'cs_net_paid': np.random.uniform(8, 800, row_count).round(2),
            'cs_net_paid_inc_tax': np.random.uniform(8, 900, row_count).round(2),
            'cs_net_paid_inc_ship': np.random.uniform(8, 900, row_count).round(2),
            'cs_net_paid_inc_ship_tax': np.random.uniform(8, 1000, row_count).round(2),
            'cs_net_profit': np.random.uniform(0, 200, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_inventory_data(self, row_count: int) -> pd.DataFrame:
        """Generate inventory data"""
        data = {
            'inv_date_sk': [self._random_sk() for _ in range(row_count)],
            'inv_item_sk': [self._random_sk() for _ in range(row_count)],
            'inv_warehouse_sk': [self._random_sk() for _ in range(row_count)],
            'inv_quantity_on_hand': np.random.randint(0, 1000, row_count)
        }
        return pd.DataFrame(data)
    
    def _generate_store_returns_data(self, row_count: int) -> pd.DataFrame:
        """Generate store returns data"""
        data = {
            'sr_returned_date_sk': [self._random_sk() for _ in range(row_count)],
            'sr_return_time_sk': [self._random_sk() for _ in range(row_count)],
            'sr_item_sk': [self._random_sk() for _ in range(row_count)],
            'sr_customer_sk': [self._random_sk() for _ in range(row_count)],
            'sr_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'sr_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'sr_addr_sk': [self._random_sk() for _ in range(row_count)],
            'sr_store_sk': [self._random_sk() for _ in range(row_count)],
            'sr_reason_sk': [self._random_sk() for _ in range(row_count)],
            'sr_ticket_number': np.random.randint(1000000, 9999999, row_count),
            'sr_return_quantity': np.random.randint(1, 10, row_count),
            'sr_return_amt': np.random.uniform(10, 1000, row_count).round(2),
            'sr_return_tax': np.random.uniform(1, 100, row_count).round(2),
            'sr_return_amt_inc_tax': np.random.uniform(11, 1100, row_count).round(2),
            'sr_fee': np.random.uniform(0, 50, row_count).round(2),
            'sr_return_ship_cost': np.random.uniform(0, 100, row_count).round(2),
            'sr_refunded_cash': np.random.uniform(0, 1000, row_count).round(2),
            'sr_reversed_charge': np.random.uniform(0, 1000, row_count).round(2),
            'sr_store_credit': np.random.uniform(0, 1000, row_count).round(2),
            'sr_net_loss': np.random.uniform(0, 100, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_store_sales_data(self, row_count: int) -> pd.DataFrame:
        """Generate store sales data"""
        data = {
            'ss_sold_date_sk': [self._random_sk() for _ in range(row_count)],
            'ss_sold_time_sk': [self._random_sk() for _ in range(row_count)],
            'ss_item_sk': [self._random_sk() for _ in range(row_count)],
            'ss_customer_sk': [self._random_sk() for _ in range(row_count)],
            'ss_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ss_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ss_addr_sk': [self._random_sk() for _ in range(row_count)],
            'ss_store_sk': [self._random_sk() for _ in range(row_count)],
            'ss_promo_sk': [self._random_sk() for _ in range(row_count)],
            'ss_ticket_number': np.random.randint(1000000, 9999999, row_count),
            'ss_quantity': np.random.randint(1, 10, row_count),
            'ss_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'ss_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'ss_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'ss_ext_discount_amt': np.random.uniform(0, 100, row_count).round(2),
            'ss_ext_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'ss_ext_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'ss_ext_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'ss_ext_tax': np.random.uniform(0, 100, row_count).round(2),
            'ss_coupon_amt': np.random.uniform(0, 50, row_count).round(2),
            'ss_net_paid': np.random.uniform(8, 800, row_count).round(2),
            'ss_net_paid_inc_tax': np.random.uniform(8, 900, row_count).round(2),
            'ss_net_profit': np.random.uniform(0, 200, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_web_returns_data(self, row_count: int) -> pd.DataFrame:
        """Generate web returns data"""
        data = {
            'wr_returned_date_sk': [self._random_sk() for _ in range(row_count)],
            'wr_returned_time_sk': [self._random_sk() for _ in range(row_count)],
            'wr_item_sk': [self._random_sk() for _ in range(row_count)],
            'wr_refunded_customer_sk': [self._random_sk() for _ in range(row_count)],
            'wr_refunded_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'wr_refunded_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'wr_refunded_addr_sk': [self._random_sk() for _ in range(row_count)],
            'wr_returning_customer_sk': [self._random_sk() for _ in range(row_count)],
            'wr_returning_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'wr_returning_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'wr_returning_addr_sk': [self._random_sk() for _ in range(row_count)],
            'wr_web_page_sk': [self._random_sk() for _ in range(row_count)],
            'wr_reason_sk': [self._random_sk() for _ in range(row_count)],
            'wr_order_number': np.random.randint(1000000, 9999999, row_count),
            'wr_return_quantity': np.random.randint(1, 10, row_count),
            'wr_return_amt': np.random.uniform(10, 1000, row_count).round(2),
            'wr_return_tax': np.random.uniform(1, 100, row_count).round(2),
            'wr_return_amt_inc_tax': np.random.uniform(11, 1100, row_count).round(2),
            'wr_fee': np.random.uniform(0, 50, row_count).round(2),
            'wr_return_ship_cost': np.random.uniform(0, 100, row_count).round(2),
            'wr_refunded_cash': np.random.uniform(0, 1000, row_count).round(2),
            'wr_reversed_charge': np.random.uniform(0, 1000, row_count).round(2),
            'wr_account_credit': np.random.uniform(0, 1000, row_count).round(2),
            'wr_net_loss': np.random.uniform(0, 100, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _generate_web_sales_data(self, row_count: int) -> pd.DataFrame:
        """Generate web sales data"""
        data = {
            'ws_sold_date_sk': [self._random_sk() for _ in range(row_count)],
            'ws_sold_time_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_date_sk': [self._random_sk() for _ in range(row_count)],
            'ws_item_sk': [self._random_sk() for _ in range(row_count)],
            'ws_bill_customer_sk': [self._random_sk() for _ in range(row_count)],
            'ws_bill_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ws_bill_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ws_bill_addr_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_customer_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_cdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_hdemo_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_addr_sk': [self._random_sk() for _ in range(row_count)],
            'ws_web_page_sk': [self._random_sk() for _ in range(row_count)],
            'ws_web_site_sk': [self._random_sk() for _ in range(row_count)],
            'ws_ship_mode_sk': [self._random_sk() for _ in range(row_count)],
            'ws_warehouse_sk': [self._random_sk() for _ in range(row_count)],
            'ws_promo_sk': [self._random_sk() for _ in range(row_count)],
            'ws_order_number': np.random.randint(1000000, 9999999, row_count),
            'ws_quantity': np.random.randint(1, 10, row_count),
            'ws_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'ws_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'ws_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'ws_ext_discount_amt': np.random.uniform(0, 100, row_count).round(2),
            'ws_ext_sales_price': np.random.uniform(8, 800, row_count).round(2),
            'ws_ext_wholesale_cost': np.random.uniform(5, 500, row_count).round(2),
            'ws_ext_list_price': np.random.uniform(10, 1000, row_count).round(2),
            'ws_ext_tax': np.random.uniform(0, 100, row_count).round(2),
            'ws_coupon_amt': np.random.uniform(0, 50, row_count).round(2),
            'ws_ext_ship_cost': np.random.uniform(0, 100, row_count).round(2),
            'ws_net_paid': np.random.uniform(8, 800, row_count).round(2),
            'ws_net_paid_inc_tax': np.random.uniform(8, 900, row_count).round(2),
            'ws_net_paid_inc_ship': np.random.uniform(8, 900, row_count).round(2),
            'ws_net_paid_inc_ship_tax': np.random.uniform(8, 1000, row_count).round(2),
            'ws_net_profit': np.random.uniform(0, 200, row_count).round(2)
        }
        return pd.DataFrame(data)
    
    def _random_sk(self) -> int:
        """Generate random surrogate key"""
        return np.random.randint(1, 1000000)
    
    def _random_date(self) -> str:
        """Generate random date string"""
        start_date = datetime(2000, 1, 1)
        random_days = np.random.randint(0, 365 * 10)  # 10 years
        random_date = start_date + timedelta(days=random_days)
        return random_date.strftime('%Y-%m-%d')
    
    def _save_dat_file(self, df: pd.DataFrame, file_path: str):
        """Save DataFrame as .dat file (pipe-separated values)"""
        # Convert DataFrame to pipe-separated format
        df.to_csv(file_path, sep='|', index=False, header=False, na_rep='')
    
    def convert_to_parquet(self) -> Dict[str, Any]:
        """Convert generated .dat files to Parquet format"""
        logger.info("Converting TPC-DS data to Parquet format")
        
        parquet_dir = os.path.join(self.data_dir, 'parquet')
        os.makedirs(parquet_dir, exist_ok=True)
        
        # TPC-DS table definitions
        table_definitions = self._get_table_definitions()
        
        converted_files = []
        
        try:
            for table_name, columns in table_definitions.items():
                dat_file = os.path.join(self.data_dir, f"{table_name}.dat")
                
                if not os.path.exists(dat_file):
                    logger.warning(f"Data file not found: {dat_file}")
                    continue
                
                # Read .dat file and convert to DataFrame
                df = self._read_dat_file(dat_file, columns)
                
                # Save as Parquet
                parquet_file = os.path.join(parquet_dir, f"{table_name}.parquet")
                df.to_parquet(parquet_file, index=False)
                converted_files.append(parquet_file)
                
                logger.info(f"Converted {table_name}: {len(df)} rows")
            
            return {
                'status': 'success',
                'parquet_directory': parquet_dir,
                'converted_files': converted_files,
                'total_files': len(converted_files)
            }
            
        except Exception as e:
            logger.error(f"Parquet conversion failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'converted_files': []
            }
    
    def _read_dat_file(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        """Read a .dat file and return as DataFrame"""
        try:
            # Read the file with proper delimiter
            df = pd.read_csv(file_path, sep='|', names=columns, na_values=[''])
            
            # Remove the last column if it's empty (common in TPC-DS files)
            if df.columns[-1].startswith('Unnamed'):
                df = df.iloc[:, :-1]
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            raise
    
    def _get_table_definitions(self) -> Dict[str, List[str]]:
        """Get TPC-DS table column definitions"""
        return {
            'call_center': [
                'cc_call_center_sk', 'cc_call_center_id', 'cc_rec_start_date', 'cc_rec_end_date',
                'cc_closed_date_sk', 'cc_open_date_sk', 'cc_name', 'cc_class', 'cc_employees',
                'cc_sq_ft', 'cc_hours', 'cc_manager', 'cc_mkt_id', 'cc_mkt_class', 'cc_mkt_desc',
                'cc_market_manager', 'cc_division', 'cc_division_name', 'cc_company', 'cc_company_name',
                'cc_street_number', 'cc_street_name', 'cc_street_type', 'cc_suite_number',
                'cc_city', 'cc_county', 'cc_state', 'cc_zip', 'cc_country', 'cc_gmt_offset', 'cc_tax_percentage'
            ],
            'catalog_page': [
                'cp_catalog_page_sk', 'cp_catalog_page_id', 'cp_start_date_sk', 'cp_end_date_sk',
                'cp_department', 'cp_catalog_number', 'cp_catalog_page_number', 'cp_description', 'cp_type'
            ],
            'customer': [
                'c_customer_sk', 'c_customer_id', 'c_current_cdemo_sk', 'c_current_hdemo_sk',
                'c_current_addr_sk', 'c_first_shipto_date_sk', 'c_first_sales_date_sk',
                'c_salutation', 'c_first_name', 'c_last_name', 'c_preferred_cust_flag',
                'c_birth_day', 'c_birth_month', 'c_birth_year', 'c_birth_country',
                'c_login', 'c_email_address', 'c_last_review_date_sk'
            ],
            'customer_address': [
                'ca_address_sk', 'ca_address_id', 'ca_street_number', 'ca_street_name',
                'ca_street_type', 'ca_suite_number', 'ca_city', 'ca_county', 'ca_state',
                'ca_zip', 'ca_country', 'ca_gmt_offset', 'ca_location_type'
            ],
            'customer_demographics': [
                'cd_demo_sk', 'cd_gender', 'cd_marital_status', 'cd_education_status',
                'cd_purchase_estimate', 'cd_credit_rating', 'cd_dep_count', 'cd_dep_employed_count', 'cd_dep_college_count'
            ],
            'date_dim': [
                'd_date_sk', 'd_date_id', 'd_date', 'd_month_seq', 'd_week_seq', 'd_quarter_seq',
                'd_year', 'd_dow', 'd_moy', 'd_dom', 'd_qoy', 'd_fy_year', 'd_fy_quarter_seq',
                'd_fy_week_seq', 'd_day_name', 'd_quarter_name', 'd_holiday', 'd_weekend',
                'd_following_holiday', 'd_first_dom', 'd_last_dom', 'd_same_day_ly', 'd_same_day_lq',
                'd_current_day', 'd_current_week', 'd_current_month', 'd_current_quarter', 'd_current_year'
            ],
            'household_demographics': [
                'hd_demo_sk', 'hd_income_band_sk', 'hd_buy_potential', 'hd_dep_count', 'hd_vehicle_count'
            ],
            'income_band': [
                'ib_income_band_sk', 'ib_lower_bound', 'ib_upper_bound'
            ],
            'item': [
                'i_item_sk', 'i_item_id', 'i_rec_start_date', 'i_rec_end_date', 'i_item_desc',
                'i_current_price', 'i_wholesale_cost', 'i_brand_id', 'i_brand', 'i_class_id',
                'i_class', 'i_category_id', 'i_category', 'i_manufact_id', 'i_manufact',
                'i_size', 'i_formulation', 'i_color', 'i_units', 'i_container', 'i_manager_id', 'i_product_name'
            ],
            'promotion': [
                'p_promo_sk', 'p_promo_id', 'p_start_date_sk', 'p_end_date_sk', 'p_item_sk',
                'p_cost', 'p_response_target', 'p_promo_name', 'p_channel_dmail', 'p_channel_email',
                'p_channel_catalog', 'p_channel_tv', 'p_channel_radio', 'p_channel_press',
                'p_channel_event', 'p_channel_demo', 'p_channel_details', 'p_purpose', 'p_discount_active'
            ],
            'reason': [
                'r_reason_sk', 'r_reason_id', 'r_reason_desc'
            ],
            'ship_mode': [
                'sm_ship_mode_sk', 'sm_ship_mode_id', 'sm_type', 'sm_code', 'sm_carrier', 'sm_contract'
            ],
            'store': [
                's_store_sk', 's_store_id', 's_rec_start_date', 's_rec_end_date', 's_closed_date_sk',
                's_store_name', 's_number_employees', 's_floor_space', 's_hours', 's_manager',
                's_market_id', 's_geography_class', 's_market_desc', 's_market_manager',
                's_division_id', 's_division_name', 's_company_id', 's_company_name',
                's_street_number', 's_street_name', 's_street_type', 's_suite_number',
                's_city', 's_county', 's_state', 's_zip', 's_country', 's_gmt_offset', 's_tax_precentage'
            ],
            'time_dim': [
                't_time_sk', 't_time_id', 't_time', 't_hour', 't_minute', 't_second',
                't_am_pm', 't_shift', 't_sub_shift', 't_meal_time'
            ],
            'warehouse': [
                'w_warehouse_sk', 'w_warehouse_id', 'w_warehouse_name', 'w_warehouse_sq_ft',
                'w_street_number', 'w_street_name', 'w_street_type', 'w_suite_number',
                'w_city', 'w_county', 'w_state', 'w_zip', 'w_country', 'w_gmt_offset'
            ],
            'web_page': [
                'wp_web_page_sk', 'wp_web_page_id', 'wp_rec_start_date', 'wp_rec_end_date',
                'wp_creation_date_sk', 'wp_access_date_sk', 'wp_autogen_flag', 'wp_customer_sk',
                'wp_url', 'wp_type', 'wp_char_count', 'wp_link_count', 'wp_image_count', 'wp_max_ad_count'
            ],
            'web_site': [
                'web_site_sk', 'web_site_id', 'web_rec_start_date', 'web_rec_end_date',
                'web_name', 'web_open_date_sk', 'web_close_date_sk', 'web_class', 'web_manager',
                'web_mkt_id', 'web_mkt_class', 'web_mkt_desc', 'web_market_manager',
                'web_company_id', 'web_company_name', 'web_street_number', 'web_street_name',
                'web_street_type', 'web_suite_number', 'web_city', 'web_county', 'web_state',
                'web_zip', 'web_country', 'web_gmt_offset', 'web_tax_percentage'
            ],
            'catalog_returns': [
                'cr_returned_date_sk', 'cr_returned_time_sk', 'cr_item_sk', 'cr_refunded_customer_sk',
                'cr_refunded_cdemo_sk', 'cr_refunded_hdemo_sk', 'cr_refunded_addr_sk',
                'cr_returning_customer_sk', 'cr_returning_cdemo_sk', 'cr_returning_hdemo_sk',
                'cr_returning_addr_sk', 'cr_call_center_sk', 'cr_catalog_page_sk', 'cr_ship_mode_sk',
                'cr_warehouse_sk', 'cr_reason_sk', 'cr_order_number', 'cr_return_quantity',
                'cr_return_amount', 'cr_return_tax', 'cr_return_amt_inc_tax', 'cr_fee',
                'cr_return_ship_cost', 'cr_refunded_cash', 'cr_reversed_charge', 'cr_store_credit', 'cr_net_loss'
            ],
            'catalog_sales': [
                'cs_sold_date_sk', 'cs_sold_time_sk', 'cs_ship_date_sk', 'cs_bill_customer_sk',
                'cs_bill_cdemo_sk', 'cs_bill_hdemo_sk', 'cs_bill_addr_sk', 'cs_ship_customer_sk',
                'cs_ship_cdemo_sk', 'cs_ship_hdemo_sk', 'cs_ship_addr_sk', 'cs_call_center_sk',
                'cs_catalog_page_sk', 'cs_ship_mode_sk', 'cs_warehouse_sk', 'cs_item_sk',
                'cs_promo_sk', 'cs_order_number', 'cs_quantity', 'cs_wholesale_cost',
                'cs_list_price', 'cs_sales_price', 'cs_ext_discount_amt', 'cs_ext_sales_price',
                'cs_ext_wholesale_cost', 'cs_ext_list_price', 'cs_ext_tax', 'cs_coupon_amt',
                'cs_ext_ship_cost', 'cs_net_paid', 'cs_net_paid_inc_tax', 'cs_net_paid_inc_ship',
                'cs_net_paid_inc_ship_tax', 'cs_net_profit'
            ],
            'inventory': [
                'inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk', 'inv_quantity_on_hand'
            ],
            'store_returns': [
                'sr_returned_date_sk', 'sr_return_time_sk', 'sr_item_sk', 'sr_customer_sk',
                'sr_cdemo_sk', 'sr_hdemo_sk', 'sr_addr_sk', 'sr_store_sk', 'sr_reason_sk',
                'sr_ticket_number', 'sr_return_quantity', 'sr_return_amt', 'sr_return_tax',
                'sr_return_amt_inc_tax', 'sr_fee', 'sr_return_ship_cost', 'sr_refunded_cash',
                'sr_reversed_charge', 'sr_store_credit', 'sr_net_loss'
            ],
            'store_sales': [
                'ss_sold_date_sk', 'ss_sold_time_sk', 'ss_item_sk', 'ss_customer_sk',
                'ss_cdemo_sk', 'ss_hdemo_sk', 'ss_addr_sk', 'ss_store_sk', 'ss_promo_sk',
                'ss_ticket_number', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price',
                'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_sales_price', 'ss_ext_wholesale_cost',
                'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid',
                'ss_net_paid_inc_tax', 'ss_net_profit'
            ],
            'web_returns': [
                'wr_returned_date_sk', 'wr_returned_time_sk', 'wr_item_sk', 'wr_refunded_customer_sk',
                'wr_refunded_cdemo_sk', 'wr_refunded_hdemo_sk', 'wr_refunded_addr_sk',
                'wr_returning_customer_sk', 'wr_returning_cdemo_sk', 'wr_returning_hdemo_sk',
                'wr_returning_addr_sk', 'wr_web_page_sk', 'wr_reason_sk', 'wr_order_number',
                'wr_return_quantity', 'wr_return_amt', 'wr_return_tax', 'wr_return_amt_inc_tax',
                'wr_fee', 'wr_return_ship_cost', 'wr_refunded_cash', 'wr_reversed_charge',
                'wr_account_credit', 'wr_net_loss'
            ],
            'web_sales': [
                'ws_sold_date_sk', 'ws_sold_time_sk', 'ws_ship_date_sk', 'ws_item_sk',
                'ws_bill_customer_sk', 'ws_bill_cdemo_sk', 'ws_bill_hdemo_sk', 'ws_bill_addr_sk',
                'ws_ship_customer_sk', 'ws_ship_cdemo_sk', 'ws_ship_hdemo_sk', 'ws_ship_addr_sk',
                'ws_web_page_sk', 'ws_web_site_sk', 'ws_ship_mode_sk', 'ws_warehouse_sk',
                'ws_promo_sk', 'ws_order_number', 'ws_quantity', 'ws_wholesale_cost',
                'ws_list_price', 'ws_sales_price', 'ws_ext_discount_amt', 'ws_ext_sales_price',
                'ws_ext_wholesale_cost', 'ws_ext_list_price', 'ws_ext_tax', 'ws_coupon_amt',
                'ws_ext_ship_cost', 'ws_net_paid', 'ws_net_paid_inc_tax', 'ws_net_paid_inc_ship',
                'ws_net_paid_inc_ship_tax', 'ws_net_profit'
            ]
        }
    
    def cleanup(self):
        """Clean up generated data files"""
        if os.path.exists(self.data_dir):
            import shutil
            shutil.rmtree(self.data_dir)
            logger.info(f"Cleaned up data directory: {self.data_dir}")


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mock TPC-DS Data Generator')
    parser.add_argument('--scale-factor', type=float, default=1.0, 
                       help='Scale factor for data generation (default: 1.0)')
    parser.add_argument('--data-dir', type=str, 
                       help='Directory to store generated data')
    parser.add_argument('--convert-parquet', action='store_true',
                       help='Convert generated data to Parquet format')
    parser.add_argument('--verbose', action='store_true', 
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    
    try:
        # Initialize generator
        generator = MockTPCDSDataGenerator(
            scale_factor=args.scale_factor,
            data_dir=args.data_dir
        )
        
        # Generate data
        print("🚀 Generating mock TPC-DS data...")
        result = generator.generate_data()
        
        if result['status'] == 'success':
            print(f"✅ Data generation completed successfully!")
            print(f"   Scale factor: {result['scale_factor']}")
            print(f"   Generation time: {result['generation_time']:.2f} seconds")
            print(f"   Data directory: {result['data_directory']}")
            print(f"   Generated files: {result['total_files']}")
            print(f"   Total rows generated: {result.get('total_rows_generated', 0):,}")
            
            # Show detailed table information
            if 'generated_tables' in result and result['generated_tables']:
                print(f"\n   Table Details:")
                
                # Dimension tables
                dimension_tables = [
                    'call_center', 'catalog_page', 'customer', 'customer_address',
                    'customer_demographics', 'date_dim', 'household_demographics',
                    'income_band', 'item', 'promotion', 'reason', 'ship_mode',
                    'store', 'time_dim', 'warehouse', 'web_page', 'web_site'
                ]
                
                print(f"   Dimension Tables:")
                for table_name in dimension_tables:
                    if table_name in result['generated_tables']:
                        rows = result['generated_tables'][table_name]
                        print(f"     {table_name}: {rows:,} rows")
                
                # Fact tables
                fact_tables = [
                    'catalog_returns', 'catalog_sales', 'inventory',
                    'store_returns', 'store_sales', 'web_returns', 'web_sales'
                ]
                
                print(f"   Fact Tables:")
                for table_name in fact_tables:
                    if table_name in result['generated_tables']:
                        rows = result['generated_tables'][table_name]
                        print(f"     {table_name}: {rows:,} rows")
            
            # Convert to Parquet if requested
            if args.convert_parquet:
                print("\n🔄 Converting to Parquet format...")
                parquet_result = generator.convert_to_parquet()
                
                if parquet_result['status'] == 'success':
                    print(f"✅ Parquet conversion completed!")
                    print(f"   Parquet directory: {parquet_result['parquet_directory']}")
                    print(f"   Converted files: {parquet_result['total_files']}")
                else:
                    print(f"❌ Parquet conversion failed: {parquet_result['error']}")
        else:
            print(f"❌ Data generation failed: {result['error']}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Mock TPC-DS data generation failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
