"""
TPC-DS Data Generator
Generates standard TPC-DS data using the official TPC-DS toolkit
"""

import logging
import subprocess
import os
import tempfile
import pandas as pd
from typing import Dict, List, Any, Optional
import time
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class TPCDSDataGenerator:
    """Generates TPC-DS data using the official TPC-DS toolkit"""
    
    def __init__(self, scale_factor: float = 1.0, data_dir: str = None):
        """
        Initialize TPC-DS data generator
        
        Args:
            scale_factor: Scale factor for data generation (1.0 = 1GB)
            data_dir: Directory to store generated data
        """
        self.scale_factor = scale_factor
        self.data_dir = data_dir or tempfile.mkdtemp(prefix='tpcds_data_')
        self.tpcds_toolkit_path = self._find_tpcds_toolkit()
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        logger.info(f"TPC-DS Data Generator initialized with scale factor {scale_factor}")
        logger.info(f"Data directory: {self.data_dir}")
    
    def _find_tpcds_toolkit(self) -> Optional[str]:
        """Find TPC-DS toolkit installation"""
        # Common locations for TPC-DS toolkit
        possible_paths = [
            '/opt/tpcds-kit',
            '/usr/local/tpcds-kit',
            '/home/tpcds-kit',
            os.path.expanduser('~/tpcds-kit'),
            os.path.join(os.getcwd(), 'tpcds-kit'),
            os.path.join(os.getcwd(), 'tools', 'tpcds-kit')
        ]
        
        for path in possible_paths:
            if os.path.exists(path) and os.path.exists(os.path.join(path, 'tools', 'dsdgen')):
                logger.info(f"Found TPC-DS toolkit at: {path}")
                return path
        
        logger.warning("TPC-DS toolkit not found. Please install it manually.")
        return None
    
    def install_tpcds_toolkit(self) -> bool:
        """Install TPC-DS toolkit if not found"""
        if self.tpcds_toolkit_path:
            return True
        
        logger.info("Installing TPC-DS toolkit...")
        
        try:
            # Create tools directory
            tools_dir = os.path.join(os.getcwd(), 'tools')
            os.makedirs(tools_dir, exist_ok=True)
            
            # Download and compile TPC-DS toolkit
            install_script = """
            #!/bin/bash
            set -e
            
            # Create tpcds-kit directory
            mkdir -p tools/tpcds-kit
            cd tools/tpcds-kit
            
            # Download TPC-DS toolkit (use curl instead of wget for macOS compatibility)
            if command -v wget >/dev/null 2>&1; then
                wget -q https://www.tpc.org/tpcds/tpcds_tools.zip
            elif command -v curl >/dev/null 2>&1; then
                curl -s -L -o tpcds_tools.zip https://www.tpc.org/tpcds/tpcds_tools.zip
            else
                echo "Error: Neither wget nor curl found. Please install one of them."
                exit 1
            fi
            
            unzip -q tpcds_tools.zip
            
            # Compile the tools
            cd tools
            make OS=LINUX CC=gcc
            
            echo "TPC-DS toolkit installed successfully"
            """
            
            # Write and execute install script
            script_path = os.path.join(tools_dir, 'install_tpcds.sh')
            with open(script_path, 'w') as f:
                f.write(install_script)
            
            os.chmod(script_path, 0o755)
            
            # Execute installation
            result = subprocess.run(['bash', script_path], 
                                  capture_output=True, text=True, cwd=tools_dir)
            
            if result.returncode == 0:
                self.tpcds_toolkit_path = os.path.join(tools_dir, 'tpcds-kit')
                logger.info("TPC-DS toolkit installed successfully")
                return True
            else:
                logger.error(f"Failed to install TPC-DS toolkit: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"Error installing TPC-DS toolkit: {e}")
            return False
    
    def generate_data(self) -> Dict[str, Any]:
        """Generate TPC-DS data using dsdgen tool"""
        if not self.tpcds_toolkit_path:
            if not self.install_tpcds_toolkit():
                raise RuntimeError("Failed to install TPC-DS toolkit")
        
        logger.info(f"Generating TPC-DS data with scale factor {self.scale_factor}")
        
        # Path to dsdgen executable
        dsdgen_path = os.path.join(self.tpcds_toolkit_path, 'tools', 'dsdgen')
        
        if not os.path.exists(dsdgen_path):
            raise FileNotFoundError(f"dsdgen not found at {dsdgen_path}")
        
        try:
            # Generate data using dsdgen
            cmd = [
                dsdgen_path,
                '-scale', str(self.scale_factor),
                '-dir', self.data_dir,
                '-terminate', 'N',
                '-force', 'Y'
            ]
            
            logger.info(f"Running command: {' '.join(cmd)}")
            
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=self.data_dir)
            
            generation_time = time.time() - start_time
            
            if result.returncode != 0:
                raise RuntimeError(f"dsdgen failed: {result.stderr}")
            
            # List generated files
            generated_files = self._list_generated_files()
            
            logger.info(f"Data generation completed in {generation_time:.2f} seconds")
            logger.info(f"Generated {len(generated_files)} files")
            
            return {
                'status': 'success',
                'generation_time': generation_time,
                'scale_factor': self.scale_factor,
                'data_directory': self.data_dir,
                'generated_files': generated_files,
                'total_files': len(generated_files)
            }
            
        except Exception as e:
            logger.error(f"Data generation failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'generation_time': 0,
                'generated_files': []
            }
    
    def _list_generated_files(self) -> List[str]:
        """List all generated data files"""
        files = []
        for file_path in Path(self.data_dir).glob('*.dat'):
            files.append(str(file_path))
        return sorted(files)
    
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
            shutil.rmtree(self.data_dir)
            logger.info(f"Cleaned up data directory: {self.data_dir}")


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='TPC-DS Data Generator')
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
        generator = TPCDSDataGenerator(
            scale_factor=args.scale_factor,
            data_dir=args.data_dir
        )
        
        # Generate data
        print("🚀 Generating TPC-DS data...")
        result = generator.generate_data()
        
        if result['status'] == 'success':
            print(f"✅ Data generation completed successfully!")
            print(f"   Scale factor: {result['scale_factor']}")
            print(f"   Generation time: {result['generation_time']:.2f} seconds")
            print(f"   Data directory: {result['data_directory']}")
            print(f"   Generated files: {result['total_files']}")
            print(f"   Total rows generated: {result.get('total_rows_generated', 0):,}")
            
            # Show detailed table information if available
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
        logger.error(f"TPC-DS data generation failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
