"""
TPC-DS Data Loader Main Application
Orchestrates the complete TPC-DS data generation and loading process
"""

import logging
import argparse
import sys
import os
import time
from pathlib import Path

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from setup.data.mock_tpcds_generator import MockTPCDSDataGenerator as TPCDSDataGenerator
from setup.data.tpcds_data_loader import TPCDSDataLoader
from tests.snowflake_connector import SnowflakeConnector
from lib.output import output_manager

logger = logging.getLogger(__name__)

class TPCDSApplication:
    """Main application for TPC-DS data generation and loading"""
    
    def __init__(self, scale_factor: float = 1.0, data_dir: str = None, verbose: bool = False, refresh_external: bool = False):
        """
        Initialize TPC-DS application
        
        Args:
            scale_factor: Scale factor for data generation (1.0 = 1GB)
            data_dir: Directory to store generated data
            verbose: Enable verbose logging
            refresh_external: Refresh external table metadata after loading
        """
        self.scale_factor = scale_factor
        self.data_dir = data_dir or f"data/tpcds_data_sf{scale_factor}"
        self.verbose = verbose
        self.refresh_external = refresh_external
        
        # Setup logging to results folder
        log_level = logging.DEBUG if verbose else logging.INFO
        log_config = output_manager.get_logger_config("tpcds_data_loader.log")
        log_config['level'] = log_level
        logging.basicConfig(**log_config)
        
        logger.info(f"TPC-DS Application initialized with scale factor {scale_factor}")
        logger.info(f"Data directory: {self.data_dir}")
        if refresh_external:
            logger.info("External table metadata refresh enabled")
    
    def run_complete_pipeline(self, formats: list = None) -> dict:
        """Run the complete TPC-DS data generation and loading pipeline"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        pipeline_results = {
            'generation': {},
            'loading': {},
            'verification': {},
            'total_duration': 0,
            'success': False
        }
        
        start_time = time.time()
        
        try:
            # Step 1: Generate TPC-DS data
            logger.info("🚀 Step 1: Generating TPC-DS data...")
            generator = TPCDSDataGenerator(
                scale_factor=self.scale_factor,
                data_dir=self.data_dir
            )
            
            generation_result = generator.generate_data()
            pipeline_results['generation'] = generation_result
            
            if generation_result['status'] != 'success':
                raise RuntimeError(f"Data generation failed: {generation_result.get('error', 'Unknown error')}")
            
            logger.info(f"✅ Data generation completed in {generation_result['generation_time']:.2f} seconds")
            
            # Step 2: Convert to Parquet
            logger.info("🔄 Step 2: Converting to Parquet format...")
            parquet_result = generator.convert_to_parquet()
            
            if parquet_result['status'] != 'success':
                raise RuntimeError(f"Parquet conversion failed: {parquet_result.get('error', 'Unknown error')}")
            
            logger.info(f"✅ Parquet conversion completed: {parquet_result['total_files']} files")
            
            # Step 3: Load data into Snowflake
            logger.info("📊 Step 3: Loading data into Snowflake...")
            connector = SnowflakeConnector()
            data_loader = TPCDSDataLoader(connector)
            
            loading_result = data_loader.load_all_tpcds_data(self.data_dir, formats, self.refresh_external)
            pipeline_results['loading'] = loading_result
            
            if 'error' in loading_result:
                raise RuntimeError(f"Data loading failed: {loading_result['error']}")
            
            logger.info(f"✅ Data loading completed in {loading_result['load_duration']:.2f} seconds")
            logger.info(f"   Total rows loaded: {loading_result['total_rows_loaded']:,}")
            
            # Step 4: Verify data
            logger.info("🔍 Step 4: Verifying loaded data...")
            verification_result = data_loader.verify_tpcds_data_loaded(formats)
            pipeline_results['verification'] = verification_result
            
            if verification_result['all_verified']:
                logger.info("✅ Data verification passed")
            else:
                logger.warning("⚠️ Data verification had issues - check logs for details")
            
            pipeline_results['success'] = True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            pipeline_results['error'] = str(e)
        
        pipeline_results['total_duration'] = time.time() - start_time
        return pipeline_results
    
    def generate_data_only(self) -> dict:
        """Generate TPC-DS data only (without loading)"""
        logger.info("🚀 Generating TPC-DS data only...")
        
        generator = TPCDSDataGenerator(
            scale_factor=self.scale_factor,
            data_dir=self.data_dir
        )
        
        # Generate data
        generation_result = generator.generate_data()
        
        if generation_result['status'] == 'success':
            # Convert to Parquet
            parquet_result = generator.convert_to_parquet()
            generation_result['parquet_conversion'] = parquet_result
        
        return generation_result
    
    def load_data_only(self, formats: list = None) -> dict:
        """Load existing TPC-DS data only (without generation)"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        logger.info("📊 Loading existing TPC-DS data...")
        
        parquet_dir = os.path.join(self.data_dir, 'parquet')
        if not os.path.exists(parquet_dir):
            raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
        
        connector = SnowflakeConnector()
        data_loader = TPCDSDataLoader(connector)
        
        return data_loader.load_all_tpcds_data(self.data_dir, formats, self.refresh_external)
    
    def verify_data_only(self, formats: list = None) -> dict:
        """Verify existing TPC-DS data only"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        logger.info("🔍 Verifying TPC-DS data...")
        
        connector = SnowflakeConnector()
        data_loader = TPCDSDataLoader(connector)
        
        return data_loader.verify_tpcds_data_loaded(formats)
    
    def cleanup_data_only(self, formats: list = None) -> dict:
        """Cleanup TPC-DS data only"""
        if formats is None:
            formats = ['native', 'iceberg_sf', 'iceberg_glue', 'external']
        
        logger.info("🧹 Cleaning up TPC-DS data...")
        
        connector = SnowflakeConnector()
        data_loader = TPCDSDataLoader(connector)
        
        return data_loader.cleanup_tpcds_data(formats)
    
    def print_summary(self, results: dict):
        """Print a summary of the results"""
        print("\n" + "="*80)
        print("TPC-DS DATA LOADER SUMMARY")
        print("="*80)
        
        if 'generation' in results:
            gen = results['generation']
            print(f"📊 Data Generation:")
            print(f"   Status: {gen.get('status', 'Unknown')}")
            print(f"   Scale Factor: {gen.get('scale_factor', 'Unknown')}")
            print(f"   Duration: {gen.get('generation_time', 0):.2f} seconds")
            print(f"   Files Generated: {gen.get('total_files', 0)}")
            print(f"   Total Rows Generated: {gen.get('total_rows_generated', 0):,}")
            
            # Show detailed table information
            if 'generated_tables' in gen and gen['generated_tables']:
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
                    if table_name in gen['generated_tables']:
                        rows = gen['generated_tables'][table_name]
                        print(f"     {table_name}: {rows:,} rows")
                
                # Fact tables
                fact_tables = [
                    'catalog_returns', 'catalog_sales', 'inventory',
                    'store_returns', 'store_sales', 'web_returns', 'web_sales'
                ]
                
                print(f"   Fact Tables:")
                for table_name in fact_tables:
                    if table_name in gen['generated_tables']:
                        rows = gen['generated_tables'][table_name]
                        print(f"     {table_name}: {rows:,} rows")
        
        if 'loading' in results:
            load = results['loading']
            print(f"\n📈 Data Loading:")
            print(f"   Duration: {load.get('load_duration', 0):.2f} seconds")
            print(f"   Total Rows: {load.get('total_rows_loaded', 0):,}")
            
            for format_type, format_results in load.get('loaded_tables', {}).items():
                print(f"   {format_type}:")
                for table_name, table_info in format_results.items():
                    print(f"     {table_name}: {table_info.get('rows_loaded', 0)} rows")
        
        if 'verification' in results:
            verif = results['verification']
            print(f"\n🔍 Data Verification:")
            print(f"   All Verified: {verif.get('all_verified', False)}")
            
            for format_type, table_counts in verif.get('table_counts', {}).items():
                print(f"   {format_type}:")
                for table_name, count in table_counts.items():
                    print(f"     {table_name}: {count} rows")
        
        if 'total_duration' in results:
            print(f"\n⏱️ Total Duration: {results['total_duration']:.2f} seconds")
        
        print(f"\n📁 Data Directory: {self.data_dir}")
        print("="*80)


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description='TPC-DS Data Loader Application')
    parser.add_argument('--action', 
                       choices=['generate', 'load', 'verify', 'cleanup', 'full'], 
                       default='full',
                       help='Action to perform')
    parser.add_argument('--scale-factor', type=float, default=1.0,
                       help='Scale factor for data generation (default: 1.0)')
    parser.add_argument('--data-dir', type=str,
                       help='Directory to store generated data')
    parser.add_argument('--formats', nargs='+', 
                       default=['native', 'iceberg_sf', 'iceberg_glue', 'external'],
                       help='Formats to process')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--refresh-external', action='store_true',
                       help='Refresh external table metadata after loading')
    
    args = parser.parse_args()
    
    try:
        # Initialize application
        app = TPCDSApplication(
            scale_factor=args.scale_factor,
            data_dir=args.data_dir,
            verbose=args.verbose,
            refresh_external=args.refresh_external
        )
        
        # Execute action
        if args.action == 'generate':
            results = app.generate_data_only()
            app.print_summary({'generation': results})
            
        elif args.action == 'load':
            results = app.load_data_only(args.formats)
            app.print_summary({'loading': results})
            
        elif args.action == 'verify':
            results = app.verify_data_only(args.formats)
            app.print_summary({'verification': results})
            
        elif args.action == 'cleanup':
            results = app.cleanup_data_only(args.formats)
            app.print_summary({'cleanup': results})
            
        elif args.action == 'full':
            results = app.run_complete_pipeline(args.formats)
            app.print_summary(results)
            
            if not results['success']:
                print(f"\n❌ Pipeline failed: {results.get('error', 'Unknown error')}")
                return 1
        
        print(f"\n✅ {args.action.title()} completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        print(f"\n❌ Application failed: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
