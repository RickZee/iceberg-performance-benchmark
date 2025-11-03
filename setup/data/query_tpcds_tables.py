#!/usr/bin/env python3
"""
TPC-DS Tables Query Script
Queries all TPC-DS tables in Snowflake and produces a report with schema, table, and row count
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from tests.snowflake_connector import SnowflakeConnector
from lib.output import output_manager

logger = logging.getLogger(__name__)

class TPCDSTableReporter:
    """Reports on TPC-DS tables in Snowflake"""
    
    def __init__(self):
        """Initialize the reporter"""
        self.connector = SnowflakeConnector()
        
        # TPC-DS schemas and their corresponding formats
        self.tpcds_schemas = {
            'tpcds_native_format': 'Native',
            'tpcds_iceberg_sf_format': 'Iceberg SF',
            'tpcds_iceberg_glue_format': 'Iceberg Glue',
            'tpcds_external_format': 'External'
        }
        
        # TPC-DS table names
        self.tpcds_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site',
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
    
    def query_all_tables(self) -> pd.DataFrame:
        """Query all TPC-DS tables and return results as DataFrame"""
        results = []
        
        for schema_name, format_type in self.tpcds_schemas.items():
            logger.info(f"Querying {format_type} format tables in schema {schema_name}")
            
            try:
                connection = self.connector.get_connection(format_type.lower().replace(' ', '_'))
                
                with connection.cursor() as cursor:
                    # Set database and schema context
                    database = os.getenv('SNOWFLAKE_DATABASE', 'tpcds_performance_test')
                    cursor.execute(f"USE DATABASE {database}")
                    cursor.execute(f"USE SCHEMA {schema_name}")
                    
                    # Query each TPC-DS table
                    for table_name in self.tpcds_tables:
                        full_table_name = f"{table_name}_{format_type.lower().replace(' ', '_')}"
                        
                        try:
                            # Get row count
                            cursor.execute(f"SELECT COUNT(*) as row_count FROM {full_table_name}")
                            row_count = cursor.fetchone()[0]
                            
                            # Get table info
                            cursor.execute(f"DESCRIBE TABLE {full_table_name}")
                            columns_info = cursor.fetchall()
                            
                            # Count columns
                            column_count = len(columns_info)
                            
                            # Get table size (if available)
                            try:
                                cursor.execute(f"""
                                    SELECT 
                                        BYTES,
                                        ROW_COUNT,
                                        CLUSTERING_KEY,
                                        CREATED,
                                        LAST_ALTERED
                                    FROM INFORMATION_SCHEMA.TABLES 
                                    WHERE TABLE_SCHEMA = '{schema_name}' 
                                    AND TABLE_NAME = '{full_table_name}'
                                """)
                                table_info = cursor.fetchone()
                                table_size_bytes = table_info[0] if table_info and table_info[0] else 0
                                table_size_mb = round(table_size_bytes / (1024 * 1024), 2) if table_size_bytes else 0
                                created_date = table_info[3] if table_info and table_info[3] else None
                                last_altered = table_info[4] if table_info and table_info[4] else None
                            except Exception as e:
                                logger.warning(f"Could not get table size info for {full_table_name}: {e}")
                                table_size_mb = 0
                                created_date = None
                                last_altered = None
                            
                            results.append({
                                'schema': schema_name,
                                'format_type': format_type,
                                'table_name': table_name,
                                'full_table_name': full_table_name,
                                'row_count': row_count,
                                'column_count': column_count,
                                'table_size_mb': table_size_mb,
                                'created_date': created_date,
                                'last_altered': last_altered,
                                'status': 'success'
                            })
                            
                            logger.info(f"  {table_name}: {row_count:,} rows, {column_count} columns, {table_size_mb} MB")
                            
                        except Exception as e:
                            logger.error(f"  Failed to query {full_table_name}: {e}")
                            results.append({
                                'schema': schema_name,
                                'format_type': format_type,
                                'table_name': table_name,
                                'full_table_name': full_table_name,
                                'row_count': 0,
                                'column_count': 0,
                                'table_size_mb': 0,
                                'created_date': None,
                                'last_altered': None,
                                'status': f'error: {str(e)}'
                            })
                
            except Exception as e:
                logger.error(f"Failed to connect to {format_type} format: {e}")
                # Add error entries for all tables in this schema
                for table_name in self.tpcds_tables:
                    results.append({
                        'schema': schema_name,
                        'format_type': format_type,
                        'table_name': table_name,
                        'full_table_name': f"{table_name}_{format_type.lower().replace(' ', '_')}",
                        'row_count': 0,
                        'column_count': 0,
                        'table_size_mb': 0,
                        'created_date': None,
                        'last_altered': None,
                        'status': f'connection_error: {str(e)}'
                    })
        
        return pd.DataFrame(results)
    
    def generate_report(self, df: pd.DataFrame) -> str:
        """Generate a comprehensive report from the DataFrame"""
        report = []
        report.append("=" * 100)
        report.append("TPC-DS TABLES REPORT")
        report.append("=" * 100)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Summary by format
        report.append("SUMMARY BY FORMAT")
        report.append("-" * 50)
        format_summary = df.groupby('format_type').agg({
            'row_count': 'sum',
            'table_size_mb': 'sum',
            'table_name': 'count'
        }).round(2)
        
        for format_type, stats in format_summary.iterrows():
            report.append(f"{format_type}:")
            report.append(f"  Tables: {stats['table_name']}")
            report.append(f"  Total Rows: {stats['row_count']:,}")
            report.append(f"  Total Size: {stats['table_size_mb']:.2f} MB")
            report.append("")
        
        # Summary by table type (dimension vs fact)
        report.append("SUMMARY BY TABLE TYPE")
        report.append("-" * 50)
        
        dimension_tables = [
            'call_center', 'catalog_page', 'customer', 'customer_address',
            'customer_demographics', 'date_dim', 'household_demographics',
            'income_band', 'item', 'promotion', 'reason', 'ship_mode',
            'store', 'time_dim', 'warehouse', 'web_page', 'web_site'
        ]
        
        fact_tables = [
            'catalog_returns', 'catalog_sales', 'inventory', 'store_returns',
            'store_sales', 'web_returns', 'web_sales'
        ]
        
        df['table_type'] = df['table_name'].apply(
            lambda x: 'Dimension' if x in dimension_tables else 'Fact' if x in fact_tables else 'Unknown'
        )
        
        type_summary = df.groupby('table_type').agg({
            'row_count': 'sum',
            'table_size_mb': 'sum',
            'table_name': 'count'
        }).round(2)
        
        for table_type, stats in type_summary.iterrows():
            report.append(f"{table_type} Tables:")
            report.append(f"  Count: {stats['table_name']}")
            report.append(f"  Total Rows: {stats['row_count']:,}")
            report.append(f"  Total Size: {stats['table_size_mb']:.2f} MB")
            report.append("")
        
        # Detailed table listing
        report.append("DETAILED TABLE LISTING")
        report.append("-" * 100)
        report.append(f"{'Schema':<25} {'Format':<15} {'Table':<20} {'Rows':<12} {'Columns':<8} {'Size(MB)':<10} {'Status':<15}")
        report.append("-" * 100)
        
        for _, row in df.iterrows():
            status = row['status'][:14] if len(str(row['status'])) > 14 else str(row['status'])
            report.append(
                f"{row['schema']:<25} {row['format_type']:<15} {row['table_name']:<20} "
                f"{row['row_count']:<12,} {row['column_count']:<8} {row['table_size_mb']:<10.2f} {status:<15}"
            )
        
        # Error summary
        error_df = df[df['status'] != 'success']
        if not error_df.empty:
            report.append("")
            report.append("ERRORS SUMMARY")
            report.append("-" * 50)
            for _, row in error_df.iterrows():
                report.append(f"{row['schema']}.{row['table_name']}: {row['status']}")
        
        # Top 10 largest tables by row count
        report.append("")
        report.append("TOP 10 LARGEST TABLES BY ROW COUNT")
        report.append("-" * 60)
        top_tables = df[df['status'] == 'success'].nlargest(10, 'row_count')
        for _, row in top_tables.iterrows():
            report.append(f"{row['table_name']:<20} ({row['format_type']:<15}): {row['row_count']:,} rows")
        
        report.append("")
        report.append("=" * 100)
        
        return "\n".join(report)
    
    def save_report(self, report: str) -> str:
        """Save report to results folder"""
        report_path = output_manager.get_tpcds_report_path("tables")
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        logger.info(f"Report saved to: {report_path}")
        return report_path
    
    def save_csv(self, df: pd.DataFrame) -> str:
        """Save detailed data to results folder"""
        csv_path = output_manager.get_tpcds_data_path("tables")
        
        df.to_csv(csv_path, index=False)
        logger.info(f"Detailed data saved to: {csv_path}")
        return csv_path


def main():
    """Main function"""
    # Setup logging to results folder
    log_config = output_manager.get_logger_config("tpcds_tables_query.log")
    logging.basicConfig(**log_config)
    
    try:
        print("🔍 Querying TPC-DS tables in Snowflake...")
        
        # Initialize reporter
        reporter = TPCDSTableReporter()
        
        # Query all tables
        df = reporter.query_all_tables()
        
        # Generate report
        report = reporter.generate_report(df)
        
        # Print report
        print("\n" + report)
        
        # Save report and CSV to results folder
        report_file = reporter.save_report(report)
        csv_file = reporter.save_csv(df)
        
        print(f"\n✅ Report generated successfully!")
        print(f"📄 Report file: {report_file}")
        print(f"📊 CSV file: {csv_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Failed to generate TPC-DS tables report: {e}")
        print(f"\n❌ Error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())
