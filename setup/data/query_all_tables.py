#!/usr/bin/env python3
"""
Query all tables from TPC-DS schemas in Snowflake
Outputs: schema|table|row_count
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.env import get_snowflake_config, get_snowflake_schemas
import snowflake.connector
import argparse
import csv

def get_tpcds_schemas():
    """Get TPC-DS schemas from config (lib.env.get_snowflake_schemas)"""
    schemas = get_snowflake_schemas()
    # Convert to uppercase to match Snowflake's behavior with unquoted identifiers
    return [s.upper() for s in schemas.values()]

def discover_all_tables(cursor, database, schema):
    """Discover all tables in a schema"""
    try:
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        # SHOW TABLES returns: (created_on, name, database_name, schema_name, kind, ...)
        table_names = [row[1] for row in tables]
        return table_names
    except Exception as e:
        print(f"Error discovering tables in schema {schema}: {e}")
        return []

def get_table_row_count(cursor, database, schema, table):
    """Get row count for a table"""
    try:
        # Use fully qualified name to avoid schema context issues
        full_table_name = f"{database}.{schema}.{table}"
        cursor.execute(f"SELECT COUNT(*) FROM {full_table_name}")
        result = cursor.fetchone()
        return result[0] if result else 0
    except Exception as e:
        # Return error message to indicate error
        error_msg = str(e).strip()
        # Replace newlines with spaces for CSV compatibility
        error_msg = error_msg.replace('\n', ' ').replace('\r', ' ')
        # Collapse multiple spaces
        error_msg = ' '.join(error_msg.split())
        # Truncate long error messages
        if len(error_msg) > 200:
            error_msg = error_msg[:197] + "..."
        return f"ERROR: {error_msg}"

def query_all_tables(database=None, output_format='table', output_file=None):
    """Query all tables from TPC-DS schemas"""
    try:
        # Get Snowflake configuration
        config = get_snowflake_config()
        
        # Use provided database or default from config
        if not database:
            database = config.get('database', 'tpcds_performance_test')
        
        # Get TPC-DS schemas from config
        schemas_to_query = get_tpcds_schemas()
        print(f"Querying TPC-DS schemas: {', '.join(schemas_to_query)}")
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=config['user'],
            account=config['account'],
            warehouse=config.get('warehouse'),
            database=database,
            role=config.get('role'),
            private_key_file=config.get('private_key_file'),
            private_key_passphrase=config.get('private_key_passphrase') if config.get('private_key_passphrase') else None
        )
        
        cursor = conn.cursor()
        
        # Collect results
        results = []
        total_tables = 0
        successful_queries = 0
        failed_queries = 0
        
        for schema in schemas_to_query:
            tables = discover_all_tables(cursor, database, schema)
            total_tables += len(tables)
            
            for table in tables:
                row_count = get_table_row_count(cursor, database, schema, table)
                
                if isinstance(row_count, int):
                    results.append({
                        'schema': schema,
                        'table': table,
                        'row_count': row_count
                    })
                    successful_queries += 1
                else:
                    # Error case
                    results.append({
                        'schema': schema,
                        'table': table,
                        'row_count': row_count if row_count else 'ERROR'
                    })
                    failed_queries += 1
        
        cursor.close()
        conn.close()
        
        # Output results
        if output_format == 'csv' or output_file:
            # Write to CSV
            csv_file = output_file or f"all_tables_{database}.csv"
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['schema', 'table', 'row_count'])
                for r in results:
                    writer.writerow([r['schema'], r['table'], r['row_count']])
            print(f"\n✅ Results saved to: {csv_file}")
        
        # Print table format
        if output_format == 'table' or not output_file:
            print("\n" + "=" * 80)
            print(f"{'SCHEMA':<30} {'TABLE':<30} {'ROW COUNT':<15}")
            print("=" * 80)
            for r in results:
                row_count_str = f"{r['row_count']:,}" if isinstance(r['row_count'], int) else str(r['row_count'])
                print(f"{r['schema']:<30} {r['table']:<30} {row_count_str:<15}")
            print("=" * 80)
        
        # Print summary
        print(f"\nSummary:")
        print(f"  Schemas queried: {len(schemas_to_query)}")
        print(f"  Total tables: {total_tables}")
        print(f"  Successful queries: {successful_queries}")
        print(f"  Failed queries: {failed_queries}")
        
        # Show failed queries if any
        if failed_queries > 0:
            print(f"\n⚠️  FAILED QUERIES:")
            for r in results:
                if not isinstance(r['row_count'], int):
                    print(f"    {r['schema']}.{r['table']}: {r['row_count']}")
        
        if successful_queries > 0:
            total_rows = sum(r['row_count'] for r in results if isinstance(r['row_count'], int))
            print(f"\n  Total rows across all tables: {total_rows:,}")
        
        return results
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Query all tables from TPC-DS schemas in Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Query TPC-DS schemas (table format)
  python query_all_tables.py
  
  # Output to CSV file
  python query_all_tables.py --output csv
  
  # Output to specific CSV file
  python query_all_tables.py --output csv --file results.csv
  
  # Query specific database
  python query_all_tables.py --database tpcds_performance_test
        """
    )
    
    parser.add_argument(
        '--database', '-d',
        type=str,
        help='Snowflake database name (default: from config)'
    )
    
    parser.add_argument(
        '--output', '-o',
        choices=['table', 'csv', 'both'],
        default='table',
        help='Output format (default: table)'
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        help='Output file path (for CSV format)'
    )
    
    args = parser.parse_args()
    
    # Determine output format
    output_format = 'both' if args.output == 'both' else args.output
    if args.file:
        output_format = 'csv'
    
    results = query_all_tables(
        database=args.database,
        output_format=output_format,
        output_file=args.file
    )
    
    return 0 if results else 1

if __name__ == '__main__':
    exit(main())

