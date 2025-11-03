#!/usr/bin/env python3
"""
Create Snowflake Iceberg tables that reference AWS Glue-managed tables.

This script reads the create_iceberg_glue.sql file and executes each
CREATE TABLE statement in Snowflake to create references to the Glue tables.

This is Step 3 of the Glue table setup workflow:
1. Create Glue tables (create_glue_tables.py)
2. Load data (load_tpcds_data.py)
3. Create Snowflake references (this script)
"""

import os
import sys
import re
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.snowflake_connector import SnowflakeConnector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def read_sql_file(file_path: str) -> str:
    """Read SQL file content."""
    with open(file_path, 'r') as f:
        return f.read()


def split_sql_statements(sql_content: str) -> list:
    """
    Split SQL content into individual statements.
    Handles multi-line statements and removes comments.
    """
    # Remove single-line comments (-- ...) but preserve newlines
    lines = sql_content.split('\n')
    cleaned_lines = []
    for line in lines:
        # Remove comments that start with --
        if '--' in line:
            comment_index = line.find('--')
            # Only remove if it's not inside a string
            in_string = False
            for i, char in enumerate(line[:comment_index]):
                if char in ("'", '"') and (i == 0 or line[i-1] != '\\'):
                    in_string = not in_string
            if not in_string:
                line = line[:comment_index]
        cleaned_lines.append(line)
    
    sql_content = '\n'.join(cleaned_lines)
    
    # Remove multi-line comments (/* ... */)
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
    
    # Split by semicolons that are not inside quotes
    statements = []
    current_statement = ""
    in_quotes = False
    quote_char = None
    
    for char in sql_content:
        if char in ("'", '"') and (not current_statement or current_statement[-1] != '\\'):
            current_statement += char
            if not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char:
                in_quotes = False
                quote_char = None
            continue
        if char == ';' and not in_quotes:
            statement = current_statement.strip()
            if statement:
                statements.append(statement)
            current_statement = ""
        else:
            current_statement += char
    
    # Add the last statement if it doesn't end with a semicolon
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    return statements


def execute_statements(connector: SnowflakeConnector, statements: list) -> dict:
    """
    Execute SQL statements and report results.
    
    Returns:
        dict with 'success', 'failed', and 'errors' keys
    """
    conn = connector.get_connection('iceberg_glue')
    cursor = conn.cursor()
    
    results = {
        'success': [],
        'failed': [],
        'errors': {}
    }
    
    for i, statement in enumerate(statements, 1):
        # Skip empty statements
        if not statement.strip():
            continue
        
        # Fix escaped quotes that appear in the SQL file
        statement = statement.replace("\\'", "'")
        
        # Extract table name for logging (get the last identifier after CREATE TABLE)
        # Match pattern like: CREATE ... TABLE database.schema.table_name
        table_match = re.search(r'CREATE\s+.*?TABLE\s+\S+\.\S+\.(\w+)', statement, re.IGNORECASE | re.DOTALL)
        if not table_match:
            # Try alternate pattern without full qualification
            table_match = re.search(r'TABLE\s+\S+\.\S+\.(\w+)', statement, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else f"statement_{i}"
        
        try:
            logger.info(f"Executing CREATE TABLE for: {table_name}")
            # Print the full statement for debugging if it fails
            full_statement = statement
            cursor.execute(statement)
            results['success'].append(table_name)
            logger.info(f"✅ Successfully created table: {table_name}")
        except Exception as e:
            error_msg = str(e)
            results['failed'].append(table_name)
            results['errors'][table_name] = error_msg
            logger.error(f"❌ Failed to create table {table_name}: {error_msg}")
            # Print the statement that failed for debugging
            logger.error(f"Failed statement:\n{full_statement}")
    
    cursor.close()
    return results


def main():
    """Main function."""
    # Get the path to the SQL file (relative to project root)
    sql_file = project_root / 'snowflake' / 'tpcds-schema' / 'create_iceberg_glue.sql'
    
    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        sys.exit(1)
    
    logger.info(f"Reading SQL file: {sql_file}")
    sql_content = read_sql_file(str(sql_file))
    
    logger.info("Parsing SQL statements...")
    statements = split_sql_statements(sql_content)
    logger.info(f"Found {len(statements)} SQL statements")
    
    # Filter to only CREATE TABLE statements
    create_statements = [
        stmt for stmt in statements
        if stmt.upper().strip().startswith('CREATE')
    ]
    logger.info(f"Found {len(create_statements)} CREATE TABLE statements")
    
    # Create connector and execute statements
    logger.info("Connecting to Snowflake...")
    connector = SnowflakeConnector()
    
    try:
        results = execute_statements(connector, create_statements)
        
        # Print summary
        print("\n" + "="*70)
        print("SUMMARY")
        print("="*70)
        print(f"✅ Successfully created: {len(results['success'])} tables")
        print(f"❌ Failed: {len(results['failed'])} tables")
        
        if results['success']:
            print("\n✅ Successfully created tables:")
            for table in sorted(results['success']):
                print(f"   - {table}")
        
        if results['failed']:
            print("\n❌ Failed tables:")
            for table in sorted(results['failed']):
                print(f"   - {table}: {results['errors'].get(table, 'Unknown error')}")
        
        print("="*70)
        
        if results['failed']:
            sys.exit(1)
        else:
            logger.info("🎉 All tables created successfully!")
            
    finally:
        connector.close_all_connections()


if __name__ == "__main__":
    main()

