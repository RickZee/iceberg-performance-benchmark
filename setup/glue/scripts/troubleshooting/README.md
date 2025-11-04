# Troubleshooting Scripts

This directory contains one-off troubleshooting scripts that were created during the initial setup and debugging of Glue-managed Iceberg tables. These scripts are kept for reference but are **not part of the standard workflow**.

## Scripts

### `drop_failing_tables.py`
One-off script to drop specific failing Glue tables so they could be recreated with correct schemas. Uses Spark SQL.

### `drop_failing_tables_glue.py`
Alternative approach to drop failing tables using the AWS Glue API directly instead of Spark SQL.

### `fix_and_integrate_glue_tables.sh`
Bash script that orchestrates the complete fix process:
1. Updates environment to use correct S3 bucket
2. Recreates all Glue tables
3. Loads data
4. Creates Snowflake references
5. Verifies setup

### `recreate_all_glue_tables.py`
Script to recreate all Glue Iceberg tables from Parquet file schemas. Reads schemas from Parquet files and recreates tables with proper backtick quoting.

### `fix_glue_schemas.py`
Script to fix Glue table schemas by dropping and recreating tables with correct schemas from YAML definitions.

## Note

These scripts were used during the troubleshooting phase described in:
- `docs/glue-integration-journey.md`
- `docs/troubleshooting-journey.md`

For standard operations, use the scripts in:
- `scripts/create/` - Table creation and data loading
- `scripts/verify/` - Verification scripts
- `scripts/maintenance/` - Cleanup and maintenance

