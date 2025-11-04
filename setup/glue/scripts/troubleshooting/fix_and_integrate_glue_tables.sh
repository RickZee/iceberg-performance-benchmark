#!/bin/bash
# Fix Glue tables and integrate with Snowflake
# This script:
# 1. Updates environment to use correct bucket (iceberg-snowflake-perf)
# 2. Recreates all Glue tables in correct bucket
# 3. Loads data into tables
# 4. Creates Snowflake table references
# 5. Verifies by querying Snowflake

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$PROJECT_ROOT"

# Load environment
source load_env_vars.sh

echo "=============================================================================="
echo "FIXING GLUE TABLES AND INTEGRATING WITH SNOWFLAKE"
echo "=============================================================================="
echo ""

# Check current bucket
CURRENT_BUCKET="${AWS_S3_BUCKET}"
CURRENT_PATH="${AWS_S3_ICEBERG_GLUE_PATH}"

echo "Current configuration:"
echo "  AWS_S3_BUCKET: $CURRENT_BUCKET"
echo "  AWS_S3_ICEBERG_GLUE_PATH: $CURRENT_PATH"
echo ""

# Determine correct bucket from external volume
# For now, we'll use iceberg-snowflake-perf based on the error messages
CORRECT_BUCKET="iceberg-snowflake-perf"
CORRECT_PATH="s3://${CORRECT_BUCKET}/iceberg-snowflake-perf/iceberg_glue_format/"

echo "Correct configuration (to match Snowflake external volume):"
echo "  AWS_S3_BUCKET: $CORRECT_BUCKET"
echo "  AWS_S3_ICEBERG_GLUE_PATH: $CORRECT_PATH"
echo ""

if [ "$CURRENT_BUCKET" != "$CORRECT_BUCKET" ]; then
    echo "⚠️  Bucket mismatch detected!"
    echo "   Updating environment variables..."
    export AWS_S3_BUCKET="$CORRECT_BUCKET"
    export AWS_S3_ICEBERG_GLUE_PATH="$CORRECT_PATH"
    echo "   ✅ Environment updated for this session"
    echo ""
fi

# Step 1: Recreate all Glue tables
echo "=============================================================================="
echo "STEP 1: Recreating all Glue tables in correct bucket..."
echo "=============================================================================="
python3 setup/glue/scripts/troubleshooting/recreate_all_glue_tables.py

# Step 2: Load data
echo ""
echo "=============================================================================="
echo "STEP 2: Loading data into Glue tables..."
echo "=============================================================================="
python3 setup/glue/scripts/create/load_tpcds_data.py

# Step 3: Create Snowflake references
echo ""
echo "=============================================================================="
echo "STEP 3: Creating Snowflake table references..."
echo "=============================================================================="
python3 setup/glue/scripts/create/create_snowflake_glue_tables.py

# Step 4: Verify in Snowflake
echo ""
echo "=============================================================================="
echo "STEP 4: Verifying tables in Snowflake..."
echo "=============================================================================="
python3 setup/glue/scripts/verify/verify_snowflake_glue_tables.py

echo ""
echo "=============================================================================="
echo "✅ ALL STEPS COMPLETED!"
echo "=============================================================================="

