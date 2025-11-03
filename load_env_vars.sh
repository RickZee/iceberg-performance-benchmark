#!/bin/bash
# Load Environment Variables
# Consolidated environment variable loader for the project

# Get the project root directory (where this script is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

# Load .env file if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from .env file..."
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
    echo "✅ Environment variables loaded from .env"
else
    echo "⚠️  .env file not found at $PROJECT_ROOT/.env"
    echo "   Using env.example as template: cp env.example .env"
fi

# Set default key locations if not already set
if [ -z "$SNOWFLAKE_PRIVATE_KEY_FILE" ]; then
    # Try common locations
    KEY_LOCATIONS=(
        "$PROJECT_ROOT/setup/data/rsa_key.p8"
        "$PROJECT_ROOT/benchmark/rsa_key.p8"
        "$HOME/.ssh/rsa_key.p8"
    )
    
    for key_path in "${KEY_LOCATIONS[@]}"; do
        if [ -f "$key_path" ]; then
            export SNOWFLAKE_PRIVATE_KEY_FILE="$key_path"
            echo "✅ Found Snowflake private key at: $key_path"
            break
        fi
    done
fi

# Export PROJECT_ROOT for use in scripts
export PROJECT_ROOT="$PROJECT_ROOT"

echo "Environment setup complete!"
echo "PROJECT_ROOT: $PROJECT_ROOT"

