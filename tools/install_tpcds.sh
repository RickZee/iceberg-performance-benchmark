#!/bin/bash
set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Create tpcds-kit directory in tools (not nested)
mkdir -p "$SCRIPT_DIR/tpcds-kit"
cd "$SCRIPT_DIR/tpcds-kit"

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

echo "TPC-DS toolkit installed successfully in $SCRIPT_DIR/tpcds-kit"
