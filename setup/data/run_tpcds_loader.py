#!/usr/bin/env python3
"""
TPC-DS Data Loader Runner
Simple script to run the TPC-DS data loader application
"""

import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from setup.data.main import main

if __name__ == '__main__':
    sys.exit(main())
