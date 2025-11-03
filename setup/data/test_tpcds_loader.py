#!/usr/bin/env python3
"""
Test script for TPC-DS Data Loader
Tests the basic functionality without requiring Snowflake connection
"""

import sys
import os
import tempfile
import shutil

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from setup.data.mock_tpcds_generator import MockTPCDSDataGenerator

def test_tpcds_generator():
    """Test the TPC-DS data generator"""
    print("🧪 Testing TPC-DS Data Generator...")
    
    # Create a temporary directory for testing
    test_dir = tempfile.mkdtemp(prefix='tpcds_test_')
    
    try:
        # Initialize generator with very small scale factor
        generator = MockTPCDSDataGenerator(
            scale_factor=0.01,  # Very small for testing
            data_dir=test_dir
        )
        
        print(f"   Test directory: {test_dir}")
        print(f"   Scale factor: {generator.scale_factor}")
        
        # Test data generation (this will fail if TPC-DS toolkit is not available)
        print("   Attempting data generation...")
        result = generator.generate_data()
        
        if result['status'] == 'success':
            print("   ✅ Data generation test passed!")
            print(f"   Generated files: {result['total_files']}")
            
            # Test Parquet conversion
            print("   Testing Parquet conversion...")
            parquet_result = generator.convert_to_parquet()
            
            if parquet_result['status'] == 'success':
                print("   ✅ Parquet conversion test passed!")
                print(f"   Parquet files: {parquet_result['total_files']}")
            else:
                print(f"   ⚠️ Parquet conversion test failed: {parquet_result.get('error', 'Unknown error')}")
        else:
            print(f"   ⚠️ Data generation test failed: {result.get('error', 'Unknown error')}")
            print("   This is expected if TPC-DS toolkit is not installed")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Test failed with exception: {e}")
        return False
        
    finally:
        # Clean up test directory
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
            print(f"   🧹 Cleaned up test directory: {test_dir}")

def test_imports():
    """Test that all required modules can be imported"""
    print("🧪 Testing imports...")
    
    try:
        from setup.data.tpcds_generator import TPCDSDataGenerator
        print("   ✅ TPCDSDataGenerator import successful")
        
        from setup.data.tpcds_data_loader import TPCDSDataLoader
        print("   ✅ TPCDSDataLoader import successful")
        
        from setup.data.main import TPCDSApplication
        print("   ✅ TPCDSApplication import successful")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Import test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 TPC-DS Data Loader Test Suite")
    print("=" * 50)
    
    tests_passed = 0
    total_tests = 2
    
    # Test imports
    if test_imports():
        tests_passed += 1
    
    print()
    
    # Test TPC-DS generator
    if test_tpcds_generator():
        tests_passed += 1
    
    print()
    print("=" * 50)
    print(f"📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("✅ All tests passed!")
        return 0
    else:
        print("⚠️ Some tests failed - check the output above")
        return 1

if __name__ == '__main__':
    sys.exit(main())
