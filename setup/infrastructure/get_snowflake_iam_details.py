#!/usr/bin/env python3
"""
Retrieve Snowflake IAM User ARN and External ID for Cross-Account Configuration
These values are needed to update the AWS IAM role trust policy.
"""

import os
import sys
import json
from pathlib import Path
import snowflake.connector
from lib.env import get_snowflake_config

def load_env_vars():
    """Load environment variables from .env file if it exists"""
    project_root = Path(__file__).parent
    env_file = project_root / '.env'
    
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def get_external_volume_details(cursor):
    """Get IAM details from external volume"""
    print("\n📦 Retrieving External Volume Details...")
    print("=" * 70)
    
    try:
        cursor.execute("DESC EXTERNAL VOLUME ICEBERG_GLUE_VOLUME")
        results = cursor.fetchall()
        
        # Parse results - DESC returns columns: property, value, default, ...
        # We need to look at the actual column structure
        iam_user_arn = None
        external_id = None
        volume_details = {}
        
        # Print all results for debugging
        print("Raw DESC results (first 5 rows):")
        for idx, row in enumerate(results[:5]):
            print(f"  Row {idx}: {row}")
        
        # Parse all rows
        # DESC returns: (property, type, value, default, ...)
        for row in results:
            # Handle different result formats - DESC typically returns (property, type, value, default, ...)
            if len(row) >= 3:
                key = str(row[0]).strip() if row[0] is not None else ""
                # Value is in column 2 (index 2), not column 1
                value = str(row[2]).strip() if len(row) > 2 and row[2] is not None else None
                volume_details[key] = value
                
                # Look for IAM-related fields
                key_upper = key.upper()
                if 'IAM_USER_ARN' in key_upper or 'STORAGE_AWS_IAM_USER_ARN' in key_upper:
                    if value and value != 'String' and value != 'None':
                        iam_user_arn = value
                if 'EXTERNAL_ID' in key_upper or 'STORAGE_AWS_EXTERNAL_ID' in key_upper:
                    if value and value != 'String' and value != 'None':
                        external_id = value
        
        print(f"External Volume: ICEBERG_GLUE_VOLUME")
        if iam_user_arn:
            print(f"  IAM User ARN: {iam_user_arn}")
        else:
            print(f"  ⚠️  IAM User ARN: Not found in DESC output")
            print(f"     Available fields: {list(volume_details.keys())[:5]}...")
        if external_id:
            print(f"  External ID: {external_id}")
        else:
            print(f"  ⚠️  External ID: Not found in DESC output")
        
        return {
            'iam_user_arn': iam_user_arn,
            'external_id': external_id,
            'all_details': volume_details
        }
    except Exception as e:
        print(f"  ❌ Error retrieving external volume details: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_catalog_integration_details(cursor):
    """Get IAM details from catalog integration"""
    print("\n📋 Retrieving Catalog Integration Details...")
    print("=" * 70)
    
    try:
        cursor.execute("DESC CATALOG INTEGRATION AWS_GLUE_CATALOG")
        results = cursor.fetchall()
        
        # Parse results - DESC returns columns: property, value, default, ...
        catalog_details = {}
        iam_user_arn = None
        external_id = None
        
        # Print all results for debugging
        print("Raw DESC results (first 10 rows):")
        for idx, row in enumerate(results[:10]):
            print(f"  Row {idx}: {row}")
        
        # Parse all rows
        # DESC returns: (property, type, value, default, ...)
        for row in results:
            # Handle different result formats - DESC typically returns (property, type, value, default, ...)
            if len(row) >= 3:
                key = str(row[0]).strip() if row[0] is not None else ""
                # Value is in column 2 (index 2), not column 1
                value = str(row[2]).strip() if len(row) > 2 and row[2] is not None else None
                catalog_details[key] = value
                
                # Look for IAM-related fields
                key_upper = key.upper()
                # For catalog integration, look for API_AWS_IAM_USER_ARN, REST_AWS_IAM_USER_ARN, etc.
                if ('IAM_USER_ARN' in key_upper or 'GLUE_AWS_IAM_USER_ARN' in key_upper or 
                    'CATALOG_AWS_IAM_USER_ARN' in key_upper or 'REST_AWS_IAM_USER_ARN' in key_upper or
                    'API_AWS_IAM_USER_ARN' in key_upper):
                    if value and value != 'String' and value != 'None' and value.strip() and len(value) > 10:
                        iam_user_arn = value
                        print(f"  ✅ Found IAM User ARN in field '{key}': {value[:50]}...")
                if ('EXTERNAL_ID' in key_upper or 'GLUE_AWS_EXTERNAL_ID' in key_upper or 
                    'CATALOG_AWS_EXTERNAL_ID' in key_upper or 'REST_AWS_EXTERNAL_ID' in key_upper or
                    'API_AWS_EXTERNAL_ID' in key_upper):
                    if value and value != 'String' and value != 'None' and value.strip() and len(value) > 5:
                        external_id = value
                        print(f"  ✅ Found External ID in field '{key}': {value[:30]}...")
        
        print(f"Catalog Integration: AWS_GLUE_CATALOG")
        if iam_user_arn:
            print(f"  IAM User ARN: {iam_user_arn}")
        else:
            print(f"  ⚠️  IAM User ARN: Not found in DESC output")
            print(f"     Available fields: {list(catalog_details.keys())[:10]}...")
        if external_id:
            print(f"  External ID: {external_id}")
        else:
            print(f"  ⚠️  External ID: Not found in DESC output")
        
        return {
            'iam_user_arn': iam_user_arn,
            'external_id': external_id,
            'all_details': catalog_details
        }
    except Exception as e:
        print(f"  ❌ Error retrieving catalog integration details: {e}")
        import traceback
        traceback.print_exc()
        return None

def generate_trust_policy_json(iam_user_arn, external_id):
    """Generate the trust policy JSON"""
    if not iam_user_arn:
        return None
    
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "AWS": iam_user_arn
                },
                "Action": "sts:AssumeRole",
            }
        ]
    }
    
    # Add external ID condition if provided
    if external_id:
        policy["Statement"][0]["Condition"] = {
            "StringEquals": {
                "sts:ExternalId": external_id
            }
        }
    
    return policy

def main():
    """Main function"""
    print("=" * 70)
    print("SNOWFLAKE IAM DETAILS RETRIEVAL")
    print("=" * 70)
    print("\nThis script retrieves the IAM user ARN and External ID from Snowflake")
    print("that are needed to configure cross-account access in AWS.\n")
    
    # Load environment variables
    load_env_vars()
    
    # Get Snowflake configuration
    try:
        config = get_snowflake_config()
        
        print("🔌 Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=config['user'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database'],
            role=config['role'],
            private_key_file=config['private_key_file'],
            private_key_passphrase=config['private_key_passphrase'] if config['private_key_passphrase'] else None
        )
        print("✅ Connected to Snowflake successfully\n")
        
        cursor = conn.cursor()
        
        # Get external volume details
        volume_details = get_external_volume_details(cursor)
        
        # Get catalog integration details
        catalog_details = get_catalog_integration_details(cursor)
        
        cursor.close()
        conn.close()
        
        # Determine which values to use
        print("\n" + "=" * 70)
        print("RECOMMENDED VALUES FOR IAM TRUST POLICY")
        print("=" * 70)
        
        # Prefer catalog integration values, fallback to external volume
        iam_user_arn = None
        external_id = None
        
        if catalog_details and catalog_details.get('iam_user_arn'):
            iam_user_arn = catalog_details['iam_user_arn']
            external_id = catalog_details.get('external_id') or volume_details.get('external_id') if volume_details else None
            print("\n✅ Using values from Catalog Integration (recommended):")
        elif volume_details and volume_details.get('iam_user_arn'):
            iam_user_arn = volume_details['iam_user_arn']
            external_id = volume_details.get('external_id')
            print("\n✅ Using values from External Volume:")
        else:
            print("\n❌ Could not retrieve IAM user ARN from either source")
            print("   Please check that the integrations were created successfully")
            sys.exit(1)
        
        print(f"   IAM User ARN: {iam_user_arn}")
        if external_id:
            print(f"   External ID: {external_id}")
        else:
            print(f"   External ID: (not set - optional but recommended)")
        
        # Generate trust policy JSON
        trust_policy = generate_trust_policy_json(iam_user_arn, external_id)
        
        print("\n" + "=" * 70)
        print("IAM TRUST POLICY JSON")
        print("=" * 70)
        print("\nCopy this JSON and use it to update the trust policy in AWS IAM Console:")
        print("\n1. Go to AWS IAM Console → Roles → snowflake-iceberg-role")
        print("2. Click 'Trust relationships' tab")
        print("3. Click 'Edit trust policy'")
        print("4. Replace the existing policy with the JSON below:")
        print("\n" + "-" * 70)
        print(json.dumps(trust_policy, indent=2))
        print("-" * 70)
        
        # Save to file
        output_file = Path(__file__).parent / 'trust_policy.json'
        with open(output_file, 'w') as f:
            json.dump(trust_policy, f, indent=2)
        print(f"\n💾 Trust policy JSON saved to: {output_file}")
        
        # Save values to a script-friendly format
        values_file = Path(__file__).parent / 'snowflake_iam_values.txt'
        with open(values_file, 'w') as f:
            f.write(f"SNOWFLAKE_IAM_USER_ARN={iam_user_arn}\n")
            if external_id:
                f.write(f"SNOWFLAKE_EXTERNAL_ID={external_id}\n")
        print(f"💾 Values saved to: {values_file}")
        
        print("\n" + "=" * 70)
        print("NEXT STEPS")
        print("=" * 70)
        print("\n1. Update the IAM role trust policy in AWS Console using the JSON above")
        print("2. Or run: python3 setup/infrastructure/update_iam_trust_policy.py")
        print("3. After updating, test the connection by running:")
        print("   SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center LIMIT 1;")
        print("\n" + "=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure you have:")
        print("  1. Set up environment variables (SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, etc.)")
        print("  2. Created a .env file or exported environment variables")
        print("  3. Configured your private key file (SNOWFLAKE_PRIVATE_KEY_FILE)")
        print("  4. Created the external volume and catalog integration in Snowflake")
        sys.exit(1)

if __name__ == "__main__":
    main()

