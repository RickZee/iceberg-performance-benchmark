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

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from lib.env import get_snowflake_config

def load_env_vars():
    """Load environment variables from .env file if it exists"""
    env_file = project_root / '.env'
    
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

def get_external_volume_details(cursor, volume_name):
    """Get IAM details from external volume"""
    print(f"\n📦 Retrieving External Volume Details: {volume_name}...")
    print("=" * 70)
    
    try:
        cursor.execute(f"DESC EXTERNAL VOLUME {volume_name}")
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
            if len(row) >= 4:
                key = str(row[0]).strip() if row[0] is not None else ""
                sub_key = str(row[1]).strip() if row[1] is not None else ""
                # Value is in column 3 (index 3), not column 2
                value = row[3] if len(row) > 3 and row[3] is not None else None
                
                # Check if this is STORAGE_LOCATIONS with JSON data
                if key.upper() == 'STORAGE_LOCATIONS' and sub_key.startswith('STORAGE_LOCATION_'):
                    if value and isinstance(value, str) and value.startswith('{'):
                        try:
                            # Parse JSON to extract IAM details
                            storage_json = json.loads(value)
                            volume_details.update(storage_json)
                            
                            # Extract IAM details from JSON
                            if 'STORAGE_AWS_IAM_USER_ARN' in storage_json:
                                iam_user_arn = storage_json['STORAGE_AWS_IAM_USER_ARN']
                            if 'STORAGE_AWS_EXTERNAL_ID' in storage_json:
                                external_id = storage_json['STORAGE_AWS_EXTERNAL_ID']
                        except json.JSONDecodeError:
                            pass
                else:
                    # Regular field
                    value_str = str(value).strip() if value else None
                    volume_details[key] = value_str
        
        print(f"External Volume: {volume_name}")
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
            'volume_name': volume_name,
            'iam_user_arn': iam_user_arn,
            'external_id': external_id,
            'all_details': volume_details
        }
    except Exception as e:
        print(f"  ❌ Error retrieving external volume details: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_storage_integration_details(cursor):
    """Get IAM details from storage integration"""
    print("\n💾 Retrieving Storage Integration Details...")
    print("=" * 70)
    
    try:
        cursor.execute("DESC STORAGE INTEGRATION S3_INTEGRATION")
        results = cursor.fetchall()
        
        integration_details = {}
        iam_user_arn = None
        external_id = None
        
        # Print all results for debugging
        print("Raw DESC results (first 10 rows):")
        for idx, row in enumerate(results[:10]):
            print(f"  Row {idx}: {row}")
        
        # Parse all rows
        for row in results:
            if len(row) >= 3:
                key = str(row[0]).strip() if row[0] is not None else ""
                value = str(row[2]).strip() if len(row) > 2 and row[2] else None
                integration_details[key] = value
                
                # Look for IAM-related fields
                key_upper = key.upper()
                if 'IAM_USER_ARN' in key_upper and value and value != 'String' and value != 'None' and value.strip() and len(value) > 10:
                    iam_user_arn = value
                    print(f"  ✅ Found IAM User ARN in field '{key}': {value[:50]}...")
                if 'EXTERNAL_ID' in key_upper and value and value != 'String' and value != 'None' and value.strip() and len(value) > 5:
                    external_id = value
                    print(f"  ✅ Found External ID in field '{key}': {value[:30]}...")
        
        print(f"Storage Integration: S3_INTEGRATION")
        if iam_user_arn:
            print(f"  IAM User ARN: {iam_user_arn}")
        else:
            print(f"  ⚠️  IAM User ARN: Not found in DESC output")
            print(f"     Available fields: {list(integration_details.keys())[:10]}...")
        if external_id:
            print(f"  External ID: {external_id}")
        else:
            print(f"  ⚠️  External ID: Not found in DESC output")
        
        return {
            'iam_user_arn': iam_user_arn,
            'external_id': external_id,
            'all_details': integration_details
        }
    except Exception as e:
        print(f"  ❌ Error retrieving storage integration details: {e}")
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

def generate_trust_policy_json(iceberg_sf_volume_details, iceberg_glue_volume_details, catalog_details, storage_integration_details):
    """
    Generate the trust policy JSON with ALL external IDs
    We need FOUR external IDs:
    1. ICEBERG_VOLUME (for Iceberg SF tables)
    2. ICEBERG_GLUE_VOLUME (for Glue tables - external volume)
    3. AWS_GLUE_CATALOG (for Glue tables - catalog integration)
    4. S3_INTEGRATION (for External tables - storage integration)
    
    All use the same IAM user ARN but have different external IDs.
    """
    # Get IAM user ARN (should be the same for all)
    iam_user_arn = None
    if catalog_details and catalog_details.get('iam_user_arn'):
        iam_user_arn = catalog_details['iam_user_arn']
    elif storage_integration_details and storage_integration_details.get('iam_user_arn'):
        iam_user_arn = storage_integration_details['iam_user_arn']
    elif iceberg_glue_volume_details and iceberg_glue_volume_details.get('iam_user_arn'):
        iam_user_arn = iceberg_glue_volume_details['iam_user_arn']
    elif iceberg_sf_volume_details and iceberg_sf_volume_details.get('iam_user_arn'):
        iam_user_arn = iceberg_sf_volume_details['iam_user_arn']
    
    if not iam_user_arn:
        return None
    
    # Get external IDs from all sources
    iceberg_sf_external_id = iceberg_sf_volume_details.get('external_id') if iceberg_sf_volume_details else None
    iceberg_glue_external_id = iceberg_glue_volume_details.get('external_id') if iceberg_glue_volume_details else None
    catalog_external_id = catalog_details.get('external_id') if catalog_details else None
    storage_integration_external_id = storage_integration_details.get('external_id') if storage_integration_details else None
    
    # Build trust policy with all external IDs
    statements = []
    
    # Statement 1: ICEBERG_VOLUME (for Iceberg SF tables)
    if iceberg_sf_external_id:
        statements.append({
            "Effect": "Allow",
            "Principal": {
                "AWS": iam_user_arn
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": iceberg_sf_external_id
                }
            }
        })
    
    # Statement 2: ICEBERG_GLUE_VOLUME (for Glue tables - external volume)
    if iceberg_glue_external_id:
        statements.append({
            "Effect": "Allow",
            "Principal": {
                "AWS": iam_user_arn
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": iceberg_glue_external_id
                }
            }
        })
    
    # Statement 3: AWS_GLUE_CATALOG (for Glue tables - catalog integration)
    if catalog_external_id:
        statements.append({
            "Effect": "Allow",
            "Principal": {
                "AWS": iam_user_arn
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": catalog_external_id
                }
            }
        })
    
    # Statement 4: S3_INTEGRATION (for External tables - storage integration)
    if storage_integration_external_id:
        statements.append({
            "Effect": "Allow",
            "Principal": {
                "AWS": iam_user_arn
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": storage_integration_external_id
                }
            }
        })
    
    # If no external IDs, create a single statement without condition
    if not statements:
        statements.append({
            "Effect": "Allow",
            "Principal": {
                "AWS": iam_user_arn
            },
            "Action": "sts:AssumeRole"
        })
    
    policy = {
        "Version": "2012-10-17",
        "Statement": statements
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
        
        # Get external volume details for ICEBERG_VOLUME (Iceberg SF tables)
        iceberg_sf_volume_details = get_external_volume_details(cursor, 'ICEBERG_VOLUME')
        
        # Get external volume details for ICEBERG_GLUE_VOLUME (Glue tables)
        iceberg_glue_volume_details = get_external_volume_details(cursor, 'ICEBERG_GLUE_VOLUME')
        
        # Get catalog integration details
        catalog_details = get_catalog_integration_details(cursor)
        
        # Get storage integration details (for External tables)
        storage_integration_details = get_storage_integration_details(cursor)
        
        cursor.close()
        conn.close()
        
        # Determine which values to use
        print("\n" + "=" * 70)
        print("IAM DETAILS SUMMARY")
        print("=" * 70)
        
        # Get IAM user ARN (should be the same for all)
        iam_user_arn = None
        if catalog_details and catalog_details.get('iam_user_arn'):
            iam_user_arn = catalog_details['iam_user_arn']
        elif storage_integration_details and storage_integration_details.get('iam_user_arn'):
            iam_user_arn = storage_integration_details['iam_user_arn']
        elif iceberg_glue_volume_details and iceberg_glue_volume_details.get('iam_user_arn'):
            iam_user_arn = iceberg_glue_volume_details['iam_user_arn']
        elif iceberg_sf_volume_details and iceberg_sf_volume_details.get('iam_user_arn'):
            iam_user_arn = iceberg_sf_volume_details['iam_user_arn']
        
        if not iam_user_arn:
            print("\n❌ Could not retrieve IAM user ARN from any source")
            print("   Please check that the integrations were created successfully")
            sys.exit(1)
        
        # Get external IDs from all sources
        iceberg_sf_external_id = iceberg_sf_volume_details.get('external_id') if iceberg_sf_volume_details else None
        iceberg_glue_external_id = iceberg_glue_volume_details.get('external_id') if iceberg_glue_volume_details else None
        catalog_external_id = catalog_details.get('external_id') if catalog_details else None
        storage_integration_external_id = storage_integration_details.get('external_id') if storage_integration_details else None
        
        print(f"\n📋 IAM User ARN (same for all): {iam_user_arn}")
        print(f"\n📋 External IDs (different for each integration):")
        if iceberg_sf_external_id:
            print(f"   ✅ ICEBERG_VOLUME (Iceberg SF): {iceberg_sf_external_id}")
        else:
            print(f"   ⚠️  ICEBERG_VOLUME External ID: Not found")
        if iceberg_glue_external_id:
            print(f"   ✅ ICEBERG_GLUE_VOLUME (Glue tables): {iceberg_glue_external_id}")
        else:
            print(f"   ⚠️  ICEBERG_GLUE_VOLUME External ID: Not found")
        if catalog_external_id:
            print(f"   ✅ AWS_GLUE_CATALOG (Glue catalog): {catalog_external_id}")
        else:
            print(f"   ⚠️  AWS_GLUE_CATALOG External ID: Not found")
        if storage_integration_external_id:
            print(f"   ✅ S3_INTEGRATION (External tables): {storage_integration_external_id}")
        else:
            print(f"   ⚠️  S3_INTEGRATION External ID: Not found")
        
        # Count how many external IDs we have
        external_ids = [id for id in [iceberg_sf_external_id, iceberg_glue_external_id, catalog_external_id, storage_integration_external_id] if id]
        if len(external_ids) > 1:
            print(f"\n💡 IMPORTANT: {len(external_ids)} different external IDs found!")
            print(f"   The trust policy will include {len(external_ids)} statements - one for each external ID.")
        elif len(external_ids) == 1:
            print(f"\n⚠️  WARNING: Only one external ID found. All four are needed for full functionality.")
        else:
            print(f"\n❌ ERROR: No external IDs found!")
        
        # Generate trust policy JSON with all external IDs
        trust_policy = generate_trust_policy_json(iceberg_sf_volume_details, iceberg_glue_volume_details, catalog_details, storage_integration_details)
        
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
            if iceberg_sf_external_id:
                f.write(f"ICEBERG_SF_VOLUME_EXTERNAL_ID={iceberg_sf_external_id}\n")
            if iceberg_glue_external_id:
                f.write(f"ICEBERG_GLUE_VOLUME_EXTERNAL_ID={iceberg_glue_external_id}\n")
            if catalog_external_id:
                f.write(f"CATALOG_EXTERNAL_ID={catalog_external_id}\n")
            if storage_integration_external_id:
                f.write(f"S3_INTEGRATION_EXTERNAL_ID={storage_integration_external_id}\n")
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

