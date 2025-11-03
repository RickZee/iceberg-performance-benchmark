#!/usr/bin/env python3
"""
Update AWS IAM Role Trust Policy with Snowflake IAM Details
This script automates the trust policy update in AWS IAM.
"""

import os
import sys
import json
import argparse
import boto3
from pathlib import Path

def load_snowflake_iam_values():
    """Load Snowflake IAM values from file or environment"""
    values_file = Path(__file__).parent / 'snowflake_iam_values.txt'
    trust_policy_file = Path(__file__).parent / 'trust_policy.json'
    
    # Try to load from trust_policy.json first
    if trust_policy_file.exists():
        with open(trust_policy_file, 'r') as f:
            trust_policy = json.load(f)
            # Extract values
            iam_user_arn = trust_policy['Statement'][0]['Principal']['AWS']
            external_id = None
            if 'Condition' in trust_policy['Statement'][0]:
                external_id = trust_policy['Statement'][0]['Condition']['StringEquals'].get('sts:ExternalId')
            return trust_policy, iam_user_arn, external_id
    
    # Try to load from values file
    if values_file.exists():
        values = {}
        with open(values_file, 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    values[key] = value
        iam_user_arn = values.get('SNOWFLAKE_IAM_USER_ARN')
        external_id = values.get('SNOWFLAKE_EXTERNAL_ID')
        
        if iam_user_arn:
            # Generate trust policy
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
            if external_id:
                policy["Statement"][0]["Condition"] = {
                    "StringEquals": {
                        "sts:ExternalId": external_id
                    }
                }
            return policy, iam_user_arn, external_id
    
    # Try environment variables
    iam_user_arn = os.getenv('SNOWFLAKE_IAM_USER_ARN')
    external_id = os.getenv('SNOWFLAKE_EXTERNAL_ID')
    
    if not iam_user_arn:
        print("❌ Error: Could not find Snowflake IAM user ARN")
        print("\nPlease run 'python3 setup/infrastructure/get_snowflake_iam_details.py' first,")
        print("or set SNOWFLAKE_IAM_USER_ARN environment variable")
        sys.exit(1)
    
    # Generate trust policy
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
    if external_id:
        policy["Statement"][0]["Condition"] = {
            "StringEquals": {
                "sts:ExternalId": external_id
            }
        }
    
    return policy, iam_user_arn, external_id

def get_iam_role_name():
    """Get IAM role name from environment or use default"""
    role_name = os.getenv('AWS_ROLE_NAME', 'snowflake-iceberg-role')
    return role_name

def update_trust_policy(role_name, trust_policy):
    """Update IAM role trust policy"""
    print(f"🔌 Connecting to AWS IAM...")
    
    try:
        iam_client = boto3.client('iam')
        
        # Verify role exists
        print(f"📋 Checking if role '{role_name}' exists...")
        try:
            role = iam_client.get_role(RoleName=role_name)
            print(f"✅ Found role: {role['Role']['Arn']}")
        except iam_client.exceptions.NoSuchEntityException:
            print(f"❌ Error: Role '{role_name}' not found in AWS IAM")
            print(f"\nPlease verify:")
            print(f"  1. The role name is correct (current: {role_name})")
            print(f"  2. AWS credentials are configured correctly")
            print(f"  3. You have permissions to access IAM")
            sys.exit(1)
        
        # Update trust policy
        print(f"\n🔄 Updating trust policy for role '{role_name}'...")
        trust_policy_json = json.dumps(trust_policy)
        
        response = iam_client.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=trust_policy_json
        )
        
        print("✅ Trust policy updated successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error updating trust policy: {e}")
        print("\nTroubleshooting:")
        print("  1. Verify AWS credentials are configured (aws configure)")
        print("  2. Check that you have iam:UpdateAssumeRolePolicy permission")
        print("  3. Verify the role name is correct")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description='Update AWS IAM Role Trust Policy with Snowflake IAM Details'
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip confirmation prompt (non-interactive mode)'
    )
    args = parser.parse_args()
    
    print("=" * 70)
    print("AWS IAM TRUST POLICY UPDATER")
    print("=" * 70)
    print("\nThis script updates the AWS IAM role trust policy with Snowflake's")
    print("IAM user ARN and external ID for cross-account access.\n")
    
    # Load Snowflake IAM values
    print("📥 Loading Snowflake IAM details...")
    try:
        trust_policy, iam_user_arn, external_id = load_snowflake_iam_values()
        print(f"✅ Loaded IAM User ARN: {iam_user_arn}")
        if external_id:
            print(f"✅ Loaded External ID: {external_id}")
        else:
            print("⚠️  No External ID found (optional but recommended)")
    except Exception as e:
        print(f"❌ Error loading values: {e}")
        print("\nPlease run 'python3 setup/infrastructure/get_snowflake_iam_details.py' first")
        sys.exit(1)
    
    # Get role name
    role_name = get_iam_role_name()
    print(f"\n📋 Target IAM Role: {role_name}")
    
    # Show the trust policy
    print("\n" + "=" * 70)
    print("TRUST POLICY TO APPLY")
    print("=" * 70)
    print(json.dumps(trust_policy, indent=2))
    print("=" * 70)
    
    # Confirm
    if not args.yes:
        print("\n⚠️  This will replace the existing trust policy for the IAM role.")
        try:
            response = input("Continue? (yes/no): ").strip().lower()
            if response not in ['yes', 'y']:
                print("Cancelled.")
                sys.exit(0)
        except EOFError:
            print("\n⚠️  No input available. Use --yes flag for non-interactive mode.")
            print("   Or run: python3 setup/infrastructure/update_iam_trust_policy.py --yes")
            sys.exit(1)
    else:
        print("\n⚠️  Non-interactive mode: proceeding with trust policy update...")
    
    # Update trust policy
    success = update_trust_policy(role_name, trust_policy)
    
    if success:
        print("\n" + "=" * 70)
        print("SUCCESS")
        print("=" * 70)
        print("\n✅ Trust policy has been updated successfully!")
        print("\nNext steps:")
        print("1. Test the connection from Snowflake:")
        print("   SELECT * FROM AWS_GLUE_CATALOG.iceberg_performance_test.call_center LIMIT 1;")
        print("\n2. If the test fails, check:")
        print("   - AWS CloudTrail logs for access denied errors")
        print("   - IAM role permissions (S3 and Glue access)")
        print("   - Network connectivity between Snowflake and AWS")
        print("\n" + "=" * 70)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()

