"""
Enable Point-in-Time Recovery (PITR) for DynamoDB tables.

PITR provides continuous backups for the last 35 days, protecting against
accidental deletes or writes.

Usage:
    python scripts/enable_pitr.py
"""

import boto3
from botocore.exceptions import ClientError


def enable_pitr(dynamodb, table_name):
    """Enable Point-in-Time Recovery for a table."""
    try:
        # Check current PITR status
        response = dynamodb.describe_continuous_backups(TableName=table_name)
        pitr_status = response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']

        if pitr_status == 'ENABLED':
            print(f"✅ {table_name}: PITR already enabled")
            return True

        # Enable PITR
        dynamodb.update_continuous_backups(
            TableName=table_name,
            PointInTimeRecoverySpecification={
                'PointInTimeRecoveryEnabled': True
            }
        )
        print(f"✅ {table_name}: PITR enabled")
        return True

    except ClientError as e:
        print(f"❌ {table_name}: Failed to enable PITR - {e}")
        return False


def main():
    """Enable PITR for all Fantasy Baseball tables."""

    print("\n" + "="*60)
    print("🛡️  Enabling Point-in-Time Recovery (PITR)")
    print("="*60)

    try:
        dynamodb = boto3.client('dynamodb', region_name='us-west-2')
        print("✅ Connected to AWS")
    except Exception as e:
        print(f"❌ Failed to connect to AWS: {e}")
        return

    # Tables to protect
    tables = [
        'FantasyBaseball-LiveData',
        'FantasyBaseball-WeeklyTimeSeries',
        'FantasyBaseball-MatchupResults',
        'FantasyBaseball-Schedule',
        'FantasyBaseball-AllTimeHistory'
    ]

    print(f"\n📋 Enabling PITR for {len(tables)} tables...\n")

    success_count = 0
    for table_name in tables:
        if enable_pitr(dynamodb, table_name):
            success_count += 1

    # Summary
    print("\n" + "="*60)
    print(f"✅ Enabled PITR for {success_count}/{len(tables)} tables")
    print("\n💡 PITR provides:")
    print("   - Continuous backups for the last 35 days")
    print("   - Protection against accidental deletes/updates")
    print("   - Point-in-time restore capability")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
