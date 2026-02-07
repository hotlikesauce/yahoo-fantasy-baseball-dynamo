"""
Setup script to create all DynamoDB tables for Yahoo Fantasy Baseball Analyzer.

This script creates 5 DynamoDB tables with proper configuration:
1. LiveData - Current/latest data (overwrite pattern)
2. WeeklyTimeSeries - Week-by-week historical data
3. MatchupResults - Weekly matchup results
4. Schedule - League schedule
5. AllTimeHistory - All-time rankings across seasons

Usage:
    python scripts/setup_dynamodb_tables.py
"""

import boto3
from botocore.exceptions import ClientError
import sys


def create_table_with_retry(dynamodb, table_config):
    """Create a DynamoDB table with error handling."""
    table_name = table_config['TableName']

    try:
        print(f"\n{'='*60}")
        print(f"Creating table: {table_name}")
        print(f"{'='*60}")

        # Check if table already exists
        try:
            existing_table = dynamodb.describe_table(TableName=table_name)
            print(f"⚠️  Table {table_name} already exists!")
            print(f"   Status: {existing_table['Table']['TableStatus']}")
            return existing_table['Table']
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise

        # Create the table
        response = dynamodb.create_table(**table_config)

        print(f"✅ Table {table_name} creation initiated")
        print(f"   Partition Key: {table_config['KeySchema'][0]['AttributeName']}")
        if len(table_config['KeySchema']) > 1:
            print(f"   Sort Key: {table_config['KeySchema'][1]['AttributeName']}")

        if 'GlobalSecondaryIndexes' in table_config:
            print(f"   GSIs: {len(table_config['GlobalSecondaryIndexes'])}")
            for gsi in table_config['GlobalSecondaryIndexes']:
                print(f"      - {gsi['IndexName']}")

        return response['TableDescription']

    except ClientError as e:
        print(f"❌ Error creating table {table_name}: {e}")
        return None


def main():
    """Create all DynamoDB tables."""

    print("\n" + "="*60)
    print("🚀 Yahoo Fantasy Baseball - DynamoDB Setup")
    print("="*60)

    # Initialize boto3 client
    try:
        dynamodb = boto3.client('dynamodb', region_name='us-west-2')

        # Test connection
        print("\n📡 Testing AWS connection...")
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ Connected as: {identity['Arn']}")
        print(f"   Account: {identity['Account']}")

    except Exception as e:
        print(f"❌ Failed to connect to AWS: {e}")
        print("\nPlease ensure:")
        print("  1. AWS CLI is configured: aws configure")
        print("  2. You have valid credentials")
        print("  3. You have DynamoDB permissions")
        sys.exit(1)

    # Table 1: LiveData
    live_data_config = {
        'TableName': 'FantasyBaseball-LiveData',
        'KeySchema': [
            {'AttributeName': 'DataType', 'KeyType': 'HASH'},  # Partition key
            {'AttributeName': 'TeamNumber', 'KeyType': 'RANGE'}  # Sort key
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'DataType', 'AttributeType': 'S'},
            {'AttributeName': 'TeamNumber', 'AttributeType': 'S'}
        ],
        'BillingMode': 'PAY_PER_REQUEST',  # On-demand pricing
        'Tags': [
            {'Key': 'Project', 'Value': 'YahooFantasyBaseball'},
            {'Key': 'Environment', 'Value': 'Production'}
        ]
    }

    # Table 2: WeeklyTimeSeries
    weekly_timeseries_config = {
        'TableName': 'FantasyBaseball-WeeklyTimeSeries',
        'KeySchema': [
            {'AttributeName': 'TeamNumber', 'KeyType': 'HASH'},
            {'AttributeName': 'Week#DataType', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'TeamNumber', 'AttributeType': 'S'},
            {'AttributeName': 'Week#DataType', 'AttributeType': 'S'},
            {'AttributeName': 'DataType', 'AttributeType': 'S'},
            {'AttributeName': 'Week', 'AttributeType': 'N'}
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'DataTypeWeekIndex',
                'KeySchema': [
                    {'AttributeName': 'DataType', 'KeyType': 'HASH'},
                    {'AttributeName': 'Week', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'Tags': [
            {'Key': 'Project', 'Value': 'YahooFantasyBaseball'},
            {'Key': 'Environment', 'Value': 'Production'}
        ]
    }

    # Table 3: MatchupResults
    matchup_results_config = {
        'TableName': 'FantasyBaseball-MatchupResults',
        'KeySchema': [
            {'AttributeName': 'Week', 'KeyType': 'HASH'},
            {'AttributeName': 'TeamNumber', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'Week', 'AttributeType': 'N'},
            {'AttributeName': 'TeamNumber', 'AttributeType': 'S'}
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'TeamHistoryIndex',
                'KeySchema': [
                    {'AttributeName': 'TeamNumber', 'KeyType': 'HASH'},
                    {'AttributeName': 'Week', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'Tags': [
            {'Key': 'Project', 'Value': 'YahooFantasyBaseball'},
            {'Key': 'Environment', 'Value': 'Production'}
        ]
    }

    # Table 4: Schedule
    schedule_config = {
        'TableName': 'FantasyBaseball-Schedule',
        'KeySchema': [
            {'AttributeName': 'Week', 'KeyType': 'HASH'},
            {'AttributeName': 'TeamNumber', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'Week', 'AttributeType': 'N'},
            {'AttributeName': 'TeamNumber', 'AttributeType': 'S'}
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'Tags': [
            {'Key': 'Project', 'Value': 'YahooFantasyBaseball'},
            {'Key': 'Environment', 'Value': 'Production'}
        ]
    }

    # Table 5: AllTimeHistory
    alltime_history_config = {
        'TableName': 'FantasyBaseball-AllTimeHistory',
        'KeySchema': [
            {'AttributeName': 'TeamNumber', 'KeyType': 'HASH'},
            {'AttributeName': 'Year', 'KeyType': 'RANGE'}
        ],
        'AttributeDefinitions': [
            {'AttributeName': 'TeamNumber', 'AttributeType': 'S'},
            {'AttributeName': 'Year', 'AttributeType': 'S'},
            {'AttributeName': 'ScoreRank', 'AttributeType': 'N'}
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'YearIndex',
                'KeySchema': [
                    {'AttributeName': 'Year', 'KeyType': 'HASH'},
                    {'AttributeName': 'ScoreRank', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'Tags': [
            {'Key': 'Project', 'Value': 'YahooFantasyBaseball'},
            {'Key': 'Environment', 'Value': 'Production'}
        ]
    }

    # Create all tables
    tables = [
        live_data_config,
        weekly_timeseries_config,
        matchup_results_config,
        schedule_config,
        alltime_history_config
    ]

    created_tables = []
    for table_config in tables:
        result = create_table_with_retry(dynamodb, table_config)
        if result:
            created_tables.append(table_config['TableName'])

    # Summary
    print("\n" + "="*60)
    print("📊 Summary")
    print("="*60)
    print(f"✅ Successfully initiated: {len(created_tables)}/{len(tables)} tables")

    if created_tables:
        print("\n📝 Tables created:")
        for table_name in created_tables:
            print(f"   - {table_name}")

        print("\n⏳ Tables are being created (this takes 1-2 minutes)")
        print("\nTo check status:")
        print("   aws dynamodb list-tables --region us-east-1")
        print("\nTo wait for all tables to become active:")
        print("   aws dynamodb wait table-exists --table-name FantasyBaseball-LiveData --region us-east-1")

        print("\n💡 Next steps:")
        print("   1. Wait for tables to become ACTIVE")
        print("   2. Enable Point-in-Time Recovery (PITR) for safety:")
        print("      python scripts/enable_pitr.py")
        print("   3. Update scrapers to use DynamoStorageManager")
        print("   4. Test dual-write (MongoDB + DynamoDB)")

    print("\n" + "="*60)
    print("🎉 Setup complete!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
