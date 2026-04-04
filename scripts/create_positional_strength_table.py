"""
One-time script: create the FantasyBaseball-PositionalStrength DynamoDB table.

Schema:
  PK: TeamNumber (String)   — '0' for computed results
  SK: DataType#Week (String) — e.g. 'computed#positional_strength'

Usage:
  python scripts/create_positional_strength_table.py
"""

import boto3
from botocore.exceptions import ClientError

REGION     = 'us-west-2'
TABLE_NAME = 'FantasyBaseball-PositionalStrength'


def main():
    dynamodb = boto3.resource('dynamodb', region_name=REGION)

    try:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'TeamNumber',    'KeyType': 'HASH'},
                {'AttributeName': 'DataType#Week', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'TeamNumber',    'AttributeType': 'S'},
                {'AttributeName': 'DataType#Week', 'AttributeType': 'S'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        print(f"Creating {TABLE_NAME}...")
        table.wait_until_exists()
        print(f"Done. Table ARN: {table.table_arn}")

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"{TABLE_NAME} already exists.")
        else:
            raise


if __name__ == '__main__':
    main()
