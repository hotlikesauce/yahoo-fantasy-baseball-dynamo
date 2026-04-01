"""
Delete ALL items with Year=2026 from FantasyBaseball-SeasonTrends.
Prints every item before deleting. Touches NOTHING without Year=2026.
Run: python scripts/clear_2026_dynamo.py
"""
import boto3
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

print("Scanning for Year=2026 items...")

items = []
scan_kwargs = {'FilterExpression': Attr('Year').eq(2026)}

while True:
    resp = table.scan(**scan_kwargs)
    items.extend(resp['Items'])
    if 'LastEvaluatedKey' not in resp:
        break
    scan_kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']

print(f"\nFound {len(items)} items with Year=2026:")
for item in sorted(items, key=lambda x: (x.get('DataType#Week', ''), x.get('TeamNumber', ''))):
    print(f"  {item['TeamNumber']:10s}  {item['DataType#Week']}")

if not items:
    print("Nothing to delete.")
    exit()

confirm = input(f"\nDelete all {len(items)} items? Type YES to confirm: ")
if confirm.strip() != 'YES':
    print("Aborted.")
    exit()

with table.batch_writer() as batch:
    for item in items:
        batch.delete_item(Key={
            'TeamNumber': item['TeamNumber'],
            'DataType#Week': item['DataType#Week'],
        })

print(f"Deleted {len(items)} items. Done.")
