"""
Copy 2025 season data from existing DynamoDB tables to FantasyBaseball-HistoricalSeasons.

Handles:
- SeasonTrends: Items with clean TeamNumber (1-12) copy directly.
  Items with ROW_XX TeamNumber get resolved via Team name -> number mapping.
- Schedule: Uses Team_Number field (not TeamNumber).

Usage:
  python scripts/copy_2025_to_historical.py
"""

import sys, io
from decimal import Decimal
from collections import Counter

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import boto3

REGION = 'us-west-2'
dynamodb = boto3.resource('dynamodb', region_name=REGION)

source_trends = dynamodb.Table('FantasyBaseball-SeasonTrends')
source_schedule = dynamodb.Table('FantasyBaseball-Schedule')
dest = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

YEAR = 2025
VALID_TEAM_NUMBERS = {str(i) for i in range(1, 13)}


def scan_all(table):
    """Scan all items from a DynamoDB table."""
    items = []
    response = table.scan()
    items.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    return items


def batch_write(table, items):
    """Write items in batches of 25, deduplicating by PK+SK."""
    seen = {}
    for item in items:
        key = (item['YearTeamNumber'], item['DataTypeWeek'])
        seen[key] = item
    items = list(seen.values())

    written = 0
    for i in range(0, len(items), 25):
        batch = items[i:i+25]
        with table.batch_writer() as writer:
            for item in batch:
                writer.put_item(Item=item)
        written += len(batch)
    return written


def to_str(val):
    """Convert DynamoDB value to clean string."""
    if val is None:
        return ''
    if isinstance(val, Decimal):
        return str(int(val))
    return str(val)


# ============================================================
# 1. Delete existing 2025 data from HistoricalSeasons
# ============================================================
print("Cleaning existing 2025 data from HistoricalSeasons...")
existing = []
response = dest.scan(
    FilterExpression='#y = :year',
    ExpressionAttributeNames={'#y': 'Year'},
    ExpressionAttributeValues={':year': YEAR}
)
existing.extend(response.get('Items', []))
while 'LastEvaluatedKey' in response:
    response = dest.scan(
        FilterExpression='#y = :year',
        ExpressionAttributeNames={'#y': 'Year'},
        ExpressionAttributeValues={':year': YEAR},
        ExclusiveStartKey=response['LastEvaluatedKey']
    )
    existing.extend(response.get('Items', []))

if existing:
    print(f"  Deleting {len(existing)} existing 2025 items...")
    with dest.batch_writer() as writer:
        for item in existing:
            writer.delete_item(Key={
                'YearTeamNumber': item['YearTeamNumber'],
                'DataTypeWeek': item['DataTypeWeek']
            })
    print(f"  Deleted.")
else:
    print("  No existing 2025 data found.")

# ============================================================
# 2. Scan all SeasonTrends items
# ============================================================
print("\nScanning FantasyBaseball-SeasonTrends...")
all_items = scan_all(source_trends)
print(f"  Found {len(all_items)} total items")

# ============================================================
# 3. Build team name -> number mapping from clean items
# ============================================================
team_name_map = {}
for item in all_items:
    tn = to_str(item.get('TeamNumber', ''))
    team = item.get('Team', '')
    if tn in VALID_TEAM_NUMBERS and team and team not in team_name_map:
        team_name_map[team] = tn

print(f"  Built team name map: {len(team_name_map)} names -> numbers")
for name, num in sorted(team_name_map.items(), key=lambda x: int(x[1])):
    print(f"    {num}: {name}")

# ============================================================
# 4. Process all items
# ============================================================
data_types = Counter()
skipped = 0
historical_items = []

for item in all_items:
    # Resolve team number
    raw_tn = to_str(item.get('TeamNumber', ''))
    if raw_tn in VALID_TEAM_NUMBERS:
        team_number = raw_tn
    else:
        # Try team name lookup
        team = item.get('Team', '')
        team_number = team_name_map.get(team)
        if not team_number:
            skipped += 1
            continue

    # Get clean DataType and Week
    data_type = item.get('DataType', '')
    if not data_type:
        skipped += 1
        continue

    week = item.get('Week', 0)
    try:
        week = int(Decimal(str(week)) if not isinstance(week, (int, float)) else week)
    except (ValueError, TypeError):
        week = 0

    data_types[data_type] += 1

    new_item = dict(item)
    new_item['YearTeamNumber'] = f"{YEAR}#{team_number}"
    new_item['DataTypeWeek'] = f"{data_type}#{week:02d}"
    new_item['YearDataType'] = f"{YEAR}#{data_type}"
    new_item['Week'] = week
    new_item['Year'] = YEAR
    new_item['TeamNumber'] = team_number
    historical_items.append(new_item)

print(f"\n  Resolved: {len(historical_items)}, skipped: {skipped}")
print(f"\n  Data types:")
for dt, count in sorted(data_types.items()):
    print(f"    {dt}: {count}")

print(f"\nWriting to HistoricalSeasons (dedup will reduce count)...")
written = batch_write(dest, historical_items)
print(f"  Wrote {written} SeasonTrends items")

# ============================================================
# 5. Copy Schedule data
# ============================================================
print("\nScanning FantasyBaseball-Schedule...")
schedule_items = scan_all(source_schedule)
print(f"  Found {len(schedule_items)} items")

sched_historical = []
for item in schedule_items:
    # Schedule uses Team_Number, not TeamNumber
    tn = item.get('Team_Number')
    if tn is None:
        continue
    team_number = to_str(tn)
    if team_number not in VALID_TEAM_NUMBERS:
        continue

    week = item.get('Week', 0)
    try:
        week = int(Decimal(str(week)) if not isinstance(week, (int, float)) else week)
    except (ValueError, TypeError):
        week = 0

    new_item = dict(item)
    new_item['YearTeamNumber'] = f"{YEAR}#{team_number}"
    new_item['DataTypeWeek'] = f"schedule#{week:02d}"
    new_item['YearDataType'] = f"{YEAR}#schedule"
    new_item['Week'] = week
    new_item['Year'] = YEAR
    new_item['TeamNumber'] = team_number
    sched_historical.append(new_item)

written = batch_write(dest, sched_historical)
print(f"  Wrote {written} Schedule items")

print(f"\nDone!")
