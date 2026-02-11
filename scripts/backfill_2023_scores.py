"""
Backfill 2023 power_ranks_season_trend with Score_Sum and per-category scores
from MongoDB running_normalized_ranks.

The existing DynamoDB items for 2023 have Score_Sum=0 or missing.
MongoDB has the real values for weeks 1-20.

This script:
1. Reads running_normalized_ranks from MongoDB Atlas (YahooFantasyBaseball_2023)
2. Updates existing DynamoDB items (weeks 0-12) with Score_Sum + category scores
3. Inserts new items for weeks 13-20

Usage:
  python scripts/backfill_2023_scores.py
"""

import sys, io, os, certifi
from decimal import Decimal, InvalidOperation
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pymongo import MongoClient
import boto3

MONGO_URI = os.environ.get('MONGO_CLIENT',
    "mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/?appName=Cluster0")

DYNAMO_TABLE = 'FantasyBaseball-HistoricalSeasons'
DYNAMO_REGION = 'us-west-2'
YEAR = 2023

# Score fields to copy from MongoDB
SCORE_FIELDS = [
    'Score_Sum', 'Score_Rank', 'Score_Variation',
    'R_Stats_Score', 'H_Stats_Score', 'HR_Stats_Score',
    'RBI_Stats_Score', 'SB_Stats_Score', 'OPS_Stats_Score',
    'K9_Stats_Score', 'QS_Stats_Score', 'SVH_Stats_Score',
    'HRA_Stats_Score', 'ERA_Stats_Score', 'WHIP_Stats_Score',
]

# All fields to migrate for NEW items (weeks 13-20)
ALL_FIELDS = [
    'Team', 'Team_Number', 'Week',
    'R_Stats', 'H_Stats', 'HR_Stats', 'RBI_Stats', 'SB_Stats', 'OPS_Stats',
    'HRA_Stats', 'ERA_Stats', 'WHIP_Stats', 'K9_Stats', 'QS_Stats', 'SVH_Stats',
    'R_Rank_Stats', 'H_Rank_Stats', 'HR_Rank_Stats', 'RBI_Rank_Stats',
    'SB_Rank_Stats', 'OPS_Rank_Stats', 'HRA_Rank_Stats', 'ERA_Rank_Stats',
    'WHIP_Rank_Stats', 'K9_Rank_Stats', 'QS_Rank_Stats', 'SVH_Rank_Stats',
    'R_Record', 'H_Record', 'HR_Record', 'RBI_Record', 'SB_Record', 'OPS_Record',
    'HRA_Record', 'ERA_Record', 'WHIP_Record', 'K9_Record', 'QS_Record', 'SVH_Record',
    'R_Rank_Record', 'H_Rank_Record', 'HR_Rank_Record', 'RBI_Rank_Record',
    'SB_Rank_Record', 'OPS_Rank_Record', 'HRA_Rank_Record', 'ERA_Rank_Record',
    'WHIP_Rank_Record', 'K9_Rank_Record', 'QS_Rank_Record', 'SVH_Rank_Record',
    'Rank', 'GB', 'Moves',
    'Stats_Power_Score', 'Stats_Power_Rank', 'Variation',
    'batter_rank', 'pitcher_rank',
] + SCORE_FIELDS


def to_decimal(val):
    """Convert a value to Decimal for DynamoDB."""
    if val is None:
        return None
    if isinstance(val, list):
        return [to_decimal(v) for v in val]
    if isinstance(val, str):
        try:
            return Decimal(val)
        except InvalidOperation:
            return val  # keep as string
    if isinstance(val, float):
        return Decimal(str(val))
    if isinstance(val, int):
        return Decimal(val)
    return val


def main():
    # Connect to MongoDB
    print("Connecting to MongoDB Atlas...")
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client['YahooFantasyBaseball_2023']
    coll = db['running_normalized_ranks']

    docs = list(coll.find())
    print(f"  {len(docs)} documents from running_normalized_ranks")

    # Connect to DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=DYNAMO_REGION)
    table = dynamodb.Table(DYNAMO_TABLE)

    # Get existing 2023 power_ranks items to know which to update vs insert
    from boto3.dynamodb.conditions import Key
    existing = {}
    resp = table.query(
        IndexName='YearDataTypeIndex',
        KeyConditionExpression=Key('YearDataType').eq(f'{YEAR}#power_ranks_season_trend')
    )
    for item in resp['Items']:
        tn = item.get('TeamNumber', '')
        week = int(float(item.get('Week', 0)))
        existing[(str(tn), week)] = item
    while 'LastEvaluatedKey' in resp:
        resp = table.query(
            IndexName='YearDataTypeIndex',
            KeyConditionExpression=Key('YearDataType').eq(f'{YEAR}#power_ranks_season_trend'),
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        for item in resp['Items']:
            tn = item.get('TeamNumber', '')
            week = int(float(item.get('Week', 0)))
            existing[(str(tn), week)] = item

    print(f"  {len(existing)} existing DynamoDB items for 2023 power_ranks")

    updated = 0
    inserted = 0
    errors = 0

    for doc in docs:
        tn = str(int(doc.get('Team_Number', 0)))
        week = int(doc.get('Week', 0))
        team_name = doc.get('Team', '')

        if not tn or tn == '0':
            print(f"  SKIP: no team number for {team_name} week {week}")
            errors += 1
            continue

        key = (tn, week)

        if key in existing:
            # UPDATE existing item with score fields
            update_expr_parts = []
            expr_values = {}
            for i, field in enumerate(SCORE_FIELDS):
                if field in doc:
                    attr_name = f':v{i}'
                    update_expr_parts.append(f'{field} = {attr_name}')
                    expr_values[attr_name] = to_decimal(doc[field])

            if update_expr_parts:
                try:
                    table.update_item(
                        Key={
                            'YearTeamNumber': f'{YEAR}#{tn}',
                            'DataTypeWeek': f'power_ranks_season_trend#{week:02d}',
                        },
                        UpdateExpression='SET ' + ', '.join(update_expr_parts),
                        ExpressionAttributeValues=expr_values,
                    )
                    updated += 1
                except Exception as e:
                    print(f"  ERROR updating TN={tn} week={week}: {e}")
                    errors += 1
        else:
            # INSERT new item
            item = {
                'YearTeamNumber': f'{YEAR}#{tn}',
                'DataTypeWeek': f'power_ranks_season_trend#{week:02d}',
                'YearDataType': f'{YEAR}#power_ranks_season_trend',
                'TeamNumber': tn,
                'Year': Decimal(YEAR),
            }
            for field in ALL_FIELDS:
                if field in doc:
                    val = to_decimal(doc[field])
                    if val is not None:
                        item[field] = val

            try:
                table.put_item(Item=item)
                inserted += 1
            except Exception as e:
                print(f"  ERROR inserting TN={tn} week={week}: {e}")
                errors += 1

        if (updated + inserted) % 24 == 0 and (updated + inserted) > 0:
            print(f"  Progress: {updated} updated, {inserted} inserted...")

    print(f"\nDone! {updated} updated, {inserted} inserted, {errors} errors")
    print(f"  Total 2023 power_ranks items now: {len(existing) + inserted}")


if __name__ == '__main__':
    main()
