"""
Migrate MongoDB data to DynamoDB (v2 - 3 Table Design).

Migrates all 24 MongoDB collections to the optimized 3-table structure:
- SeasonTrends: All weekly time-series data
- TeamInfo: Team metadata
- Schedule: League schedule

Usage:
    python scripts/migrate_mongodb_to_dynamodb_v2.py
"""

import os
import sys
from pathlib import Path
from decimal import Decimal

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from dotenv import load_dotenv
import pandas as pd
from pymongo import MongoClient
import certifi
import boto3
from botocore.exceptions import ClientError

load_dotenv()

# MongoDB connection
MONGO_CLIENT = os.environ.get('MONGO_CLIENT')
MONGO_DB = 'YahooFantasyBaseball_2025'

# DynamoDB configuration
DYNAMO_REGION = 'us-west-2'


# Collection mapping to tables
SEASON_TRENDS_COLLECTIONS = {
    'power_ranks', 'Power_Ranks', 'power_ranks_lite', 'normalized_ranks',
    'live_standings', 'playoff_status', 'playoff_probabilities', 'playoff_probabilities_static',
    'coefficient', 'Coefficient_Last_Four', 'Coefficient_Last_Two',
    'Running_ELO', 'running_normalized_ranks',
    'power_ranks_season_trend', 'standings_season_trend',
    'weekly_stats', 'week_stats', 'weekly_results', 'weekly_luck_analysis'
}

TEAM_INFO_COLLECTIONS = {
    'team_dict', 'remaining_sos', 'seasons_best_long', 'seasons_best_regular'
}

SCHEDULE_COLLECTIONS = {
    'schedule'
}


def convert_floats_to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]
    return obj


def get_mongo_collections():
    """Get list of all collections in MongoDB."""
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client[MONGO_DB]
    collections = db.list_collection_names()
    client.close()
    return collections


def read_mongo_collection(collection_name: str) -> pd.DataFrame:
    """Read all data from a MongoDB collection."""
    ca = certifi.where()
    client = MongoClient(MONGO_CLIENT, tlsCAFile=ca)
    db = client[MONGO_DB]
    collection = db[collection_name]

    data = list(collection.find({}))
    df = pd.DataFrame(data)

    client.close()
    return df


def write_to_season_trends(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to SeasonTrends table."""
    if df.empty:
        print(f"   [WARNING]  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-SeasonTrends')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber field
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    # Determine Week field
    week_field = None
    for field in ['Week', 'week']:
        if field in df.columns:
            week_field = field
            break

    # If no team field found, try to use index as identifier or skip
    if not team_field:
        # For collections without team numbers, we'll use row index as TeamNumber
        print(f"   [INFO] No team number field found, using row index as identifier")

    # For non-weekly data, use Week=0 (current/latest)
    has_week = week_field is not None

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        # Get week number - fix pandas array ambiguity
        if has_week:
            week_val = row[week_field]
            week_num = int(week_val) if pd.notna(week_val) else 0
        else:
            week_num = 0

        # Get team number or use row index if no team field
        if team_field:
            team_num = str(row[team_field])
        else:
            team_num = f"ROW_{idx}"

        # Create composite sort key: DataType#Week#Index (to handle duplicates)
        sort_key = f"{collection_name}#{week_num}#{idx}"

        # Create GSI key: DataType#Week (same for all teams)
        datatype_week = f"{collection_name}#{week_num}"

        item = {
            'TeamNumber': team_num,
            'DataType#Week': sort_key,
            'DataTypeWeek': datatype_week,  # For GSI
            'Week': week_num,
            'DataType': collection_name,
        }

        # Add all other fields
        for col in df.columns:
            if col not in [team_field, week_field, '_id']:
                value = row[col]
                # Handle arrays/lists and scalar values
                try:
                    if isinstance(value, (list, tuple)):
                        # Convert list/array values
                        item[col] = convert_floats_to_decimal(value)
                    elif pd.notna(value):
                        # Convert scalar values
                        item[col] = convert_floats_to_decimal(value)
                except (ValueError, TypeError):
                    # Skip values that can't be converted
                    pass

        batch.append({'PutRequest': {'Item': item}})

        # Write in batches of 25
        if len(batch) >= 25:
            try:
                dynamodb.batch_write_item(RequestItems={'FantasyBaseball-SeasonTrends': batch})
                write_count += len(batch)
                batch = []
            except ClientError as e:
                print(f"   [ERROR] Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-SeasonTrends': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   [ERROR] Batch write error: {e}")

    return write_count


def write_to_team_info(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to TeamInfo table."""
    if df.empty:
        print(f"   [WARNING]  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-TeamInfo')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber field
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    if not team_field and collection_name == 'team_dict':
        team_field = 'Team_Number'

    if not team_field:
        print(f"   [WARNING]  No team number field found, storing as attributes")

    write_count = 0
    batch = []

    # If no team field, this is global data - store under TeamNumber="GLOBAL"
    if not team_field:
        item = {'TeamNumber': 'GLOBAL'}
        for col in df.columns:
            if col != '_id':
                # Store the entire dataframe as a single item
                value = df[col].tolist() if len(df) > 1 else df[col].iloc[0]
                if pd.notna(value):
                    item[col] = convert_floats_to_decimal(value)

        batch.append({'PutRequest': {'Item': item}})
    else:
        # Store per team
        for idx, row in df.iterrows():
            item = {
                'TeamNumber': str(row[team_field]),
                'DataType': collection_name,
            }

            for col in df.columns:
                if col not in [team_field, '_id']:
                    value = row[col]
                    if pd.notna(value):
                        item[col] = convert_floats_to_decimal(value)

            batch.append({'PutRequest': {'Item': item}})

            # Write in batches of 25
            if len(batch) >= 25:
                try:
                    dynamodb.batch_write_item(RequestItems={'FantasyBaseball-TeamInfo': batch})
                    write_count += len(batch)
                    batch = []
                except ClientError as e:
                    print(f"   [ERROR] Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-TeamInfo': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   [ERROR] Batch write error: {e}")

    return write_count


def write_to_schedule(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to Schedule table."""
    if df.empty:
        print(f"   [WARNING]  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-Schedule')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine fields
    week_field = None
    for field in ['Week', 'week']:
        if field in df.columns:
            week_field = field
            break

    if not week_field:
        print(f"   [WARNING]  No week field found, skipping")
        return 0

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        week_num = int(row[week_field]) if pd.notna(row[week_field]) else 0

        # Create a unique matchup ID
        matchup_id = f"matchup_{idx}"

        item = {
            'Week': week_num,
            'MatchupID': matchup_id,
        }

        # Add all other fields
        for col in df.columns:
            if col not in [week_field, '_id']:
                value = row[col]
                if pd.notna(value):
                    item[col] = convert_floats_to_decimal(value)

        batch.append({'PutRequest': {'Item': item}})

        # Write in batches of 25
        if len(batch) >= 25:
            try:
                dynamodb.batch_write_item(RequestItems={'FantasyBaseball-Schedule': batch})
                write_count += len(batch)
                batch = []
            except ClientError as e:
                print(f"   [ERROR] Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-Schedule': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   [ERROR] Batch write error: {e}")

    return write_count


def main():
    """Main migration function."""
    print("\n" + "="*70)
    print("MongoDB to DynamoDB Migration (v2 - 3 Table Design)")
    print("="*70)
    print(f"\nSource: MongoDB '{MONGO_DB}'")
    print(f"Target: 3 DynamoDB tables in {DYNAMO_REGION}\n")

    # Initialize DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=DYNAMO_REGION)

    # Get all MongoDB collections
    print("Discovering MongoDB collections...")
    collections = get_mongo_collections()
    print(f"   Found {len(collections)} collections\n")

    # Track migration stats
    total_collections = 0
    total_records = 0
    skipped_collections = []

    # Migrate each collection
    for collection_name in sorted(collections):
        print(f"{'='*70}")
        print(f"Processing: {collection_name}")

        # Determine target table
        if collection_name in SEASON_TRENDS_COLLECTIONS:
            target_table = 'SeasonTrends'
        elif collection_name in TEAM_INFO_COLLECTIONS:
            target_table = 'TeamInfo'
        elif collection_name in SCHEDULE_COLLECTIONS:
            target_table = 'Schedule'
        else:
            print(f"   [WARNING]  Unknown collection, skipping")
            skipped_collections.append(collection_name)
            continue

        print(f"   Target: FantasyBaseball-{target_table}")

        # Read from MongoDB
        print(f"   Reading MongoDB data...")
        try:
            df = read_mongo_collection(collection_name)
            print(f"   Read {len(df)} records")
        except Exception as e:
            print(f"   [ERROR] Error reading collection: {e}")
            skipped_collections.append(collection_name)
            continue

        if df.empty:
            print(f"   [WARNING]  Empty collection, skipping")
            continue

        # Write to DynamoDB
        print(f"   Writing to DynamoDB...")
        try:
            if target_table == 'SeasonTrends':
                write_count = write_to_season_trends(collection_name, df, dynamodb)
            elif target_table == 'TeamInfo':
                write_count = write_to_team_info(collection_name, df, dynamodb)
            elif target_table == 'Schedule':
                write_count = write_to_schedule(collection_name, df, dynamodb)
            else:
                write_count = 0

            print(f"   [SUCCESS] Wrote {write_count} items")
            total_collections += 1
            total_records += write_count

        except Exception as e:
            print(f"   [ERROR] Error writing to DynamoDB: {e}")
            import traceback
            traceback.print_exc()
            skipped_collections.append(collection_name)

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY: Migration Summary")
    print("="*70)
    print(f"[SUCCESS] Successfully migrated: {total_collections} collections")
    print(f"Records: Total records written: {total_records:,}")

    if skipped_collections:
        print(f"\n[WARNING]  Skipped collections ({len(skipped_collections)}):")
        for coll in skipped_collections:
            print(f"   - {coll}")

    print(f"\n{'='*70}")
    print("COMPLETE: Migration Complete!")
    print("="*70)
    print("\nNEXT STEPS: Next steps:")
    print("   1. Verify data in DynamoDB console")
    print("   2. Test queries against SeasonTrends table")
    print("   3. Update scrapers to write to new tables")
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
