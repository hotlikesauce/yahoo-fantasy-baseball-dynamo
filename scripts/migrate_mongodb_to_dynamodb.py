"""
Migrate all data from MongoDB (YahooFantasyBaseball_2025) to DynamoDB tables.

This script reads all 24 MongoDB collections and writes them to the appropriate
DynamoDB tables using the storage abstraction layer.

Usage:
    python scripts/migrate_mongodb_to_dynamodb.py
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
MONGO_DB = 'YahooFantasyBaseball_2025'  # Source database

# DynamoDB configuration
DYNAMO_REGION = 'us-west-2'


class CollectionMapper:
    """Maps MongoDB collections to DynamoDB tables."""

    # Collections that go to LiveData table (overwrite pattern)
    LIVE_DATA_COLLECTIONS = {
        'live_standings', 'playoff_status', 'power_ranks', 'normalized_ranks',
        'power_ranks_lite', 'team_dict', 'remaining_sos', 'weekly_luck_analysis',
        'Coefficient_Last_Four', 'Coefficient_Last_Two', 'Power_Ranks',
        'playoff_probabilities', 'playoff_probabilities_static',
        'seasons_best_long', 'seasons_best_regular'
    }

    # Collections that go to WeeklyTimeSeries table (week-by-week history)
    WEEKLY_TIMESERIES_COLLECTIONS = {
        'running_normalized_ranks', 'power_ranks_season_trend',
        'standings_season_trend', 'weekly_stats', 'coefficient',
        'Running_ELO', 'week_stats'
    }

    # Collections that go to MatchupResults table
    MATCHUP_COLLECTIONS = {'weekly_results'}

    # Collections that go to Schedule table
    SCHEDULE_COLLECTIONS = {'schedule'}

    @classmethod
    def get_table_for_collection(cls, collection_name: str) -> str:
        """Determine which DynamoDB table a collection should go to."""
        if collection_name in cls.LIVE_DATA_COLLECTIONS:
            return 'FantasyBaseball-LiveData'
        elif collection_name in cls.WEEKLY_TIMESERIES_COLLECTIONS:
            return 'FantasyBaseball-WeeklyTimeSeries'
        elif collection_name in cls.MATCHUP_COLLECTIONS:
            return 'FantasyBaseball-MatchupResults'
        elif collection_name in cls.SCHEDULE_COLLECTIONS:
            return 'FantasyBaseball-Schedule'
        else:
            return None


def convert_floats_to_decimal(obj):
    """Recursively convert floats to Decimal for DynamoDB compatibility."""
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


def write_to_live_data_table(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to LiveData table."""
    if df.empty:
        print(f"   ⚠️  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-LiveData')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber field
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    if not team_field:
        print(f"   ⚠️  No team number field found, writing as global entry")
        team_field = None

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        item = {
            'DataType': collection_name,
            'TeamNumber': str(row[team_field]) if team_field else 'GLOBAL',
        }

        # Add all other fields
        for col in df.columns:
            if col != team_field and col != '_id':
                value = row[col]
                if pd.notna(value):
                    item[col] = convert_floats_to_decimal(value)

        batch.append({'PutRequest': {'Item': item}})

        # Write in batches of 25
        if len(batch) >= 25:
            try:
                dynamodb.batch_write_item(RequestItems={'FantasyBaseball-LiveData': batch})
                write_count += len(batch)
                batch = []
            except ClientError as e:
                print(f"   ❌ Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-LiveData': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   ❌ Batch write error: {e}")

    return write_count


def write_to_weekly_timeseries_table(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to WeeklyTimeSeries table."""
    if df.empty:
        print(f"   ⚠️  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-WeeklyTimeSeries')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber and Week fields
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    week_field = None
    for field in ['Week', 'week']:
        if field in df.columns:
            week_field = field
            break

    if not team_field or not week_field:
        print(f"   ⚠️  Missing TeamNumber or Week field, skipping")
        return 0

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        week_num = int(row[week_field]) if pd.notna(row[week_field]) else 0

        item = {
            'TeamNumber': str(row[team_field]),
            'Week#DataType': f"{week_num}#{collection_name}",
            'Week': week_num,
            'DataType': collection_name,
        }

        # Add all other fields
        for col in df.columns:
            if col not in [team_field, week_field, '_id']:
                value = row[col]
                if pd.notna(value):
                    item[col] = convert_floats_to_decimal(value)

        batch.append({'PutRequest': {'Item': item}})

        # Write in batches of 25
        if len(batch) >= 25:
            try:
                dynamodb.batch_write_item(RequestItems={'FantasyBaseball-WeeklyTimeSeries': batch})
                write_count += len(batch)
                batch = []
            except ClientError as e:
                print(f"   ❌ Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-WeeklyTimeSeries': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   ❌ Batch write error: {e}")

    return write_count


def write_to_matchup_results_table(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to MatchupResults table."""
    if df.empty:
        print(f"   ⚠️  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-MatchupResults')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber and Week fields
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    week_field = None
    for field in ['Week', 'week']:
        if field in df.columns:
            week_field = field
            break

    if not team_field or not week_field:
        print(f"   ⚠️  Missing TeamNumber or Week field, skipping")
        return 0

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        week_num = int(row[week_field]) if pd.notna(row[week_field]) else 0

        item = {
            'Week': week_num,
            'TeamNumber': str(row[team_field]),
        }

        # Add all other fields
        for col in df.columns:
            if col not in [team_field, week_field, '_id']:
                value = row[col]
                if pd.notna(value):
                    item[col] = convert_floats_to_decimal(value)

        batch.append({'PutRequest': {'Item': item}})

        # Write in batches of 25
        if len(batch) >= 25:
            try:
                dynamodb.batch_write_item(RequestItems={'FantasyBaseball-MatchupResults': batch})
                write_count += len(batch)
                batch = []
            except ClientError as e:
                print(f"   ❌ Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-MatchupResults': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   ❌ Batch write error: {e}")

    return write_count


def write_to_schedule_table(collection_name: str, df: pd.DataFrame, dynamodb):
    """Write data to Schedule table."""
    if df.empty:
        print(f"   ⚠️  Empty collection, skipping")
        return 0

    table = dynamodb.Table('FantasyBaseball-Schedule')

    # Remove MongoDB _id field
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)

    # Determine TeamNumber and Week fields
    team_field = None
    for field in ['Team_Number', 'TeamNumber', 'team_number']:
        if field in df.columns:
            team_field = field
            break

    week_field = None
    for field in ['Week', 'week']:
        if field in df.columns:
            week_field = field
            break

    if not team_field or not week_field:
        print(f"   ⚠️  Missing TeamNumber or Week field, skipping")
        return 0

    write_count = 0
    batch = []

    for idx, row in df.iterrows():
        week_num = int(row[week_field]) if pd.notna(row[week_field]) else 0

        item = {
            'Week': week_num,
            'TeamNumber': str(row[team_field]),
        }

        # Add all other fields
        for col in df.columns:
            if col not in [team_field, week_field, '_id']:
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
                print(f"   ❌ Batch write error: {e}")

    # Write remaining items
    if batch:
        try:
            dynamodb.batch_write_item(RequestItems={'FantasyBaseball-Schedule': batch})
            write_count += len(batch)
        except ClientError as e:
            print(f"   ❌ Batch write error: {e}")

    return write_count


def main():
    """Main migration function."""
    print("\n" + "="*70)
    print("📦 MongoDB → DynamoDB Migration")
    print("="*70)
    print(f"\nSource: MongoDB '{MONGO_DB}'")
    print(f"Target: DynamoDB tables in {DYNAMO_REGION}\n")

    # Initialize DynamoDB
    dynamodb = boto3.resource('dynamodb', region_name=DYNAMO_REGION)

    # Get all MongoDB collections
    print("📋 Discovering MongoDB collections...")
    collections = get_mongo_collections()
    print(f"   Found {len(collections)} collections\n")

    # Track migration stats
    total_collections = 0
    total_records = 0
    skipped_collections = []

    # Migrate each collection
    for collection_name in sorted(collections):
        print(f"{'='*70}")
        print(f"📝 Processing: {collection_name}")

        # Determine target table
        target_table = CollectionMapper.get_table_for_collection(collection_name)

        if not target_table:
            print(f"   ⚠️  Unknown collection type, skipping")
            skipped_collections.append(collection_name)
            continue

        print(f"   Target: {target_table}")

        # Read from MongoDB
        print(f"   Reading MongoDB data...")
        try:
            df = read_mongo_collection(collection_name)
            print(f"   Read {len(df)} records")
        except Exception as e:
            print(f"   ❌ Error reading collection: {e}")
            skipped_collections.append(collection_name)
            continue

        if df.empty:
            print(f"   ⚠️  Empty collection, skipping")
            continue

        # Write to DynamoDB
        print(f"   Writing to DynamoDB...")
        try:
            if 'LiveData' in target_table:
                write_count = write_to_live_data_table(collection_name, df, dynamodb)
            elif 'WeeklyTimeSeries' in target_table:
                write_count = write_to_weekly_timeseries_table(collection_name, df, dynamodb)
            elif 'MatchupResults' in target_table:
                write_count = write_to_matchup_results_table(collection_name, df, dynamodb)
            elif 'Schedule' in target_table:
                write_count = write_to_schedule_table(collection_name, df, dynamodb)
            else:
                write_count = 0

            print(f"   ✅ Wrote {write_count} items to {target_table}")
            total_collections += 1
            total_records += write_count

        except Exception as e:
            print(f"   ❌ Error writing to DynamoDB: {e}")
            skipped_collections.append(collection_name)

    # Summary
    print(f"\n{'='*70}")
    print("📊 Migration Summary")
    print("="*70)
    print(f"✅ Successfully migrated: {total_collections} collections")
    print(f"📝 Total records written: {total_records:,}")

    if skipped_collections:
        print(f"\n⚠️  Skipped collections ({len(skipped_collections)}):")
        for coll in skipped_collections:
            print(f"   - {coll}")

    print(f"\n{'='*70}")
    print("🎉 Migration Complete!")
    print("="*70)
    print("\n💡 Next steps:")
    print("   1. Verify data in DynamoDB tables")
    print("   2. Test frontend with DynamoDB data")
    print("   3. Update scrapers to write to DynamoDB")
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
