"""
Migrate historical season data from MongoDB to DynamoDB HistoricalSeasons table.

Handles schema differences across years:
- 2022: "Team Name" field, "NW_Stats" instead of "HRA_Stats", no weekly_results/weekly_stats
- 2023: "Team Name" in some collections, "Player_Name" field
- 2024: "Team" field, most complete data

Table: FantasyBaseball-HistoricalSeasons
  PK: YearTeamNumber = "{year}#{team_number}"
  SK: DataTypeWeek = "{data_type}#{week:02d}"
  GSI: YearDataTypeIndex - PK=YearDataType, SK=Week

Collections migrated per year (when available):
  - power_ranks_season_trend (season line charts)
  - weekly_stats (weekly category breakdowns)
  - weekly_results (H2H matchup outcomes - critical for all-time records)
  - standings_season_trend / season_standings_season_trend (standings over time)
  - coefficient (matchup expected wins)
  - schedule (who played who)
  - running_normalized_ranks (normalized scoring)
  - Running_ELO (ELO ratings)
  - team_dict (team name <-> number mapping, stored as week 0)
  - live_standings (final standings, stored as week 0)
  - power_ranks / Power_Ranks (final power ranks, stored as week 0)
  - normalized_ranks (final normalized ranks, stored as week 0)

Usage:
  python scripts/migrate_mongo_to_dynamo.py [year]
  python scripts/migrate_mongo_to_dynamo.py         # migrate all (2022, 2023, 2024)
  python scripts/migrate_mongo_to_dynamo.py 2024    # migrate just 2024
"""

import sys, io, os, math
from decimal import Decimal, InvalidOperation
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from pymongo import MongoClient
import certifi
import boto3

# ============================================================
# Config
# ============================================================
MONGO_URI = os.environ.get('MONGO_CLIENT',
    "mongodb+srv://admin:Aggies_1435@cluster0.qj2j8.mongodb.net/?appName=Cluster0")

DYNAMO_TABLE = 'FantasyBaseball-HistoricalSeasons'
DYNAMO_REGION = 'us-west-2'

YEAR_DB_MAP = {
    2022: 'YahooFantasyBaseball',
    2023: 'YahooFantasyBaseball_2023',
    2024: 'YahooFantasyBaseball_2024',
}

# Collections to migrate - grouped by type
# "weekly" = has Week field, one row per team per week
# "snapshot" = end-of-season snapshot, stored as week=0
WEEKLY_COLLECTIONS = [
    'power_ranks_season_trend',
    'weekly_stats',
    'weekly_results',
    'standings_season_trend',
    'season_standings_season_trend',  # 2022 name for standings_season_trend
    'coefficient',
    'schedule',
    'running_normalized_ranks',
    'Running_ELO',
    'power_ranks_lite',
    'Coefficient_Last_Two',
    'Coefficient_Last_Four',
    'week_stats',
]

SNAPSHOT_COLLECTIONS = [
    'team_dict',
    'live_standings',
    'power_ranks',
    'Power_Ranks',
    'normalized_ranks',
    'remaining_sos',
]


def safe_decimal(val):
    """Convert a value to Decimal safely for DynamoDB."""
    if val is None or val == '' or val == '-':
        return val
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return None
        return Decimal(str(val))
    if isinstance(val, str):
        try:
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return Decimal(val)
        except (ValueError, InvalidOperation):
            return val
    return val


def convert_item(item):
    """Convert a MongoDB document to DynamoDB-safe format."""
    result = {}
    for k, v in item.items():
        if k == '_id':
            continue
        if isinstance(v, dict):
            result[k] = {dk: safe_decimal(dv) for dk, dv in v.items()}
        elif isinstance(v, list):
            result[k] = [safe_decimal(lv) for lv in v]
        else:
            converted = safe_decimal(v)
            if converted is not None:
                result[k] = converted
    return result


def normalize_team_field(doc):
    """Normalize 'Team Name' -> 'Team' for consistency."""
    if 'Team Name' in doc and 'Team' not in doc:
        doc['Team'] = doc.pop('Team Name')
    return doc


def get_team_number(doc, team_dict=None):
    """Extract team number from document, handling various field names."""
    # Direct Team_Number or TeamNumber
    for field in ['Team_Number', 'TeamNumber']:
        if field in doc:
            val = doc[field]
            try:
                return str(int(float(str(val))))
            except (ValueError, TypeError):
                pass

    # Fallback: look up from team_dict by team name
    if team_dict:
        team_name = doc.get('Team', doc.get('Team Name', ''))
        if team_name in team_dict:
            return str(team_dict[team_name])

    # Last resort: 'index' field (used in 2022 data where no Team_Number exists)
    if 'index' in doc:
        try:
            return str(int(float(str(doc['index']))))
        except (ValueError, TypeError):
            pass

    return None


def get_week(doc):
    """Extract week number from document."""
    week = doc.get('Week', doc.get('week', 0))
    try:
        return int(float(str(week)))
    except (ValueError, TypeError):
        return 0


def batch_write_dynamo(table, items):
    """Write items to DynamoDB in batches of 25, deduplicating by PK+SK."""
    # Deduplicate: last item wins for same PK+SK
    seen = {}
    for item in items:
        key = (item['YearTeamNumber'], item['DataTypeWeek'])
        seen[key] = item
    items = list(seen.values())
    if len(items) < len(seen) + (len(items) - len(seen)):
        pass  # just use deduplicated list

    total = len(items)
    written = 0
    for i in range(0, total, 25):
        batch = items[i:i+25]
        with table.batch_writer() as writer:
            for item in batch:
                writer.put_item(Item=item)
        written += len(batch)
    return written


def build_team_dict(db):
    """Build team name -> team number mapping from team_dict collection."""
    mapping = {}
    coll = db.get_collection('team_dict')
    for doc in coll.find():
        team = doc.get('Team', doc.get('Team Name', ''))
        num = doc.get('Team_Number', doc.get('TeamNumber'))
        if team and num is not None:
            mapping[team] = int(float(str(num)))
    return mapping


def migrate_year(year, mongo_client, dynamo_table):
    """Migrate all data for a single year from MongoDB to DynamoDB."""
    db_name = YEAR_DB_MAP[year]
    db = mongo_client[db_name]
    collections = db.list_collection_names()

    print(f"\n{'='*60}")
    print(f"MIGRATING {year} from {db_name}")
    print(f"Collections available: {sorted(collections)}")
    print(f"{'='*60}")

    # Build team_dict for name -> number lookups
    team_dict = build_team_dict(db)
    print(f"Team dict: {len(team_dict)} teams mapped")

    # Enrich team_dict from collections that have both Team name and Team_Number
    # This catches name changes mid-season and collections with different naming
    enrich_sources = ['weekly_results', 'power_ranks_season_trend',
                      'live_standings', 'running_normalized_ranks', 'Coefficient_Last_Two',
                      'Coefficient_Last_Four']
    for coll_name in enrich_sources:
        if coll_name in collections:
            for doc in db[coll_name].find():
                doc_copy = normalize_team_field(dict(doc))
                team = doc_copy.get('Team', '')
                # Try Team_Number first, then index (2022 fallback)
                num = doc_copy.get('Team_Number', doc_copy.get('TeamNumber'))
                if num is None and 'index' in doc_copy:
                    num = doc_copy['index']
                if team and num is not None and team not in team_dict:
                    try:
                        team_dict[team] = int(float(str(num)))
                    except (ValueError, TypeError):
                        pass
    if team_dict:
        print(f"Team dict (enriched): {len(team_dict)} teams mapped")

    total_items = 0

    # ---- Migrate weekly collections ----
    for coll_name in WEEKLY_COLLECTIONS:
        if coll_name not in collections:
            continue

        coll = db[coll_name]
        count = coll.count_documents({})
        if count == 0:
            print(f"  {coll_name}: empty, skipping")
            continue

        # Normalize collection name for storage
        # 2022 uses 'season_standings_season_trend' -> store as 'standings_season_trend'
        storage_name = coll_name
        if coll_name == 'season_standings_season_trend':
            storage_name = 'standings_season_trend'

        print(f"  {coll_name}: {count} docs -> {storage_name}...", end=' ')

        items = []
        skipped = 0
        for doc in coll.find():
            doc = normalize_team_field(doc)
            doc = convert_item(doc)

            week = get_week(doc)

            # Special handling for schedule - use Team_Number directly
            if coll_name == 'schedule':
                tn = doc.get('Team_Number')
                if tn is not None:
                    tn = str(int(float(str(tn))))
                else:
                    skipped += 1
                    continue
            else:
                tn = get_team_number(doc, team_dict)

            if tn is None:
                skipped += 1
                continue

            # Build DynamoDB item
            item = dict(doc)
            item['YearTeamNumber'] = f"{year}#{tn}"
            item['DataTypeWeek'] = f"{storage_name}#{week:02d}"
            item['YearDataType'] = f"{year}#{storage_name}"
            item['Week'] = week
            item['Year'] = year
            item['TeamNumber'] = tn

            # Clean up redundant/problematic fields
            for field in ['_id', 'index', 'Unnamed: 0', 'Unnamed: 0.1']:
                item.pop(field, None)

            items.append(item)

        if items:
            written = batch_write_dynamo(dynamo_table, items)
            total_items += written
            msg = f"{written} items written"
            if skipped:
                msg += f" ({skipped} skipped - no team number)"
            print(msg)
        else:
            print(f"no valid items (skipped {skipped})")

    # ---- Migrate snapshot collections (stored as week=0) ----
    for coll_name in SNAPSHOT_COLLECTIONS:
        if coll_name not in collections:
            continue

        coll = db[coll_name]
        count = coll.count_documents({})
        if count == 0:
            print(f"  {coll_name}: empty, skipping")
            continue

        print(f"  {coll_name}: {count} docs (snapshot)...", end=' ')

        items = []
        skipped = 0
        for doc in coll.find():
            doc = normalize_team_field(doc)
            doc = convert_item(doc)

            tn = get_team_number(doc, team_dict)
            if tn is None:
                skipped += 1
                continue

            item = dict(doc)
            item['YearTeamNumber'] = f"{year}#{tn}"
            item['DataTypeWeek'] = f"{coll_name}#00"
            item['YearDataType'] = f"{year}#{coll_name}"
            item['Week'] = 0
            item['Year'] = year
            item['TeamNumber'] = tn

            for field in ['_id', 'index', 'Unnamed: 0', 'Unnamed: 0.1']:
                item.pop(field, None)

            items.append(item)

        if items:
            written = batch_write_dynamo(dynamo_table, items)
            total_items += written
            msg = f"{written} items written"
            if skipped:
                msg += f" ({skipped} skipped)"
            print(msg)
        else:
            print(f"no valid items (skipped {skipped})")

    print(f"\n  TOTAL for {year}: {total_items} items written to DynamoDB")
    return total_items


def main():
    years_to_migrate = list(YEAR_DB_MAP.keys())

    if len(sys.argv) > 1:
        try:
            year = int(sys.argv[1])
            if year not in YEAR_DB_MAP:
                print(f"Unknown year {year}. Available: {list(YEAR_DB_MAP.keys())}")
                sys.exit(1)
            years_to_migrate = [year]
        except ValueError:
            print(f"Invalid year: {sys.argv[1]}")
            sys.exit(1)

    # Connect to MongoDB
    print("Connecting to MongoDB...")
    ca = certifi.where()
    mongo_client = MongoClient(MONGO_URI, tlsCAFile=ca)

    # Connect to DynamoDB
    print("Connecting to DynamoDB...")
    dynamodb = boto3.resource('dynamodb', region_name=DYNAMO_REGION)
    table = dynamodb.Table(DYNAMO_TABLE)

    grand_total = 0
    for year in sorted(years_to_migrate):
        total = migrate_year(year, mongo_client, table)
        grand_total += total

    mongo_client.close()
    print(f"\n{'='*60}")
    print(f"MIGRATION COMPLETE: {grand_total} total items written")
    print(f"Years migrated: {years_to_migrate}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
