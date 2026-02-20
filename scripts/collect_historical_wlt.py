#!/usr/bin/env python3
"""
Pull W-L-T records from Yahoo Fantasy API for all years (2007-2026)
Store in DynamoDB HistoricalSeasons table
"""

import os
import json
import sys
import io
import boto3
import requests
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_IDS_STR = os.getenv('YAHOO_LEAGUE_IDS')

# Parse league IDs
league_ids = {}
if YAHOO_LEAGUE_IDS_STR:
    for pair in YAHOO_LEAGUE_IDS_STR.split(','):
        year, lid = pair.split(':')
        league_ids[int(year)] = lid

# Game keys for each year
GAME_KEYS = {
    2007: 171, 2008: 195, 2009: 215, 2010: 238, 2011: 253, 2012: 268,
    2013: 308, 2014: 328, 2015: 346, 2016: 357, 2017: 370, 2018: 378,
    2019: 388, 2020: 398, 2021: 404, 2022: 412, 2023: 422, 2024: 431,
    2025: 458, 2026: 469
}

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

# DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

def get_access_token():
    """Get fresh access token"""
    url = "https://api.login.yahoo.com/oauth2/get_token"
    data = {
        'client_id': YAHOO_CONSUMER_KEY,
        'client_secret': YAHOO_CONSUMER_SECRET,
        'refresh_token': YAHOO_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=data)
    if response.status_code == 200:
        return response.json().get('access_token')
    return None

def api_get(token, endpoint):
    """Make API call"""
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }
    url = f"{BASE_URL}/{endpoint}?format=json"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    return None

def pull_standings(token, year, league_id):
    """Pull W-L-T records for a year"""
    game_key = GAME_KEYS.get(year)
    if not game_key:
        print(f"  ❌ No game key for {year}")
        return None

    league_key = f"{game_key}.l.{league_id}"
    print(f"  📡 Pulling standings for {year}...", end=' ')

    data = api_get(token, f"league/{league_key}/standings")
    if not data:
        print("❌ No data")
        return None

    try:
        fc = data.get('fantasy_content', {})
        league_arr = fc.get('league', [])

        if len(league_arr) < 2:
            print("❌ Invalid league response structure")
            return None

        # league[1] contains the standings
        standings_wrapper = league_arr[1]
        if 'standings' not in standings_wrapper:
            print("❌ No standings in response")
            return None

        standings_list = standings_wrapper.get('standings', [])
        if not standings_list:
            print("❌ Empty standings list")
            return None

        standings_data = standings_list[0]
        if 'teams' not in standings_data:
            print("❌ No teams in standings")
            return None

        teams = standings_data.get('teams', {})

        records = {}
        for team_idx, team_wrapper in teams.items():
            if team_idx == 'count':
                continue

            if 'team' not in team_wrapper:
                continue

            team_list = team_wrapper['team']
            if not isinstance(team_list, list) or len(team_list) < 3:
                continue

            # Element 0: basic team info
            team_basic = team_list[0]
            # Element 2: team standings
            team_standings_wrapper = team_list[2]

            # Extract team info from basic info list
            team_id = None
            team_name = None
            manager = None

            for item in team_basic:
                if not isinstance(item, dict):
                    continue
                if 'team_id' in item:
                    team_id = item['team_id']
                elif 'name' in item:
                    team_name = item['name']
                elif 'managers' in item:
                    managers = item['managers']
                    if isinstance(managers, list) and len(managers) > 0:
                        mgr_dict = managers[0].get('manager', {})
                        manager = mgr_dict.get('nickname', 'Unknown')

            # Extract W-L-T from team_standings
            team_standings = team_standings_wrapper.get('team_standings', {})
            outcome_totals = team_standings.get('outcome_totals', {})

            wins = int(outcome_totals.get('wins', 0))
            losses = int(outcome_totals.get('losses', 0))
            ties = int(outcome_totals.get('ties', 0))

            if team_id and team_name:
                records[str(team_id)] = {
                    'team_id': team_id,
                    'team_name': team_name,
                    'manager': manager or 'Unknown',
                    'wins': wins,
                    'losses': losses,
                    'ties': ties
                }

        print(f"✅ {len(records)} teams")
        return records

    except Exception as e:
        print(f"❌ Error: {str(e)[:100]}")
        import traceback
        traceback.print_exc()
        return None

def store_in_dynamodb(year, team_records):
    """Store W-L-T data in DynamoDB"""
    if not team_records:
        return

    for team_id, record in team_records.items():
        pk = f"{year}#{team_id}"
        sk = "final_standings#00"

        try:
            table.put_item(
                Item={
                    'YearTeamNumber': pk,
                    'DataTypeWeek': sk,
                    'Year': str(year),
                    'TeamNumber': int(team_id),
                    'TeamName': record['team_name'],
                    'Manager': record['manager'],
                    'Wins': record['wins'],
                    'Losses': record['losses'],
                    'Ties': record['ties'],
                    'WLT': f"{record['wins']}-{record['losses']}-{record['ties']}"
                }
            )
        except Exception as e:
            print(f"    ⚠️  Error storing {year}#{team_id}: {str(e)[:50]}")

# Main
print("\n" + "=" * 80)
print("📊 PULLING HISTORICAL W-L-T RECORDS (2007-2026)")
print("=" * 80)

if not league_ids:
    print("❌ No league IDs found in .env")
    sys.exit(1)

token = get_access_token()
if not token:
    print("❌ Could not get access token")
    sys.exit(1)

print(f"\n✅ Got access token\n")

for year in sorted(league_ids.keys()):
    # Only pull 2007-2022 (keep 2023-2025 unchanged since they're already good)
    if year > 2022:
        print(f"\n{year}: ⏭️  SKIPPED (2023-2025 already have good data)")
        continue

    # Skip 2020 (COVID, non-standard format)
    if year == 2020:
        print(f"\n{year}: ⏭️  SKIPPED (COVID year - non-standard format)")
        continue

    league_id = league_ids[year]
    print(f"\n{year}:")

    records = pull_standings(token, year, league_id)
    if records:
        store_in_dynamodb(year, records)
        print(f"  💾 Stored {len(records)} teams in DynamoDB")

print("\n" + "=" * 80)
print("✅ Historical W-L-T data collection complete!")
print("=" * 80)
