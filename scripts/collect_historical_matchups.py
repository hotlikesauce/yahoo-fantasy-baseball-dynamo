#!/usr/bin/env python3
"""
Pull historical H2H matchup data from Yahoo Fantasy API (2007-2022)
Store in DynamoDB HistoricalSeasons table as weekly_results
"""

import os
import json
import sys
import io
import boto3
import requests
import time
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_IDS_STR = os.getenv('YAHOO_LEAGUE_IDS')

# Parse league IDs from .env
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
    """Get fresh access token (OAuth)"""
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

def get_week_range(token, league_key):
    """Get valid week range from league metadata"""
    data = api_get(token, f"league/{league_key}/scoreboard;week=1")
    if not data:
        return None, None

    league_arr = data.get('fantasy_content', {}).get('league', [])
    if len(league_arr) < 1:
        return None, None

    league_meta = league_arr[0]
    start_week = int(league_meta.get('start_week', 1))
    end_week = int(league_meta.get('end_week', 21))

    # Limit to regular season (exclude playoffs week 22+)
    end_week = min(end_week, 21)

    return start_week, end_week

def extract_team_data(team_list):
    """
    Extract team info from team list structure
    team_list[0] = list of basic info dicts
    team_list[1] = dict with team_points
    """
    team_id = None
    team_name = None
    manager = None

    # Parse basic info (list of single-key dicts)
    basic_info = team_list[0]
    for item in basic_info:
        if not isinstance(item, dict):
            continue
        if 'team_id' in item:
            team_id = item['team_id']
        elif 'name' in item:
            team_name = item['name']
        elif 'managers' in item:
            mgrs = item['managers']
            if isinstance(mgrs, list) and len(mgrs) > 0:
                mgr_dict = mgrs[0].get('manager', {})
                manager = mgr_dict.get('nickname', 'Unknown')

    # Parse points
    stats_wrapper = team_list[1]
    team_points = stats_wrapper.get('team_points', {})
    points = int(team_points.get('total', 0))

    return {
        'id': team_id,
        'name': team_name,
        'manager': manager,
        'points': points
    }

def pull_matchups(token, year, league_key, week):
    """Pull all matchups for a specific week"""
    data = api_get(token, f"league/{league_key}/scoreboard;week={week}")
    if not data:
        return []

    try:
        fc = data.get('fantasy_content', {})
        league_arr = fc.get('league', [])

        if len(league_arr) < 2:
            return []

        scoreboard = league_arr[1].get('scoreboard', {})
        matchups_wrapper = scoreboard.get('0', {})
        matchups = matchups_wrapper.get('matchups', {})

        results = []

        # Iterate through matchups (keys: '0', '1', '2', ..., 'count')
        for matchup_idx in matchups.keys():
            if matchup_idx == 'count':
                continue

            matchup_wrapper = matchups[matchup_idx]
            matchup = matchup_wrapper.get('matchup', {})
            teams_wrapper = matchup.get('0', {})
            teams = teams_wrapper.get('teams', {})

            # Extract both teams
            if '0' not in teams or '1' not in teams:
                continue

            team_a_list = teams['0']['team']
            team_b_list = teams['1']['team']

            team_a = extract_team_data(team_a_list)
            team_b = extract_team_data(team_b_list)

            # Create bidirectional records
            results.append({
                'team_id': team_a['id'],
                'team_name': team_a['name'],
                'manager': team_a['manager'],
                'score': team_a['points'],
                'opponent': team_b['name'],
                'opponent_score': team_b['points']
            })

            results.append({
                'team_id': team_b['id'],
                'team_name': team_b['name'],
                'manager': team_b['manager'],
                'score': team_b['points'],
                'opponent': team_a['name'],
                'opponent_score': team_a['points']
            })

        return results

    except Exception as e:
        print(f"Error parsing matchups: {str(e)[:100]}")
        return []

def store_matchup(year, week, matchup_data):
    """Store matchup result in DynamoDB"""
    pk = f"{year}#{matchup_data['team_id']}"
    sk = f"weekly_results#{week:02d}"

    try:
        table.put_item(Item={
            'YearTeamNumber': pk,
            'DataTypeWeek': sk,
            'YearDataType': f"{year}#weekly_results",
            'Year': year,
            'Week': week,
            'TeamNumber': str(matchup_data['team_id']),
            'Manager': matchup_data['manager'],
            'Team': matchup_data['team_name'],
            'Opponent': matchup_data['opponent'],
            'Score': matchup_data['score'],
            'Opponent_Score': matchup_data['opponent_score']
        })
    except Exception as e:
        print(f"    ⚠️  Error storing {year}#W{week}: {str(e)[:50]}")

# Main execution
print("\n" + "=" * 80)
print("📊 PULLING HISTORICAL MATCHUP DATA (2007-2022)")
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
    # Only pull 2007-2022 (skip 2023-2025 which already have data)
    if year < 2007 or year > 2022:
        continue

    # Skip 2020 (COVID year)
    if year == 2020:
        print(f"{year}: ⏭️  SKIPPED (COVID year - non-standard format)\n")
        continue

    league_id = league_ids[year]
    game_key = GAME_KEYS.get(year)
    if not game_key:
        print(f"{year}: ❌ No game key\n")
        continue

    league_key = f"{game_key}.l.{league_id}"

    print(f"{year}: {league_key}")

    # Get week range
    start_week, end_week = get_week_range(token, league_key)
    if not start_week:
        print(f"  ❌ Could not get week range\n")
        continue

    print(f"  📅 Weeks {start_week}-{end_week}")

    total_records = 0

    for week in range(start_week, end_week + 1):
        print(f"    Week {week:2d}...", end=' ')

        matchups = pull_matchups(token, year, league_key, week)

        if not matchups:
            print("❌ No data")
            continue

        # Store all matchup records
        for matchup in matchups:
            store_matchup(year, week, matchup)

        total_records += len(matchups)
        print(f"✅ {len(matchups)} records")

        # Rate limiting
        time.sleep(0.5)

    print(f"  💾 Stored {total_records} total records for {year}\n")

print("=" * 80)
print("✅ Historical matchup data collection complete!")
print("=" * 80)
