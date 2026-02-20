#!/usr/bin/env python3
"""
Test Yahoo Fantasy API connection using direct HTTP requests
Pulls 2025 league data to verify credentials and see what's available
"""

import os
import json
import requests
from dotenv import load_dotenv
from xml.etree import ElementTree as ET

# Load environment variables
load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_ACCESS_TOKEN = os.getenv('YAHOO_ACCESS_TOKEN')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_ID = os.getenv('YAHOO_LEAGUE_ID')

# Extract league ID from URL if needed
if YAHOO_LEAGUE_ID.startswith('http'):
    league_id = YAHOO_LEAGUE_ID.split('/')[-2]
else:
    league_id = YAHOO_LEAGUE_ID

# Try different game keys based on year
# 2025: 458, 2024: 431, 2023: 422, etc.
# Try 2024 first since 2025 season might not be available yet
league_key_2024 = f"431.l.{league_id}"
league_key_2025 = f"458.l.{league_id}"

print(f"League ID: {league_id}")
print(f"Trying 2024 League Key: {league_key_2024}")
print(f"(Fallback 2025 League Key: {league_key_2025})")

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

def refresh_access_token():
    """Refresh the access token using refresh token"""
    print("🔄 Refreshing access token...")
    url = "https://api.login.yahoo.com/oauth2/get_token"

    data = {
        'client_id': YAHOO_CONSUMER_KEY,
        'client_secret': YAHOO_CONSUMER_SECRET,
        'refresh_token': YAHOO_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }

    response = requests.post(url, data=data)
    if response.status_code == 200:
        token_data = response.json()
        new_access_token = token_data.get('access_token')
        print(f"✅ Token refreshed successfully")
        return new_access_token
    else:
        print(f"❌ Failed to refresh token: {response.status_code}")
        print(response.text)
        return None

# Refresh token before making requests
access_token = refresh_access_token()
if not access_token:
    print("Cannot proceed without valid access token")
    exit(1)

# Setup headers with Bearer token
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json'
}

def api_get(endpoint):
    """Make API call and return JSON"""
    url = f"{BASE_URL}/{endpoint}?format=json"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error {response.status_code}: {response.text}")
        return None
    return response.json()

try:
    print("\n=== Testing Yahoo Fantasy API ===\n")

    # Test 1: Get league standings
    print("1️⃣  FETCHING LEAGUE STANDINGS (W-L-T RECORDS)...")
    standings_data = api_get(f"league/{league_key_2024}/standings")

    # Fallback to 2025 if 2024 doesn't work
    if not standings_data:
        print("   Trying 2025 instead...")
        league_key = league_key_2025
        standings_data = api_get(f"league/{league_key}/standings")
    else:
        league_key = league_key_2024

    if standings_data:
        league_info = standings_data.get('fantasy_content', {}).get('league', {})
        standings = league_info.get('standings', {}).get('0', {}).get('teams', {})

        print(f"\n📊 STANDINGS for League {league_id}:")
        print("-" * 80)

        for team_id, team_data in standings.items():
            if team_id == 'count':
                continue

            team_info = team_data.get('team', {})
            if isinstance(team_info, list):
                team_info = team_info[0]

            team_name = team_info.get('name', 'Unknown')
            manager = team_info.get('managers', {}).get('0', {}).get('manager', {}).get('nickname', 'Unknown')

            # Get standings info
            standings_info = team_info.get('team_standings', {}).get('0', {})
            wins = standings_info.get('wins', 0)
            losses = standings_info.get('losses', 0)
            ties = standings_info.get('ties', 0)

            print(f"  {team_name:40} ({manager:20}) - {wins:2d}-{losses:2d}-{ties:d}")

    # Test 2: Get current scoreboard (weekly matchups)
    print("\n" + "-" * 80)
    print("\n2️⃣  FETCHING CURRENT WEEK MATCHUPS...")
    scoreboard_data = api_get(f"league/{league_key}/scoreboard")

    if scoreboard_data:
        scoreboard = scoreboard_data.get('fantasy_content', {}).get('league', {}).get('scoreboard', {})
        week_num = scoreboard.get('week', 'current')
        print(f"\n📅 Week {week_num} Matchups:")
        print("-" * 80)

        matchups = scoreboard.get('0', {}).get('matchups', {})
        for matchup_id, matchup_data in matchups.items():
            if matchup_id == 'count':
                continue

            matchup = matchup_data.get('matchup', {})
            if isinstance(matchup, list):
                matchup = matchup[0]

            teams = matchup.get('teams', {})
            if len(teams) >= 2:
                team1 = teams.get('0', {}).get('team', {})
                team2 = teams.get('1', {}).get('team', {})

                if isinstance(team1, list):
                    team1 = team1[0]
                if isinstance(team2, list):
                    team2 = team2[0]

                name1 = team1.get('name', 'Team 1')
                name2 = team2.get('name', 'Team 2')

                print(f"  {name1:35} vs  {name2:35}")

    # Test 3: Get team stats for one team
    print("\n" + "-" * 80)
    print("\n3️⃣  FETCHING TEAM STATS (Sample)...")
    team_stats = api_get(f"league/{league_key}/teams/0")

    if team_stats:
        team = team_stats.get('fantasy_content', {}).get('team', {})
        if isinstance(team, list):
            team = team[0]

        team_name = team.get('name', 'Unknown')
        team_id = team.get('team_id', 'Unknown')
        manager = team.get('managers', {}).get('0', {}).get('manager', {}).get('nickname', 'Unknown')

        print(f"\n✅ Sample Team: {team_name} (ID: {team_id})")
        print(f"   Manager: {manager}")
        print(f"   Full response keys: {list(team.keys())}")

    # Test 4: Check stat categories
    print("\n" + "-" * 80)
    print("\n4️⃣  FETCHING STAT CATEGORIES...")
    settings = api_get(f"league/{league_key}/settings")

    if settings:
        league_settings = settings.get('fantasy_content', {}).get('league', {}).get('settings', {})
        if isinstance(league_settings, list):
            league_settings = league_settings[0]

        stat_cats = league_settings.get('stat_categories', {})
        print(f"\n📈 Available Stat Categories:")
        for cat_id, cat_data in stat_cats.items():
            if cat_id == 'count':
                continue
            cat = cat_data.get('stat', {})
            if isinstance(cat, list):
                cat = cat[0]
            cat_name = cat.get('name', 'Unknown')
            print(f"  - {cat_name}")

    print("\n" + "=" * 80)
    print("\n✅ API Connection Successful!")
    print("\nNEXT STEPS:")
    print("1. Verify team IDs and manager names above")
    print("2. Create mapping: Year → TeamID → Manager name")
    print("3. Build historical data collector for 2007-2024")
    print("4. Store W-L-T records in DynamoDB historical table")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
