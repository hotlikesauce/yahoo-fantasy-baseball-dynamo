#!/usr/bin/env python3
"""
Test if we can access private/archived leagues via OAuth
Try 2015 with known league ID 70003
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

def get_token():
    url = "https://api.login.yahoo.com/oauth2/get_token"
    data = {
        'client_id': YAHOO_CONSUMER_KEY,
        'client_secret': YAHOO_CONSUMER_SECRET,
        'refresh_token': YAHOO_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    }
    response = requests.post(url, data=data)
    return response.json().get('access_token') if response.status_code == 200 else None

token = get_token()
if not token:
    print("❌ Could not get token")
    exit(1)

headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

# Test 2015 league (known ID: 70003, game key: 346)
year = 2015
game_key = 346
league_id = 70003
league_key = f"{game_key}.l.{league_id}"

print(f"\n🔍 Testing private league access")
print("=" * 70)
print(f"Year: {year}")
print(f"League ID: {league_id}")
print(f"League Key: {league_key}")
print("=" * 70)

# Try 1: Get league info
print(f"\n1️⃣  Getting league info...")
url = f"{BASE_URL}/league/{league_key}?format=json"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    league = data.get('fantasy_content', {}).get('league', {})
    if isinstance(league, list):
        league = league[0]
    print(f"✅ League accessible: {league.get('name', 'Unknown')}")
elif response.status_code == 401:
    print(f"❌ Unauthorized - Token expired or invalid")
elif response.status_code == 403:
    print(f"❌ Forbidden - Not allowed to view this league (privacy settings)")
elif response.status_code == 404:
    print(f"❌ Not Found - League doesn't exist or wrong ID")
else:
    error = response.json().get('error', {}).get('description', f'Status {response.status_code}')
    print(f"⚠️  Error: {error}")

# Try 2: Get standings
print(f"\n2️⃣  Getting standings...")
url = f"{BASE_URL}/league/{league_key}/standings?format=json"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    league = data.get('fantasy_content', {}).get('league', {})
    if isinstance(league, list):
        league = league[0]
    standings_data = league.get('standings', {})
    if isinstance(standings_data, list):
        standings_data = standings_data[0]
    standings = standings_data.get('0', {}).get('teams', {})

    print(f"✅ Standings accessible - {len([t for t in standings if t != 'count'])} teams found:")
    for team_id, team_data in standings.items():
        if team_id == 'count':
            continue
        team_info = team_data.get('team', {})
        if isinstance(team_info, list):
            team_info = team_info[0]

        team_name = team_info.get('name', 'Unknown')
        manager = team_info.get('managers', {}).get('0', {}).get('manager', {}).get('nickname', 'Unknown')
        standings_info = team_info.get('team_standings', {}).get('0', {})
        wins = standings_info.get('wins', 0)
        losses = standings_info.get('losses', 0)
        ties = standings_info.get('ties', 0)

        print(f"  {team_name:35} ({manager:20}) - {wins}-{losses}-{ties}")

elif response.status_code == 403:
    print(f"❌ Forbidden - Cannot access private league data")
else:
    error = response.json().get('error', {}).get('description', f'Status {response.status_code}')
    print(f"⚠️  Error: {error}")

print("\n" + "=" * 70)
if response.status_code == 200:
    print("✅ PRIVATE LEAGUES ARE ACCESSIBLE VIA OAUTH!")
    print("   You can provide all league IDs and we'll pull the data.")
else:
    print("❌ Cannot access private leagues via this OAuth token")
    print("   They may need to be made public or resync credentials")
