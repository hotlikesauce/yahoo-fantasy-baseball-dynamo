#!/usr/bin/env python3
"""
Find all leagues the user has been in across all years
This pulls league history from Yahoo Fantasy API
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

print("\n🔍 Searching for all baseball leagues you've been in...\n")
print("=" * 80)

# Game keys for all years
game_keys = {
    2026: 469,
    2025: 458,
    2024: 431,
    2023: 422,
    2022: 412,
    2021: 404,
    2020: 398,
    2019: 388,
    2018: 378,
    2017: 370,
    2016: 357,
    2015: 346,
    2014: 328,
    2013: 308,
    2012: 268,
    2011: 253,
    2010: 238,
    2009: 215,
    2008: 195,
    2007: 171,
}

leagues_found = {}

for year in sorted(game_keys.keys(), reverse=True):
    game_key = game_keys[year]

    # Try to get user's leagues for this year
    url = f"{BASE_URL}/users;use_login=1/games;game_types=full;game_ids={game_key}/leagues?format=json"

    try:
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()
            users = data.get('fantasy_content', {}).get('users', {})

            if users and '0' in users:
                user = users['0'].get('user', {})
                if isinstance(user, list):
                    user = user[0]

                games = user.get('games', {})

                if games and '0' in games:
                    game = games['0'].get('game', {})
                    if isinstance(game, list):
                        game = game[0]

                    leagues = game.get('leagues', {})

                    if leagues:
                        for league_key, league_data in leagues.items():
                            if league_key == 'count':
                                continue

                            league = league_data.get('league', {})
                            if isinstance(league, list):
                                league = league[0]

                            league_id = league.get('league_id')
                            league_name = league.get('name', 'Unknown')
                            league_type = league.get('league_type', '?')

                            if league_id:
                                leagues_found[year] = {
                                    'id': league_id,
                                    'name': league_name,
                                    'type': league_type
                                }
                                print(f"✅ {year}: League ID {league_id} - {league_name}")

        elif response.status_code == 401:
            # Not in league that year
            pass
        elif response.status_code == 404:
            pass
        else:
            error = response.json().get('error', {}).get('description', f'Status {response.status_code}')

    except Exception as e:
        print(f"⚠️  {year}: Error - {str(e)[:50]}")

print("\n" + "=" * 80)
print("\n📊 SUMMARY OF LEAGUES FOUND:")
print("=" * 80)

if leagues_found:
    for year in sorted(leagues_found.keys(), reverse=True):
        league = leagues_found[year]
        print(f"{year}: League ID {league['id']:5} - {league['name']:40}")

    print("\n" + "=" * 80)
    print("\n✅ MAPPING TABLE (for .env or config):")
    print("=" * 80)
    print("\nYEAR_TO_LEAGUE_ID = {")
    for year in sorted(leagues_found.keys()):
        print(f"    {year}: '{leagues_found[year]['id']}',")
    print("}")
else:
    print("\n❌ No leagues found across all years")
    print("   This could mean:")
    print("   - Your account wasn't active in those years")
    print("   - The API doesn't have access to that data")
    print("   - You may need to check Yahoo's website manually")
