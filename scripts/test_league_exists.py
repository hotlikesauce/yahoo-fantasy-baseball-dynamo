#!/usr/bin/env python3
"""
Test if a league exists in different years
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_ID = os.getenv('YAHOO_LEAGUE_ID')

league_id = YAHOO_LEAGUE_ID.split('/')[-2] if YAHOO_LEAGUE_ID.startswith('http') else YAHOO_LEAGUE_ID

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
headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

# Game keys for years 2025, 2024, 2023, 2022, 2021, 2020
game_keys = {
    2025: 458,
    2024: 431,
    2023: 422,
    2022: 412,
    2021: 404,
    2020: 398,
    2019: 388,
    2018: 378,
    2017: 370,
}

print(f"\nTesting league {league_id} across years...")
print("=" * 70)

for year in sorted(game_keys.keys(), reverse=True):
    game_key = game_keys[year]
    league_key = f"{game_key}.l.{league_id}"

    # Try simple info endpoint first
    url = f"{BASE_URL}/league/{league_key}?format=json"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        league = data.get('fantasy_content', {}).get('league', {})
        if isinstance(league, list):
            league = league[0]
        name = league.get('name', '?')
        print(f"✅ {year}: EXISTS - {name}")
    elif response.status_code == 404:
        print(f"❌ {year}: NOT FOUND")
    else:
        error = response.json().get('error', {}).get('description', 'Unknown')
        print(f"⚠️  {year}: Error - {error}")
