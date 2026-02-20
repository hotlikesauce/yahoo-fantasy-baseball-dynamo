#!/usr/bin/env python3
"""
Find the Yahoo Fantasy Baseball game key for a given year
Game keys change each season
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')

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

def get_games(access_token):
    """Get available MLB games"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    url = "https://fantasysports.yahooapis.com/fantasy/v2/games;game_types=full;sport=mlb?format=json"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        games = response.json().get('fantasy_content', {}).get('games', {})
        print("\n📚 Available MLB Game Keys (Season):")
        print("=" * 60)

        for game_key, game_data in sorted(games.items(), reverse=True):
            if game_key == 'count':
                continue

            game = game_data.get('game', {})
            if isinstance(game, list):
                game = game[0]

            game_key = game.get('game_key')
            game_id = game.get('game_id')
            season = game.get('season')
            game_name = game.get('name')

            print(f"  {season}: game_key = {game_key} | {game_name}")

        return games
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

if __name__ == "__main__":
    print("🔄 Getting access token...")
    token = get_access_token()

    if token:
        print("✅ Token obtained\n")
        get_games(token)
        print("\n💡 Use the game_key value above in your league URLs")
        print("   Format: {game_key}.l.{league_id}")
    else:
        print("❌ Could not get access token")
