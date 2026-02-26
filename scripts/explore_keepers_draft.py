#!/usr/bin/env python3
"""
Pull 2026 keeper data and draft order from Yahoo Fantasy API.
"""

import os
import json
import sys
import io
import requests
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_IDS_STR = os.getenv('YAHOO_LEAGUE_IDS')

league_ids = {}
if YAHOO_LEAGUE_IDS_STR:
    for pair in YAHOO_LEAGUE_IDS_STR.split(','):
        year, lid = pair.split(':')
        league_ids[int(year)] = lid

LEAGUE_ID_2026 = league_ids.get(2026)
LEAGUE_KEY = f"469.l.{LEAGUE_ID_2026}"
BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

def get_access_token():
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
    print(f"Token error: {response.status_code} {response.text}")
    return None

def api_get(token, endpoint):
    url = f"{BASE_URL}/{endpoint}?format=json"
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers, timeout=15)
    if response.status_code == 200:
        return response.json()
    print(f"API error {response.status_code}: {response.text[:300]}")
    return None

def extract_player_info(player_list):
    """Extract name, position, team, rank, keeper info from player array."""
    info = {}
    if isinstance(player_list, list):
        for item in player_list:
            if isinstance(item, dict):
                if 'name' in item:
                    info['name'] = item['name'].get('full', '')
                if 'display_position' in item:
                    info['position'] = item['display_position']
                if 'editorial_team_abbr' in item:
                    info['mlb_team'] = item['editorial_team_abbr']
                if 'is_keeper' in item:
                    info['is_keeper'] = item['is_keeper']
                if 'player_id' in item:
                    info['player_id'] = item['player_id']
    return info

def main():
    print(f"League Key: {LEAGUE_KEY}\n")
    token = get_access_token()
    if not token:
        print("Failed to get token")
        return

    # Pull players sorted by rank, scan for keepers
    # Yahoo returns max 25 per request, paginate through top 300
    print("=== SCANNING FOR KEEPERS (top 300 players by rank) ===")
    keepers = []
    keeper_by_team = {}

    for start in range(0, 300, 25):
        data = api_get(token, f"league/{LEAGUE_KEY}/players;sort=AR;start={start};count=25")
        if not data:
            break
        players_raw = data.get('fantasy_content', {}).get('league', [{}])
        if len(players_raw) < 2:
            break
        players = players_raw[1].get('players', {})

        found_any = False
        for key, val in players.items():
            if key == 'count':
                continue
            found_any = True
            player_data = val.get('player', [])
            if not player_data:
                continue
            info = extract_player_info(player_data[0] if isinstance(player_data[0], list) else player_data)
            keeper_info = info.get('is_keeper', {})
            rank = start + int(key) + 1

            if keeper_info.get('kept'):
                team_id = str(keeper_info.get('ik_tid', '?'))
                entry = {
                    'rank': rank,
                    'name': info.get('name', '?'),
                    'position': info.get('position', '?'),
                    'mlb_team': info.get('mlb_team', '?'),
                    'team_id': team_id,
                }
                keepers.append(entry)
                if team_id not in keeper_by_team:
                    keeper_by_team[team_id] = []
                keeper_by_team[team_id].append(entry)

        if not found_any:
            break

    print(f"\nFound {len(keepers)} keepers total:\n")
    for k in sorted(keepers, key=lambda x: x['rank']):
        print(f"  Rank #{k['rank']:3d} - {k['name']:<25} {k['position']:<5} {k['mlb_team']:<5} (Team ID: {k['team_id']})")

    # Get team names to map team_id -> team name
    print("\n=== TEAM NAMES ===")
    data = api_get(token, f"league/{LEAGUE_KEY}/teams")
    if data:
        teams_raw = data.get('fantasy_content', {}).get('league', [{}])
        if len(teams_raw) >= 2:
            teams = teams_raw[1].get('teams', {})
            team_names = {}
            for key, val in teams.items():
                if key == 'count':
                    continue
                team_data = val.get('team', [])
                if not team_data:
                    continue
                t_info = team_data[0] if isinstance(team_data[0], list) else team_data
                tid = None
                tname = None
                draft_pos = None
                for item in t_info:
                    if isinstance(item, dict):
                        if 'team_id' in item:
                            tid = str(item['team_id'])
                        if 'name' in item:
                            tname = item['name']
                        if 'draft_position' in item:
                            draft_pos = item['draft_position']
                if tid:
                    team_names[tid] = {'name': tname, 'draft_position': draft_pos}
                    print(f"  Team {tid}: {tname} (Draft pos: {draft_pos})")

            print("\n=== KEEPERS BY TEAM ===")
            for tid, players in sorted(keeper_by_team.items(), key=lambda x: int(x[0])):
                tname = team_names.get(tid, {}).get('name', f'Team {tid}')
                print(f"\n  {tname} (Team {tid}):")
                for p in players:
                    print(f"    #{p['rank']:3d} {p['name']:<25} {p['position']:<5} {p['mlb_team']}")

if __name__ == '__main__':
    main()
