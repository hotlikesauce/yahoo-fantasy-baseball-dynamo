"""
Create FantasyBaseball-Matchups2026 table and populate with 2026 weekly matchup data.
Simple table: Week + TeamNumber -> Score, OpponentTeamNumber, OpponentScore.

Usage: python scripts/setup_matchups_2026.py
"""

import os
import sys
import io
import time
import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

REGION = 'us-west-2'
TABLE_NAME = 'FantasyBaseball-Matchups2026'
GAME_KEY = 469  # 2026 MLB
LEAGUE_ID = '8614'
LEAGUE_KEY = f'{GAME_KEY}.l.{LEAGUE_ID}'
BASE_URL = 'https://fantasysports.yahooapis.com/fantasy/v2'

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')

dynamodb = boto3.resource('dynamodb', region_name=REGION)


def get_access_token():
    url = 'https://api.login.yahoo.com/oauth2/get_token'
    data = {
        'client_id': YAHOO_CONSUMER_KEY,
        'client_secret': YAHOO_CONSUMER_SECRET,
        'refresh_token': YAHOO_REFRESH_TOKEN,
        'grant_type': 'refresh_token',
    }
    resp = requests.post(url, data=data)
    if resp.status_code == 200:
        return resp.json().get('access_token')
    print(f'Token error: {resp.status_code} {resp.text[:200]}')
    return None


def api_get(token, endpoint):
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    url = f'{BASE_URL}/{endpoint}?format=json'
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()
    print(f'API error: {resp.status_code}')
    return None


def create_table():
    try:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'Week',       'KeyType': 'HASH'},
                {'AttributeName': 'TeamNumber', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'Week',       'AttributeType': 'N'},
                {'AttributeName': 'TeamNumber', 'AttributeType': 'S'},
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        print(f'Creating {TABLE_NAME}...')
        table.wait_until_exists()
        print(f'Table created: {table.table_arn}')
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f'{TABLE_NAME} already exists, using existing table.')
            return dynamodb.Table(TABLE_NAME)
        raise


def pull_week(token, week):
    data = api_get(token, f'league/{LEAGUE_KEY}/scoreboard;week={week}')
    if not data:
        return []

    try:
        league_arr = data['fantasy_content']['league']
        if len(league_arr) < 2:
            return []

        scoreboard = league_arr[1].get('scoreboard', {})
        matchups = scoreboard.get('0', {}).get('matchups', {})
        results = []

        for idx, val in matchups.items():
            if idx == 'count':
                continue
            matchup = val.get('matchup', {})
            teams = matchup.get('0', {}).get('teams', {})
            if '0' not in teams or '1' not in teams:
                continue

            def parse_team(tlist):
                team_id = name = None
                for item in tlist[0]:
                    if isinstance(item, dict):
                        if 'team_id' in item:
                            team_id = str(item['team_id'])
                        elif 'name' in item:
                            name = item['name']
                pts = int(tlist[1].get('team_points', {}).get('total', 0))
                return team_id, name, pts

            a_id, a_name, a_pts = parse_team(teams['0']['team'])
            b_id, b_name, b_pts = parse_team(teams['1']['team'])

            if not a_id or not b_id:
                continue

            results.append({'team_id': a_id, 'team_name': a_name, 'score': a_pts,
                            'opp_id': b_id,  'opp_name': b_name,  'opp_score': b_pts})
            results.append({'team_id': b_id, 'team_name': b_name, 'score': b_pts,
                            'opp_id': a_id,  'opp_name': a_name,  'opp_score': a_pts})

        return results
    except Exception as e:
        print(f'  Parse error: {e}')
        return []


def get_current_week(token):
    data = api_get(token, f'league/{LEAGUE_KEY}/standings')
    if not data:
        return None
    try:
        return int(data['fantasy_content']['league'][0]['current_week'])
    except Exception:
        return None


def main():
    table = create_table()

    token = get_access_token()
    if not token:
        print('Could not get access token')
        sys.exit(1)
    print('Got access token')

    current_week = get_current_week(token)
    if not current_week:
        print('Could not determine current week')
        sys.exit(1)

    # Pull all completed weeks (up to current_week - 1)
    last_completed = current_week - 1
    print(f'Current week: {current_week}. Pulling weeks 1-{last_completed}.\n')

    with table.batch_writer() as batch:
        for week in range(1, last_completed + 1):
            print(f'Week {week}...', end=' ')
            results = pull_week(token, week)
            if not results:
                print('no data')
                continue
            for r in results:
                batch.put_item(Item={
                    'Week':               week,
                    'TeamNumber':         r['team_id'],
                    'Team':               r['team_name'],
                    'Score':              r['score'],
                    'OpponentTeamNumber': r['opp_id'],
                    'Opponent':           r['opp_name'],
                    'OpponentScore':      r['opp_score'],
                    'Year':               2026,
                })
            print(f'{len(results)} records')
            time.sleep(0.3)

    print(f'\nDone. Populated {TABLE_NAME} for weeks 1-{last_completed}.')


if __name__ == '__main__':
    main()
