"""
Lambda: Serve cached roster data from DynamoDB.
Returns full rosters for all 12 teams, sorted by value descending.
Reads from FantasyBaseball-RosterData populated daily by pull_roster_data.

Triggered by: Lambda Function URL (HTTPS)
"""

import json
import logging
from typing import Dict, Any

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table    = dynamodb.Table('FantasyBaseball-RosterData')

CORS_HEADERS = {
    'Content-Type':                 'application/json',
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control':                'max-age=3600',
}

SCALE = 242.0
DECAY = 0.98


def rank_to_value(rank) -> float:
    if rank is None:
        return 0.0
    return round(SCALE * (DECAY ** (rank - 1)), 1)


def lambda_handler(event, context) -> Dict[str, Any]:
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        resp  = table.query(KeyConditionExpression=Key('Year').eq(2026))
        items = resp.get('Items', [])

        rosters   = {}
        timestamp = None
        meta      = {}

        for item in items:
            sk = item.get('TeamName', '')
            if sk == '#meta':
                meta      = {k: str(v) for k, v in item.items() if k not in ('Year', 'TeamName')}
                timestamp = item.get('Timestamp')
                continue
            if sk.startswith('#'):
                continue

            players_raw = json.loads(item.get('Players', '[]'))
            players = [
                {
                    'name':  p['name'],
                    'pos':   p.get('pos', 'Util'),
                    'rank':  p.get('rank'),
                    'value': rank_to_value(p.get('rank')),
                }
                for p in players_raw
            ]
            players.sort(key=lambda p: -p['value'])
            rosters[sk] = players

        if not rosters:
            return {
                'statusCode': 200,
                'headers':    CORS_HEADERS,
                'body':       json.dumps({
                    'error':   'No roster data yet — pull_roster_data has not run.',
                    'rosters': {},
                }),
            }

        return {
            'statusCode': 200,
            'headers':    CORS_HEADERS,
            'body':       json.dumps({'rosters': rosters, 'meta': meta, 'timestamp': timestamp}),
        }

    except Exception as e:
        logger.error(f"serve_roster_data FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers':    CORS_HEADERS,
            'body':       json.dumps({'error': str(e)}),
        }
