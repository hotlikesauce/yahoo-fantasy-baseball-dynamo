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
    'Cache-Control':                'max-age=300',
}

SCALE      = 242.0
DECAY      = 0.995  # gentle enough that AR #200-400 still has real value
ADP_DECAY  = 0.97   # steeper curve so elite ADP ranks (top 10 vs top 20) spread further apart
ADP_WEIGHT = 0.45   # ADP floor — reduced so current AR rank can dominate for active players


def rank_to_value(rank) -> float:
    if rank is None:
        return 0.0
    return SCALE * (DECAY ** (rank - 1))


def adp_to_value(adp) -> float:
    if adp is None:
        return 0.0
    return SCALE * (ADP_DECAY ** (adp - 1))


def blended_value(rank, adp) -> float:
    ar_val  = rank_to_value(rank)
    adp_val = adp_to_value(adp)
    raw = max(ar_val, adp_val * ADP_WEIGHT)
    if raw == 0.0:
        return 0.0
    return round(max(raw, 5.0), 1)


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
                    'name':     p['name'],
                    'pos':      p.get('pos', 'Util'),
                    'eligible': p.get('eligible', []),
                    'rank':     p.get('rank'),
                    'adp':      p.get('adp'),
                    'value':    blended_value(p.get('rank'), p.get('adp')),
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
