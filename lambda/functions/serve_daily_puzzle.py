"""
Lambda: Serve today's daily Stat Line Guesser puzzle.
Returns stat line + hints. Answer (player + fantasy owner) included for client-side reveal.
Triggered by: Lambda Function URL (HTTPS)
"""

import json
import logging
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb   = boto3.resource('dynamodb', region_name='us-west-2')
puzzle_tbl = dynamodb.Table('FantasyBaseball-DailyPuzzle')

CORS_HEADERS = {
    'Content-Type':                 'application/json',
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control':                'max-age=3600',
}

MST = timezone(timedelta(hours=-7))


def today_mst() -> str:
    return datetime.now(MST).strftime('%Y-%m-%d')


def lambda_handler(event, context):
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        date_str = today_mst()
        item     = puzzle_tbl.get_item(Key={'Date': date_str}).get('Item')

        # Fall back to yesterday if today's puzzle isn't ready yet
        if not item:
            yesterday = (datetime.now(MST) - timedelta(days=1)).strftime('%Y-%m-%d')
            item = puzzle_tbl.get_item(Key={'Date': yesterday}).get('Item')

        if not item:
            return {
                'statusCode': 200,
                'headers':    CORS_HEADERS,
                'body':       json.dumps({'available': False}),
            }

        rank      = item.get('Rank', '')
        rank_hint = f"#{rank}" if rank else "Outside top 1000"

        puzzle = {
            'available':  True,
            'date':       item['Date'],
            'statLine':   item['StatLine'],
            'isPitcher':  item.get('IsPitcher', False),
            'managers':   json.loads(item.get('Managers', '[]')),
            'hints': {
                '2': f"Position: {item['Position']}",
                '4': f"AR Rank: {rank_hint}",
                '6': f"First name: {item['PlayerName'].split()[0]}",
            },
            'answer': {
                'playerName':  item['PlayerName'],
                'mlbTeam':     item.get('MlbTeam', ''),
                'fantasyTeam': item.get('FantasyTeam', ''),
                'position':    item['Position'],
                'rank':        rank,
            },
        }

        return {
            'statusCode': 200,
            'headers':    CORS_HEADERS,
            'body':       json.dumps(puzzle),
        }

    except Exception as e:
        logger.error(f"serve_daily_puzzle FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers':    CORS_HEADERS,
            'body':       json.dumps({'error': str(e)}),
        }
