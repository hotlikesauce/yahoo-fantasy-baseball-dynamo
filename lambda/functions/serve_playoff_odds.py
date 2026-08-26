"""
Read side of the playoff race: hand the browser the current picture plus every
snapshot on file, as one JSON document.

Behind a Lambda Function URL with CORS open, called straight from
docs/live_standings_2026.html. Pure DynamoDB read - it never touches Yahoo, so
it stays fast and cannot be rate-limited by them.
"""

import os
import json
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

TABLE = os.environ.get('ODDS_TABLE', 'FantasyBaseball-PlayoffOdds')
REGION = os.environ.get('AWS_REGION', 'us-west-2')
YEAR = os.environ.get('SEASON_YEAR', '2026')


class DecimalEncoder(json.JSONEncoder):
    """Decimals come back out as the ints and floats the page expects."""
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        return super().default(o)


def lambda_handler(event, context):
    table = boto3.resource('dynamodb', region_name=REGION).Table(TABLE)
    rows = table.query(KeyConditionExpression=Key('Year').eq(YEAR)).get('Items', [])

    current, meta, points = None, {}, []
    for row in rows:
        slot = row.get('Slot')
        if slot == 'current':
            current = row
        elif slot == 'meta':
            meta = row
        else:
            points.append(row)
    points.sort(key=lambda p: p['Slot'])

    body = {
        'year': int(YEAR),
        'tracked': [int(t) for t in meta.get('tracked', [])],
        'slot_hours': [int(h) for h in meta.get('slot_hours', [])],
        'current': current,
        'points': points,
    }
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json; charset=utf-8',
            'Access-Control-Allow-Origin': '*',
            # the snapshot only moves five times a day; let the browser and the
            # CDN sit on it for a few minutes rather than re-reading every load
            'Cache-Control': 'public, max-age=300',
        },
        'body': json.dumps(body, cls=DecimalEncoder, ensure_ascii=False),
    }
