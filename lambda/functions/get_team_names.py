"""
Lambda: Return current team names as JSON for the static site.
Exposed via Lambda Function URL (no API Gateway needed).
The pull_live_standings function keeps team_names#current up to date every 5 min.
"""

import json
import logging
import boto3
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super().default(o)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET,OPTIONS',
    'Content-Type': 'application/json',
}


def lambda_handler(event, context):
    # Handle preflight OPTIONS request
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        table = dynamodb.Table('FantasyBaseball-SeasonTrends')
        result = table.get_item(Key={
            'TeamNumber': '0',
            'DataType#Week': 'team_names#current',
        })

        item = result.get('Item')
        if not item:
            return {
                'statusCode': 404,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'No team names found. Run pull_live_standings first.'}),
            }

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'year': item.get('Year', 2026),
                'teams': item.get('Teams', {}),   # {team_id: team_name}
                'timestamp': item.get('Timestamp', ''),
            }, cls=DecimalEncoder),
        }

    except Exception as e:
        logger.error(f"get_team_names error: {e}")
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)}),
        }
