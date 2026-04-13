"""
Lambda: Serve 2027 draft capital as JSON.
Reads pre-computed data stored by compute_draft_capital from DynamoDB.
Triggered by: Lambda Function URL (HTTPS) - called from browser.
"""

import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'max-age=3600',
}


def lambda_handler(event, context):
    try:
        if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
            return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

        resp = table.get_item(Key={
            'TeamNumber': '0',
            'DataType#Week': 'computed#draft_capital',
        })
        item = resp.get('Item')
        if not item or 'Data' not in item:
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'No draft capital data yet. Trigger compute_draft_capital to scrape.', 'teams': []}),
            }

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': item['Data'],
        }

    except Exception as e:
        logger.error(f"serve_draft_capital FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)}),
        }
