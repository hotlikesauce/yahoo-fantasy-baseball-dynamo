"""
Lambda: Serve pre-computed positional strength data as JSON.
Triggered by: Lambda Function URL (HTTPS) — called from browser.
"""

import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table    = dynamodb.Table('FantasyBaseball-PositionalStrength')

CORS_HEADERS = {
    'Content-Type':                 'application/json',
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control':                'max-age=3600',
}


def lambda_handler(event, context):
    try:
        resp = table.get_item(Key={
            'TeamNumber':    '0',
            'DataType#Week': 'computed#positional_strength',
        })
        item = resp.get('Item')
        if not item or 'Data' not in item:
            return {
                'statusCode': 200,
                'headers':    CORS_HEADERS,
                'body':       json.dumps({'error': 'No data yet — run pull_positional_strength first', 'teams': {}}),
            }
        return {
            'statusCode': 200,
            'headers':    CORS_HEADERS,
            'body':       item['Data'],
        }
    except Exception as e:
        logger.error(f"serve_positional_strength FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers':    CORS_HEADERS,
            'body':       json.dumps({'error': str(e)}),
        }
