"""
Shared utilities for Yahoo Fantasy Baseball Lambda functions.
Handles OAuth token refresh, API calls, DynamoDB operations.
"""

import os
import json
import boto3
import requests
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
secrets_client = boto3.client('secretsmanager', region_name='us-west-2')

BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"
GAME_KEYS = {
    2026: 469,
}

# Credentials from Lambda environment variables
def get_secrets():
    """Fetch Yahoo OAuth credentials from environment variables."""
    try:
        secrets = {
            'YAHOO_CONSUMER_KEY': os.getenv('YAHOO_CONSUMER_KEY'),
            'YAHOO_CONSUMER_SECRET': os.getenv('YAHOO_CONSUMER_SECRET'),
            'YAHOO_REFRESH_TOKEN': os.getenv('YAHOO_REFRESH_TOKEN'),
            'YAHOO_LEAGUE_ID_2026': os.getenv('YAHOO_LEAGUE_ID_2026'),
        }
        if not all(secrets.values()):
            raise ValueError("Missing required environment variables")
        return secrets
    except Exception as e:
        logger.error(f"Failed to get secrets: {e}")
        raise

def get_access_token(secrets: Dict[str, str]) -> Optional[str]:
    """Refresh OAuth token using refresh token."""
    try:
        url = "https://api.login.yahoo.com/oauth2/get_token"
        data = {
            'client_id': secrets['YAHOO_CONSUMER_KEY'],
            'client_secret': secrets['YAHOO_CONSUMER_SECRET'],
            'refresh_token': secrets['YAHOO_REFRESH_TOKEN'],
            'grant_type': 'refresh_token'
        }
        response = requests.post(url, data=data, timeout=10)
        if response.status_code == 200:
            token_data = response.json()
            # Optionally update refresh token if provided
            if 'refresh_token' in token_data:
                secrets['YAHOO_REFRESH_TOKEN'] = token_data['refresh_token']
            return token_data['access_token']
        else:
            logger.error(f"Token refresh failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Token refresh error: {e}")
        return None

def api_get(token: str, endpoint: str, timeout: int = 10) -> Optional[Dict]:
    """Make authenticated API call to Yahoo Fantasy."""
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        url = f"{BASE_URL}/{endpoint}?format=json"
        response = requests.get(url, headers=headers, timeout=timeout)
        if response.status_code == 200:
            return response.json()
        else:
            logger.warning(f"API call failed: {response.status_code} - {endpoint}")
            return None
    except Exception as e:
        logger.error(f"API call error: {e}")
        return None

def put_item(table_name: str, item: Dict[str, Any]) -> bool:
    """Write item to DynamoDB."""
    try:
        table = dynamodb.Table(table_name)
        table.put_item(Item=item)
        return True
    except Exception as e:
        logger.error(f"DynamoDB put_item error: {e}")
        return False

def batch_write_items(table_name: str, items: list) -> int:
    """Batch write items to DynamoDB. Returns count of successful writes."""
    try:
        table = dynamodb.Table(table_name)
        with table.batch_writer(batch_size=25) as batch:
            for item in items:
                batch.put_item(Item=item)
        return len(items)
    except Exception as e:
        logger.error(f"DynamoDB batch write error: {e}")
        return 0

def get_league_key(year: int, league_id: str) -> str:
    """Build Yahoo league key."""
    game_key = GAME_KEYS.get(year)
    if not game_key:
        raise ValueError(f"Game key not found for year {year}")
    return f"{game_key}.l.{league_id}"

def log_execution(function_name: str, status: str, details: str = ""):
    """Log Lambda execution to CloudWatch."""
    timestamp = datetime.utcnow().isoformat()
    log_msg = f"[{timestamp}] {function_name}: {status}"
    if details:
        log_msg += f" - {details}"
    logger.info(log_msg)
