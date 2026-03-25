#!/usr/bin/env python3
"""
Re-authenticate with Yahoo OAuth and get a new refresh token.
Run this when your refresh token has expired or been rotated.
"""

import os
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')

AUTH_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

# Step 1: Print authorization URL
print("\n=== Yahoo OAuth Re-Authentication ===\n")
print("1. Visit this URL in your browser:")
REDIRECT_URI = "https://localhost"

print(f"\n  {AUTH_URL}?client_id={CONSUMER_KEY}&redirect_uri={REDIRECT_URI}&response_type=code\n")
print("2. Authorize the app")
print("3. Yahoo will redirect to https://localhost — the page will show a connection error,")
print("   that's fine. Look at the URL bar: it will contain ?code=XXXX")
print("   Copy just that code value (everything after 'code=' up to any '&')\n")

code = input("Paste the code here: ").strip()

# Step 2: Exchange code for tokens
credentials = base64.b64encode(f"{CONSUMER_KEY}:{CONSUMER_SECRET}".encode()).decode()
headers = {
    'Authorization': f'Basic {credentials}',
    'Content-Type': 'application/x-www-form-urlencoded'
}
data = {
    'grant_type': 'authorization_code',
    'redirect_uri': REDIRECT_URI,
    'code': code
}

response = requests.post(TOKEN_URL, headers=headers, data=data)
if response.status_code == 200:
    tokens = response.json()
    access_token = tokens.get('access_token')
    refresh_token = tokens.get('refresh_token')
    print(f"\n✅ Success!\n")
    print(f"New YAHOO_REFRESH_TOKEN:\n{refresh_token}\n")
    print("Update your .env file with this new refresh token.")
    print("Also update the Lambda env vars:")
    print(f"  aws lambda update-function-configuration --function-name pull-live-standings --environment \"Variables={{...YAHOO_REFRESH_TOKEN={refresh_token}}}\" --region us-west-2")
else:
    print(f"\n❌ Failed: {response.status_code}")
    print(response.text)
