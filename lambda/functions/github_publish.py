"""
Commit a file to GitHub from Lambda, via the Contents API.

No git binary and no checkout: read the file that is there, compare, and PUT a
new blob if it actually differs. GitHub Pages picks the commit up from there.

The comparison ignores the parts of the page that change on every render (the
build stamp, the scrape timestamp), so a schedule that fires five times a day
against unchanged standings does not produce five empty commits.
"""

import os
import re
import json
import base64
from urllib.request import Request, urlopen
from urllib.error import HTTPError

API = 'https://api.github.com'
REPO = os.environ.get('GITHUB_REPO', 'hotlikesauce/yahoo-fantasy-baseball-dynamo')
BRANCH = os.environ.get('GITHUB_BRANCH', 'main')
# named to fall inside the role's existing yahoo-fantasy-baseball* grant, so
# publishing needed no new IAM
SECRET = os.environ.get('GITHUB_SECRET_NAME', 'yahoo-fantasy-baseball-github')

# Everything here moves on its own every render; none of it is a real change.
VOLATILE = [
    re.compile(r'Built [A-Z][a-z]{2} [A-Z][a-z]{2} \d+, [\d: ]+[AP]M'),
    re.compile(r'"scraped_at": "[^"]*"'),
    re.compile(r'"recorded_at": "[^"]*"'),
]

_token = None


def token():
    """Cached across warm invocations - Secrets Manager calls are not free."""
    global _token
    if _token:
        return _token
    import boto3
    raw = boto3.client(
        'secretsmanager',
        region_name=os.environ.get('AWS_REGION', 'us-west-2')
    ).get_secret_value(SecretId=SECRET)['SecretString']
    try:
        _token = json.loads(raw)['GITHUB_TOKEN']
    except (ValueError, KeyError):
        _token = raw.strip()        # a bare token string is fine too
    return _token


def _call(method, path, payload=None):
    req = Request(
        f'{API}{path}',
        method=method,
        data=json.dumps(payload).encode() if payload else None,
        headers={
            'Authorization': f'Bearer {token()}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
            'User-Agent': 'fantasy-baseball-lambda',
            'Content-Type': 'application/json',
        },
    )
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def normalize(text):
    for pat in VOLATILE:
        text = pat.sub('', text)
    return text


def publish(path, content, message):
    """
    Write `content` to `path` on the branch. Returns one of 'created',
    'updated' or 'unchanged'.
    """
    sha, existing = None, None
    try:
        cur = _call('GET', f'/repos/{REPO}/contents/{path}?ref={BRANCH}')
        sha = cur['sha']
        existing = base64.b64decode(cur['content']).decode('utf-8', 'replace')
    except HTTPError as e:
        if e.code != 404:
            raise                       # 404 just means the file is new

    if existing is not None and normalize(existing) == normalize(content):
        return 'unchanged'

    body = {
        'message': message,
        'content': base64.b64encode(content.encode('utf-8')).decode(),
        'branch': BRANCH,
    }
    if sha:
        body['sha'] = sha
    _call('PUT', f'/repos/{REPO}/contents/{path}', body)
    return 'updated' if sha else 'created'
