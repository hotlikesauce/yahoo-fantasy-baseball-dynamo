"""Yahoo's public fantasy API - the fantasy layer.

pub-api-rw needs no OAuth and kept working through the 2026 app-wide 403s, so
it is the only Yahoo surface this project can still rely on. It gives us
rosters, position eligibility, the free agent pool and league settings.
"""
import json
import time
from urllib.request import Request, urlopen

PUB_API = 'https://pub-api-rw.fantasysports.yahoo.com/fantasy/v2'
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

YEAR = 2026
GAME_KEY = 469
LEAGUE_ID = '8614'
LEAGUE_KEY = '{}.l.{}'.format(GAME_KEY, LEAGUE_ID)


def get(url, tries=3):
    for attempt in range(tries):
        try:
            req = Request(url, headers={'User-Agent': UA})
            with urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode('utf-8', 'replace'))
        except Exception as e:
            if attempt == tries - 1:
                raise
            print('    retry {} after {}: {}'.format(attempt + 1, type(e).__name__, e))
            time.sleep(2 * (attempt + 1))


def flatten(block):
    """Yahoo nests metadata as a list of single-key dicts."""
    out = {}
    for item in block:
        if isinstance(item, dict):
            out.update(item)
    return out


def _iter_numeric(container):
    """Yahoo collections are dicts keyed '0','1',... plus a 'count'."""
    for k in sorted((k for k in container if k.isdigit()), key=int):
        yield container[k]


def parse_player(block):
    """One Yahoo player block -> the fields the hub actually needs."""
    meta = flatten(block[0] if isinstance(block[0], list) else block)
    eligible = [p['position'] for p in meta.get('eligible_positions', [])
                if isinstance(p, dict) and 'position' in p]
    player = {
        'player_key': meta.get('player_key'),
        'player_id': meta.get('player_id'),
        'name': meta.get('name', {}).get('full'),
        'first': meta.get('name', {}).get('ascii_first'),
        'last': meta.get('name', {}).get('ascii_last'),
        'team_abbr': meta.get('editorial_team_abbr'),
        'display_position': meta.get('display_position'),
        'position_type': meta.get('position_type'),
        'eligible_positions': eligible,
        'headshot': (meta.get('headshot') or {}).get('url'),
        'status': meta.get('status'),
        'status_full': meta.get('status_full'),
    }
    # The roster endpoint appends the slot the manager had him in.
    if isinstance(block, list) and len(block) > 1 and isinstance(block[1], dict):
        sel = block[1].get('selected_position')
        if sel:
            player['selected_position'] = flatten(sel).get('position')
    return player


def team_roster(team_id, date=None):
    """Every player on a team, as of a date (defaults to today)."""
    suffix = ';date={}'.format(date) if date else ''
    url = '{}/team/{}.t.{}/roster{}?format=json'.format(PUB_API, LEAGUE_KEY, team_id, suffix)
    team = get(url)['fantasy_content']['team']
    players = team[1]['roster']['0']['players']
    return [parse_player(p['player']) for p in _iter_numeric(players)]


def teams():
    """{team_id: team name} for all 12."""
    lg = get('{}/league/{}/teams?format=json'.format(PUB_API, LEAGUE_KEY))['fantasy_content']['league']
    out = {}
    for t in _iter_numeric(lg[1]['teams']):
        meta = flatten(t['team'][0])
        out[int(str(meta['team_key']).rsplit('.', 1)[-1])] = meta.get('name')
    return out


def free_agents(batch=25, limit=1000):
    """Walk the free agent pool. Yahoo pages 25 at a time."""
    out = []
    start = 0
    while start < limit:
        url = '{}/league/{}/players;status=FA;start={};count={}?format=json'.format(
            PUB_API, LEAGUE_KEY, start, batch)
        lg = get(url)['fantasy_content']['league']
        players = lg[1]['players']
        if not isinstance(players, dict) or int(players.get('count', 0)) == 0:
            break
        chunk = [parse_player(p['player']) for p in _iter_numeric(players)]
        out.extend(chunk)
        if len(chunk) < batch:
            break
        start += batch
        time.sleep(0.3)
    return out


def roster_positions():
    """The league's roster slots, e.g. {'C': 1, 'OF': 3, 'BN': 5, ...}."""
    lg = get('{}/league/{}/settings?format=json'.format(PUB_API, LEAGUE_KEY))['fantasy_content']['league']
    out = {}
    for rp in lg[1]['settings'][0]['roster_positions']:
        p = rp['roster_position']
        out[p['position']] = int(p['count'])
    return out
