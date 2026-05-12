"""
Lambda: Serve live positional strength data.
Fetches rosters + AR ranks from Yahoo on every request — no DynamoDB cache.
Same pattern as serve_trade_grades.py.

Triggered by: Lambda Function URL (HTTPS) — called from browser.
Timeout: 90 seconds.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CORS_HEADERS = {
    'Content-Type':                 'application/json',
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control':                'no-cache',
}

YEAR          = 2026
LEAGUE_ID_KEY = 'YAHOO_LEAGUE_ID_2026'

STARTER_SLOTS = {
    'C':  1,
    '1B': 1,
    '2B': 1,
    'SS': 1,
    '3B': 1,
    'OF': 3,
    'SP': 5,
    'RP': 2,
}
BENCH_WEIGHT   = 0.0
TRACKED        = set(STARTER_SLOTS.keys())
AR_FETCH_LIMIT = 600


# ── Yahoo API helpers ─────────────────────────────────────────────────────────

def get_teams(token, league_key) -> Dict[str, dict]:
    resp = yfl.api_get(token, f"league/{league_key}/teams")
    teams_raw = resp['fantasy_content']['league'][1]['teams']
    out = {}
    for tidx, tdata in teams_raw.items():
        if tidx == 'count':
            continue
        info = tdata['team'][0]
        team_key = team_name = team_id = None
        for item in info:
            if not isinstance(item, dict):
                continue
            if 'team_key' in item:
                team_key = item['team_key']
            elif 'team_id' in item:
                team_id = str(item['team_id'])
            elif 'name' in item:
                team_name = item['name']
        if team_key and team_name:
            out[team_key] = {'name': team_name, 'id': team_id}
    return out


def get_roster(token, team_key) -> List[dict]:
    resp = yfl.api_get(token, f"team/{team_key}/roster/players")
    if not resp:
        return []
    try:
        players_raw = resp['fantasy_content']['team'][1]['roster']['0']['players']
        out = []
        for pidx, pdata in players_raw.items():
            if pidx == 'count':
                continue
            player_list = pdata.get('player', [[]])[0]
            player_key = player_name = None
            eligible = []
            for item in player_list:
                if not isinstance(item, dict):
                    continue
                if 'player_key' in item:
                    player_key = item['player_key']
                elif 'name' in item and isinstance(item['name'], dict):
                    player_name = item['name'].get('full', '')
                elif 'eligible_positions' in item:
                    ep = item['eligible_positions']
                    if isinstance(ep, list):
                        pos_list = ep
                    elif isinstance(ep, dict):
                        pos_list = [v for v in ep.values() if v != ep.get('count')]
                    else:
                        pos_list = []
                    for p in pos_list:
                        pos = p.get('position', '') if isinstance(p, dict) else str(p)
                        if pos in TRACKED:
                            eligible.append(pos)
            if player_key and player_name:
                out.append({
                    'player_key': player_key,
                    'name':       player_name,
                    'eligible':   list(set(eligible)),
                })
        return out
    except Exception as e:
        logger.warning(f"Error parsing roster for {team_key}: {e}")
        return []


def get_ar_rank_map(token, league_key) -> Dict[str, int]:
    rank_map = {}
    CHUNK = 25
    for start in range(0, AR_FETCH_LIMIT, CHUNK):
        resp = yfl.api_get(
            token,
            f"league/{league_key}/players;start={start};count={CHUNK};sort=AR",
            timeout=15,
        )
        if not resp:
            break
        try:
            players_raw = resp['fantasy_content']['league'][1]['players']
            count = int(players_raw.get('count', 0))
            for i in range(count):
                pdata = players_raw[str(i)]['player'][0]
                pkey = next(
                    (x['player_key'] for x in pdata if isinstance(x, dict) and 'player_key' in x),
                    None,
                )
                if pkey:
                    rank_map[pkey] = start + i + 1
            if count < CHUNK:
                break
        except Exception as e:
            logger.warning(f"AR rank page start={start}: {e}")
            break
    return rank_map


# ── Scoring ───────────────────────────────────────────────────────────────────

def rank_to_value(rank: int) -> float:
    return max(0.0, (AR_FETCH_LIMIT - rank) / AR_FETCH_LIMIT * 100)


def compute_raw_scores(teams_data: List[dict]) -> Dict[str, dict]:
    results = {}
    for team in teams_data:
        pos_scores = {}
        for pos in TRACKED:
            eligible = sorted(
                [p for p in team['players'] if pos in p['eligible']],
                key=lambda p: p['value'],
                reverse=True,
            )
            n_starters = STARTER_SLOTS[pos]
            starters   = eligible[:n_starters]

            starter_avg = sum(p['value'] for p in starters) / len(starters) if starters else 0.0

            player_list = []
            for idx, p in enumerate(eligible):
                if idx <= n_starters:
                    player_list.append({
                        'name':         p['name'],
                        'current_rank': p['current_rank'],
                        'value':        round(p['value'], 1),
                        'starter':      idx < n_starters,
                    })

            pos_scores[pos] = {'raw': starter_avg, 'players': player_list}
        results[team['name']] = pos_scores
    return results


def normalize(raw_results: Dict[str, dict]) -> Dict[str, dict]:
    team_names = list(raw_results.keys())
    for pos in TRACKED:
        scores = [raw_results[t][pos]['raw'] for t in team_names]
        lo, hi = min(scores), max(scores)
        spread = hi - lo if hi != lo else 1
        for t in team_names:
            raw = raw_results[t][pos]['raw']
            raw_results[t][pos]['score'] = round((raw - lo) / spread * 100, 1)
    return raw_results


# ── Lambda handler ────────────────────────────────────────────────────────────

def lambda_handler(event, context) -> Dict[str, Any]:
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        secrets   = yfl.get_secrets()
        league_id = secrets.get(LEAGUE_ID_KEY)
        if not league_id:
            raise ValueError(f"{LEAGUE_ID_KEY} not found in secrets")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(YEAR, league_id)

        # 1. Teams + rosters
        teams_meta   = get_teams(token, league_key)
        team_rosters = {}
        for tk, meta in teams_meta.items():
            team_rosters[tk] = get_roster(token, tk)

        # 2. Live AR rank map
        ar_rank_map = get_ar_rank_map(token, league_key)
        logger.info(f"AR rank map: {len(ar_rank_map)} players")

        # 3. Assign value per player
        for tk in teams_meta:
            for p in team_rosters[tk]:
                rank = ar_rank_map.get(p['player_key'], AR_FETCH_LIMIT + 1)
                p['value']        = rank_to_value(rank)
                p['current_rank'] = rank if rank <= AR_FETCH_LIMIT else None

        # 4. Compute + normalize
        teams_data = [{'name': meta['name'], 'players': team_rosters[tk]}
                      for tk, meta in teams_meta.items()]
        final = normalize(compute_raw_scores(teams_data))

        # 5. Full rosters for trade analyzer (all players, not just top per position)
        POS_PRIORITY = ['C', '1B', '2B', 'SS', '3B', 'OF', 'SP', 'RP']
        rosters_out = {}
        for tk, meta in teams_meta.items():
            all_players = []
            for p in team_rosters[tk]:
                elig = p.get('eligible', [])
                pos = next((pp for pp in POS_PRIORITY if pp in elig), elig[0] if elig else 'Util')
                all_players.append({
                    'name':  p['name'],
                    'pos':   pos,
                    'rank':  p.get('current_rank'),
                    'value': round(p.get('value', 0), 1),
                })
            all_players.sort(key=lambda x: -(x['value'] or 0))
            rosters_out[meta['name']] = all_players

        output = {
            'teams':         final,
            'rosters':       rosters_out,
            'positions':     list(TRACKED),
            'starter_slots': STARTER_SLOTS,
            'timestamp':     datetime.utcnow().isoformat() + 'Z',
        }

        return {
            'statusCode': 200,
            'headers':    CORS_HEADERS,
            'body':       json.dumps(output),
        }

    except Exception as e:
        logger.error(f"serve_positional_strength FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers':    CORS_HEADERS,
            'body':       json.dumps({'error': str(e)}),
        }
