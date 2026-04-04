"""
Lambda: Pull team rosters + season player points from Yahoo, compute positional
strength scores per team, store result in FantasyBaseball-SeasonTrends.

Triggered by: EventBridge schedule (daily recommended)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

YEAR = 2026
LEAGUE_ID_KEY = 'YAHOO_LEAGUE_ID_2026'

# Number of active roster slots per position — adjust to match league settings
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
BENCH_WEIGHT  = 0.4   # bench players count at 40% vs starters at 100%
TRACKED       = set(STARTER_SLOTS.keys())


# ── Yahoo API helpers ────────────────────────────────────────────────────────

def get_teams(token, league_key) -> Dict[str, dict]:
    """Return {team_key: {name, id}} for all teams in the league."""
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
    """Return list of {player_key, name, eligible_positions} for a team's roster."""
    resp = yfl.api_get(token, f"team/{team_key}/roster/players")
    if not resp:
        return []
    try:
        team_data  = resp['fantasy_content']['team']
        players_raw = team_data[1]['roster']['0']['players']
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
                elif 'full_name' in item:
                    player_name = item['full_name']
                elif 'eligible_positions' in item:
                    pos_list = item['eligible_positions']
                    if isinstance(pos_list, list):
                        for p in pos_list:
                            pos = p.get('position', '') if isinstance(p, dict) else p
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


def get_player_points(token, league_key, player_keys: List[str]) -> Dict[str, float]:
    """Return {player_key: season_points} for a list of player keys."""
    CHUNK = 25
    points = {}
    for i in range(0, len(player_keys), CHUNK):
        chunk = player_keys[i:i + CHUNK]
        keys_str = ','.join(chunk)
        resp = yfl.api_get(
            token,
            f"league/{league_key}/players;player_keys={keys_str};out=player_points"
        )
        if not resp:
            continue
        try:
            players_raw = resp['fantasy_content']['league'][1]['players']
            for pidx, pdata in players_raw.items():
                if pidx == 'count':
                    continue
                player_list = pdata.get('player', [])
                if len(player_list) < 2:
                    continue
                pkey = None
                for item in player_list[0]:
                    if isinstance(item, dict) and 'player_key' in item:
                        pkey = item['player_key']
                        break
                total = 0.0
                if isinstance(player_list[1], dict):
                    raw = player_list[1].get('player_points', {}).get('total', 0)
                    try:
                        total = float(raw) if raw else 0.0
                    except (TypeError, ValueError):
                        total = 0.0
                if pkey:
                    points[pkey] = total
        except Exception as e:
            logger.warning(f"Error parsing player points chunk {i}: {e}")
    return points


# ── Scoring ──────────────────────────────────────────────────────────────────

def compute_raw_scores(teams_data: List[dict]) -> Dict[str, dict]:
    """
    For each team, for each position: sort eligible players by points,
    apply starter/bench weights, sum to a raw score.
    Returns {team_name: {pos: {raw, players}}}
    """
    results = {}
    for team in teams_data:
        pos_scores = {}
        for pos in TRACKED:
            eligible = sorted(
                [p for p in team['players'] if pos in p['eligible']],
                key=lambda p: p['points'],
                reverse=True,
            )
            n_starters = STARTER_SLOTS[pos]
            raw = 0.0
            player_list = []
            for idx, p in enumerate(eligible):
                weight = 1.0 if idx < n_starters else BENCH_WEIGHT
                raw += p['points'] * weight
                player_list.append({
                    'name':    p['name'],
                    'points':  round(p['points'], 1),
                    'starter': idx < n_starters,
                })
            pos_scores[pos] = {'raw': raw, 'players': player_list}
        results[team['name']] = pos_scores
    return results


def normalize(raw_results: Dict[str, dict]) -> Dict[str, dict]:
    """Normalize raw scores 0-100 per position across all teams."""
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
    try:
        yfl.log_execution("pull_positional_strength", "START")

        secrets = yfl.get_secrets()
        league_id = secrets.get(LEAGUE_ID_KEY)
        if not league_id:
            raise ValueError(f"{LEAGUE_ID_KEY} not found in env")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(YEAR, league_id)

        # 1. All teams
        teams_meta = get_teams(token, league_key)

        # 2. Rosters for every team
        all_player_keys = []
        team_rosters = {}
        for team_key, meta in teams_meta.items():
            roster = get_roster(token, team_key)
            team_rosters[team_key] = roster
            all_player_keys.extend(p['player_key'] for p in roster)

        # 3. Season fantasy points for every player on every roster
        points_map = get_player_points(token, league_key, list(set(all_player_keys)))

        # 4. Attach points
        teams_data = []
        for team_key, meta in teams_meta.items():
            players = team_rosters[team_key]
            for p in players:
                p['points'] = points_map.get(p['player_key'], 0.0)
            teams_data.append({'name': meta['name'], 'players': players})

        # 5. Compute + normalize
        raw   = compute_raw_scores(teams_data)
        final = normalize(raw)

        output = {
            'teams':        final,
            'positions':    list(TRACKED),
            'starter_slots': STARTER_SLOTS,
            'bench_weight': BENCH_WEIGHT,
            'timestamp':    datetime.utcnow().isoformat() + 'Z',
        }

        yfl.put_item('FantasyBaseball-PositionalStrength', {
            'TeamNumber':   '0',
            'DataType#Week': 'computed#positional_strength',
            'Year':         YEAR,
            'Data':         json.dumps(output),
            'Timestamp':    datetime.utcnow().isoformat(),
        })

        msg = f"{len(final)} teams, {len(set(all_player_keys))} players"
        yfl.log_execution("pull_positional_strength", "SUCCESS", msg)
        return {'statusCode': 200, 'body': json.dumps({'message': msg})}

    except Exception as e:
        yfl.log_execution("pull_positional_strength", "FAILED", str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
