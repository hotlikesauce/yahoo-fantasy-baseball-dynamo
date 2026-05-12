"""
Lambda: Compute positional strength scores per team using current Yahoo AR rank.
Mirrors the trade grader — pure current performance, no preseason ADP blending.

Scoring per position:
  - Average of starter slots only — bench carries zero weight

Triggered by: EventBridge schedule (daily recommended)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
BENCH_WEIGHT = 0.0    # starters only — bench has no weight on score
TRACKED      = set(STARTER_SLOTS.keys())

AR_FETCH_LIMIT = 600  # top N players to fetch by current AR rank


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
                    # Yahoo returns list-of-dicts, list-of-strings, or a dict — handle all
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
    """
    Fetch top AR_FETCH_LIMIT players sorted by current in-season overall rank.
    sort=OR;sort_type=season = Yahoo's live season performance rank.
    Returns {player_key: rank} where rank 1 = best.
    """
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
            logger.warning(f"Error parsing AR rank at start={start}: {e}")
            break
    return rank_map


# ── Value score ───────────────────────────────────────────────────────────────

def rank_to_value(rank: int, max_rank: int = AR_FETCH_LIMIT) -> float:
    """Rank 1 → ~100, max_rank → ~0. Players outside top AR_FETCH_LIMIT score 0."""
    return max(0.0, (max_rank - rank) / max_rank * 100)


# ── Scoring ───────────────────────────────────────────────────────────────────

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
            n_starters  = STARTER_SLOTS[pos]
            starters    = eligible[:n_starters]
            bench_one   = eligible[n_starters:n_starters + 1]

            starter_avg = sum(p['value'] for p in starters) / len(starters) if starters else 0.0
            depth_bonus = (bench_one[0]['value'] * BENCH_WEIGHT) if bench_one else 0.0
            raw         = starter_avg + depth_bonus

            player_list = []
            for idx, p in enumerate(eligible):
                if idx < n_starters or idx == n_starters:
                    player_list.append({
                        'name':         p['name'],
                        'current_rank': p['current_rank'],
                        'value':        round(p['value'], 1),
                        'starter':      idx < n_starters,
                    })

            pos_scores[pos] = {'raw': raw, 'players': player_list}
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
    try:
        yfl.log_execution("pull_positional_strength", "START")

        secrets    = yfl.get_secrets()
        league_id  = secrets.get(LEAGUE_ID_KEY)
        if not league_id:
            raise ValueError(f"{LEAGUE_ID_KEY} not found in env")

        token      = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(YEAR, league_id)

        # 1. Teams + rosters
        teams_meta   = get_teams(token, league_key)
        team_rosters = {}
        for tk, meta in teams_meta.items():
            team_rosters[tk] = get_roster(token, tk)

        # 2. Current AR rank map (top 400, same as trade grader)
        ar_rank_map = get_ar_rank_map(token, league_key)
        logger.info(f"AR rank map built: {len(ar_rank_map)} players")

        # 3. Assign value per player based purely on current rank
        for tk in teams_meta:
            tname = teams_meta[tk]['name']
            for p in team_rosters[tk]:
                rank = ar_rank_map.get(p['player_key'], AR_FETCH_LIMIT + 1)
                p['value']        = rank_to_value(rank)
                p['current_rank'] = rank if rank <= AR_FETCH_LIMIT else None
            ss_players = [p for p in team_rosters[tk] if 'SS' in p.get('eligible', [])]
            logger.info(f"SS pool for {tname}: {[(p['name'], p['current_rank']) for p in ss_players]}")

        # 4. Compute + normalize
        teams_data = [{'name': meta['name'], 'players': team_rosters[tk]}
                      for tk, meta in teams_meta.items()]
        raw   = compute_raw_scores(teams_data)
        final = normalize(raw)

        output = {
            'teams':         final,
            'positions':     list(TRACKED),
            'starter_slots': STARTER_SLOTS,
            'bench_weight':  BENCH_WEIGHT,
            'rank_source':   'AR',
            'ar_fetch_limit': AR_FETCH_LIMIT,
            'timestamp':     datetime.utcnow().isoformat() + 'Z',
        }

        unique_players = sum(len(r) for r in team_rosters.values())
        yfl.put_item('FantasyBaseball-PositionalStrength', {
            'TeamNumber':    '0',
            'DataType#Week': 'computed#positional_strength',
            'Year':          YEAR,
            'Data':          json.dumps(output),
            'Timestamp':     datetime.utcnow().isoformat(),
        })

        msg = f"{len(final)} teams, {unique_players} roster spots, {len(ar_rank_map)} AR-ranked players"
        yfl.log_execution("pull_positional_strength", "SUCCESS", msg)
        return {'statusCode': 200, 'body': json.dumps({'message': msg})}

    except Exception as e:
        yfl.log_execution("pull_positional_strength", "FAILED", str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
