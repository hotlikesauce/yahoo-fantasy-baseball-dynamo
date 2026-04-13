"""
Lambda: Compute positional strength scores per team using a blended value metric:
  - Pre-season: 100% Yahoo preseason ADP rank (lower ADP = better player)
  - Mid-season: blends in current Yahoo overall rank as games pile up
  - Late season: current rank dominates

Scoring per position:
  - Average of starter slots (quality over quantity)
  - Small depth bonus capped at 1 bench player

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
SEASON_GAMES  = 162   # MLB regular season length — used to calculate blend weight

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
BENCH_WEIGHT = 0.15   # single bench player depth bonus weight
TRACKED      = set(STARTER_SLOTS.keys())

# Fallback rank if a player has no preseason ADP (very low-value player)
DEFAULT_PRESEASON_RANK = 500


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


def get_current_rank_map(token, league_key, all_player_keys: List[str]) -> Dict[str, int]:
    """
    Fetch all rostered players sorted by current overall rank (sort=OR).
    Their position in the sorted list = their current rank (1 = best).
    Returns {player_key: current_rank}
    """
    rank_map = {}
    start = 0
    CHUNK = 100
    roster_set = set(all_player_keys)

    while True:
        resp = yfl.api_get(
            token,
            f"league/{league_key}/players;sort=OR;sort_type=season;status=T;start={start};count={CHUNK}"
        )
        if not resp:
            break
        try:
            players_raw = resp['fantasy_content']['league'][1]['players']
            count = int(players_raw.get('count', 0))
            if count == 0:
                break
            for i in range(count):
                pdata = players_raw[str(i)]['player'][0]
                pkey = next((x['player_key'] for x in pdata if isinstance(x, dict) and 'player_key' in x), None)
                if pkey and pkey in roster_set:
                    rank_map[pkey] = start + i + 1  # 1-indexed rank
            if count < CHUNK:
                break
            start += CHUNK
        except Exception as e:
            logger.warning(f"Error parsing current rank at start={start}: {e}")
            break

    return rank_map


def get_preseason_rank_map(token, league_key, player_keys: List[str]) -> Dict[str, float]:
    """
    Fetch preseason ADP for each player via draft_analysis.
    Returns {player_key: preseason_average_pick} — lower = better.
    """
    CHUNK = 25
    adp_map = {}
    for i in range(0, len(player_keys), CHUNK):
        chunk = player_keys[i:i + CHUNK]
        keys_str = ','.join(chunk)
        resp = yfl.api_get(token, f"league/{league_key}/players;player_keys={keys_str};out=draft_analysis")
        if not resp:
            continue
        try:
            players_raw = resp['fantasy_content']['league'][1]['players']
            for pidx, pdata in players_raw.items():
                if pidx == 'count':
                    continue
                player_list = pdata.get('player', [])
                pkey = None
                for item in player_list[0]:
                    if isinstance(item, dict) and 'player_key' in item:
                        pkey = item['player_key']
                        break
                adp = DEFAULT_PRESEASON_RANK
                if len(player_list) > 1 and isinstance(player_list[1], dict):
                    da = player_list[1].get('draft_analysis', [])
                    for entry in da:
                        if isinstance(entry, dict) and 'preseason_average_pick' in entry:
                            try:
                                adp = float(entry['preseason_average_pick'])
                            except (TypeError, ValueError):
                                adp = DEFAULT_PRESEASON_RANK
                            break
                if pkey:
                    adp_map[pkey] = adp
        except Exception as e:
            logger.warning(f"Error parsing draft_analysis chunk {i}: {e}")
    return adp_map


def get_games_played(token, league_key) -> int:
    """Return approximate games played this season (from current week)."""
    try:
        resp = yfl.api_get(token, f"league/{league_key}")
        week = int(resp['fantasy_content']['league'][0].get('current_week', 1))
        return max(0, (week - 1) * 7)  # rough approximation
    except Exception:
        return 0


# ── Blend weight ──────────────────────────────────────────────────────────────

def blend_weight(games_played: int) -> float:
    """
    Returns the weight (0.0–1.0) to apply to current rank.
    At 0 games: 0.0 (100% preseason ADP)
    At 81 games (midseason): 0.5
    At 162 games: 1.0 (100% current rank)
    """
    return min(1.0, games_played / SEASON_GAMES)


# ── Value score ───────────────────────────────────────────────────────────────

def rank_to_value(rank: float, max_rank: int = 500) -> float:
    """
    Convert a rank (lower = better) to a value score (higher = better).
    Rank 1 → ~100, Rank 500 → ~0.
    """
    return max(0.0, (max_rank - rank) / max_rank * 100)


# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_raw_scores(teams_data: List[dict]) -> Dict[str, dict]:
    """
    For each team + position:
      - Sort eligible players by blended value (higher = better)
      - Score = average of starter slots + small depth bonus from best 1 bench player
    """
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

            starter_avg  = sum(p['value'] for p in starters) / len(starters) if starters else 0.0
            depth_bonus  = (bench_one[0]['value'] * BENCH_WEIGHT) if bench_one else 0.0
            raw          = starter_avg + depth_bonus

            player_list = []
            for idx, p in enumerate(eligible):
                if idx < n_starters or idx == n_starters:
                    player_list.append({
                        'name':          p['name'],
                        'current_rank':  p['current_rank'],
                        'preseason_adp': round(p['preseason_adp'], 1),
                        'value':         round(p['value'], 1),
                        'starter':       idx < n_starters,
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
        all_keys     = []
        for tk, meta in teams_meta.items():
            roster = get_roster(token, tk)
            team_rosters[tk] = roster
            all_keys.extend(p['player_key'] for p in roster)
        unique_keys = list(set(all_keys))

        # 2. How far into the season are we?
        games_played  = get_games_played(token, league_key)
        w_current     = blend_weight(games_played)
        w_preseason   = 1.0 - w_current
        logger.info(f"Games played ~{games_played}, w_current={w_current:.2f}, w_preseason={w_preseason:.2f}")

        # 3. Current rank (position in OR-sorted list)
        current_rank_map = get_current_rank_map(token, league_key, unique_keys)

        # 4. Preseason ADP
        preseason_map = get_preseason_rank_map(token, league_key, unique_keys)

        # 5. Blend into a single value score per player
        for tk, meta in teams_meta.items():
            for p in team_rosters[tk]:
                pkey         = p['player_key']
                cur_rank     = current_rank_map.get(pkey, DEFAULT_PRESEASON_RANK)
                pre_adp      = preseason_map.get(pkey, DEFAULT_PRESEASON_RANK)
                cur_val      = rank_to_value(cur_rank)
                pre_val      = rank_to_value(pre_adp)
                p['value']        = w_preseason * pre_val + w_current * cur_val
                p['current_rank'] = cur_rank
                p['preseason_adp'] = pre_adp

        # 6. Compute + normalize
        teams_data = [{'name': meta['name'], 'players': team_rosters[tk]}
                      for tk, meta in teams_meta.items()]
        raw   = compute_raw_scores(teams_data)
        final = normalize(raw)

        output = {
            'teams':         final,
            'positions':     list(TRACKED),
            'starter_slots': STARTER_SLOTS,
            'bench_weight':  BENCH_WEIGHT,
            'games_played':  games_played,
            'w_preseason':   round(w_preseason, 2),
            'w_current':     round(w_current, 2),
            'value_metric':  'blended_rank',
            'timestamp':     datetime.utcnow().isoformat() + 'Z',
        }

        yfl.put_item('FantasyBaseball-PositionalStrength', {
            'TeamNumber':    '0',
            'DataType#Week': 'computed#positional_strength',
            'Year':          YEAR,
            'Data':          json.dumps(output),
            'Timestamp':     datetime.utcnow().isoformat(),
        })

        msg = f"{len(final)} teams, {len(unique_keys)} players, games~{games_played}, blend={w_preseason:.0%} pre/{w_current:.0%} current"
        yfl.log_execution("pull_positional_strength", "SUCCESS", msg)
        return {'statusCode': 200, 'body': json.dumps({'message': msg})}

    except Exception as e:
        yfl.log_execution("pull_positional_strength", "FAILED", str(e))
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
