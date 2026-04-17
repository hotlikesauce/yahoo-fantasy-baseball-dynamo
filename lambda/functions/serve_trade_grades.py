"""
Lambda: Serve trade grades for 2026 season.
Fetches all completed trades from Yahoo Fantasy API, grades them using current
player rankings + draft pick values (same exponential decay as draft capital page).

Triggered by: Lambda Function URL (HTTPS) - called from browser on page load.
Timeout: Set to 60 seconds (5 Yahoo API calls needed).
Memory: 256 MB.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'no-cache',
}

# Same formula as draft_picks_2026.html / gen_draft_picks.py
SCALE = 242.0
DECAY = 0.98
KEEPER_ROUNDS = 2  # R1-R2 are keeper rounds (not tradeable)
SP_MULTIPLIER = 4.0  # Pure SPs boosted 4x — Yahoo AR undervalues SPs in a 50 IP/week league
PICK_DISCOUNT = 0.5  # Future picks discounted 50% — outcome uncertain (tanking vs contending)


def pick_value_for_round(round_num: int) -> float:
    """Average value for a pick in the given round (using avg pick position in round)."""
    if round_num <= KEEPER_ROUNDS:
        return 0.0
    # Overall pick number (1-indexed) for the average slot in this round
    avg_overall = (round_num - KEEPER_ROUNDS - 1) * 12 + 6.5
    return round(SCALE * (DECAY ** (avg_overall - 1)) * PICK_DISCOUNT, 1)


def player_value(rank: Optional[int]) -> float:
    """Convert overall player rank to point value (same scale as picks)."""
    if rank is None or rank > 600:
        return 1.5
    return round(SCALE * (DECAY ** (rank - 1)), 1)


def assign_grade(advantage: float):
    """
    advantage = (received - given) / max(received, given)
    Returns (letter_grade, hex_color).
    """
    if advantage >= 0.50:  return ('A+', '#22c55e')
    if advantage >= 0.30:  return ('A',  '#4ade80')
    if advantage >= 0.10:  return ('B',  '#86efac')
    if advantage >= -0.10: return ('C',  '#fbbf24')
    if advantage >= -0.30: return ('D',  '#f97316')
    return ('F', '#ef4444')


def fetch_team_names(token: str, league_key: str) -> dict:
    data = yfl.api_get(token, f"league/{league_key}/teams")
    if not data:
        return {}
    team_map = {}
    try:
        league = data.get('fantasy_content', {}).get('league', [])
        if len(league) < 2:
            return {}
        teams_raw = league[1].get('teams', {})
        count = int(teams_raw.get('count', 0))
        for i in range(count):
            t_list = teams_raw.get(str(i), {}).get('team', [])
            if not t_list:
                continue
            t_info = t_list[0]
            team_key = None
            team_name = None
            for item in (t_info if isinstance(t_info, list) else []):
                if not isinstance(item, dict):
                    continue
                if 'team_key' in item:
                    team_key = item['team_key']
                elif 'name' in item:
                    team_name = item['name']
            if team_key and team_name:
                team_map[team_key] = team_name
    except Exception as e:
        logger.warning(f"Error parsing teams: {e}")
    return team_map


def fetch_trades(token: str, league_key: str) -> list:
    """Fetch all successful trade transactions."""
    data = yfl.api_get(token, f"league/{league_key}/transactions;types=trade;count=100", timeout=20)
    if not data:
        return []
    trades = []
    try:
        league = data.get('fantasy_content', {}).get('league', [])
        if len(league) < 2:
            return []
        txns_raw = league[1].get('transactions', {})
        count = int(txns_raw.get('count', 0))

        for i in range(count):
            txn_list = txns_raw.get(str(i), {}).get('transaction', [])
            if len(txn_list) < 2:
                continue
            meta = txn_list[0]
            detail = txn_list[1]

            if not isinstance(meta, dict):
                continue
            if meta.get('type') != 'trade' or meta.get('status') != 'successful':
                continue

            trade = {
                'key': meta.get('transaction_key', ''),
                'timestamp': int(meta.get('timestamp', 0) or 0),
                'trader_team_key': meta.get('trader_team_key', ''),
                'tradee_team_key': meta.get('tradee_team_key', ''),
                'players': [],
                'picks': [],
            }

            # Parse players (in detail/t[1])
            players_raw = detail.get('players', {}) if isinstance(detail, dict) else {}
            if isinstance(players_raw, dict):
                p_count = int(players_raw.get('count', 0))
                for pi in range(p_count):
                    p_list = players_raw.get(str(pi), {}).get('player', [])
                    if len(p_list) < 2:
                        continue
                    p_info = p_list[0]
                    p_txn_wrapper = p_list[1]

                    player_key = None
                    player_name = None
                    player_pos = None
                    for item in (p_info if isinstance(p_info, list) else []):
                        if not isinstance(item, dict):
                            continue
                        if 'player_key' in item:
                            player_key = item['player_key']
                        elif 'name' in item:
                            n = item['name']
                            player_name = n.get('full', '') if isinstance(n, dict) else str(n)
                        elif 'display_position' in item:
                            player_pos = item['display_position']

                    txn_data = p_txn_wrapper.get('transaction_data', {}) if isinstance(p_txn_wrapper, dict) else {}
                    if isinstance(txn_data, list):
                        txn_data = txn_data[0] if txn_data else {}

                    if player_key:
                        trade['players'].append({
                            'player_key': player_key,
                            'name': player_name or player_key,
                            'position': player_pos or '',
                            'source': txn_data.get('source_team_key', ''),
                            'dest': txn_data.get('destination_team_key', ''),
                        })

            # Picks live in meta (t[0]) as a plain list of {"pick": {...}} objects
            picks_raw = meta.get('picks', [])
            if isinstance(picks_raw, list):
                for pk_wrapper in picks_raw:
                    pk = pk_wrapper.get('pick', {}) if isinstance(pk_wrapper, dict) else {}
                    if not isinstance(pk, dict):
                        continue
                    try:
                        rnd = int(pk.get('round', 0))
                        if rnd > 0:
                            trade['picks'].append({
                                'round': rnd,
                                'source': pk.get('source_team_key', ''),
                                'dest': pk.get('destination_team_key', ''),
                                'original_owner': pk.get('original_team_name', ''),
                            })
                    except Exception:
                        pass

            if trade['players'] or trade['picks']:
                trades.append(trade)

    except Exception as e:
        logger.error(f"Error parsing trades: {e}", exc_info=True)

    return trades


def _extract_adp(da) -> Optional[float]:
    """Parse preseason_average_pick (or average_pick fallback) from draft_analysis."""
    da_list = da if isinstance(da, list) else [da] if isinstance(da, dict) else []
    for key in ('preseason_average_pick', 'average_pick'):
        for item in da_list:
            if isinstance(item, dict) and key in item:
                try:
                    v = float(item[key])
                    if v > 0:
                        return v
                except (ValueError, TypeError):
                    pass
    return None


def _parse_player_list(p_list: list, rank: Optional[int] = None) -> tuple:
    """Extract (player_key, name, pos, adp) from a Yahoo player list."""
    player_key = player_name = player_pos = None
    adp = None
    all_items = list(p_list[0]) if isinstance(p_list[0], list) else []
    if len(p_list) > 1 and isinstance(p_list[1], dict):
        all_items.append(p_list[1])
    for item in all_items:
        if not isinstance(item, dict):
            continue
        if 'player_key' in item:
            player_key = item['player_key']
        elif 'name' in item:
            n = item['name']
            player_name = n.get('full', '') if isinstance(n, dict) else str(n)
        elif 'display_position' in item:
            player_pos = item['display_position']
        elif 'draft_analysis' in item:
            adp = _extract_adp(item['draft_analysis'])
    return player_key, player_name, player_pos, adp


def fetch_player_ranks(token: str, league_key: str, traded_keys: list) -> dict:
    """
    Build rank map:
    1. Top 300 by current in-season rank (sort=AR), 25 per page (Yahoo limit).
    2. Direct lookup of all traded player keys for ADP.
    Returns {player_key: {rank, adp, name, pos}}.
    """
    rank_map: dict = {}

    # Step 1: top 300 current rank, 25/page
    for start in range(0, 300, 25):
        data = yfl.api_get(token, f"league/{league_key}/players;start={start};count=25;sort=AR", timeout=15)
        if not data:
            continue
        try:
            league_data = data.get('fantasy_content', {}).get('league', [])
            if len(league_data) < 2:
                continue
            players_raw = league_data[1].get('players', {})
            cnt = int(players_raw.get('count', 0))
            for i in range(cnt):
                p_list = players_raw.get(str(i), {}).get('player', [])
                if not p_list:
                    continue
                pkey, pname, ppos, _ = _parse_player_list(p_list)
                if pkey:
                    rank_map.setdefault(pkey, {}).update({'rank': start + i + 1, 'name': pname, 'pos': ppos})
        except Exception as e:
            logger.warning(f"Rank page start={start}: {e}")

    # Step 2: direct lookup for ADP on traded players
    if traded_keys:
        keys_str = ','.join(traded_keys[:50])
        data = yfl.api_get(token, f"league/{league_key}/players;player_keys={keys_str};out=draft_analysis", timeout=20)
        if data:
            try:
                league_data = data.get('fantasy_content', {}).get('league', [])
                if len(league_data) >= 2:
                    players_raw = league_data[1].get('players', {})
                    cnt = int(players_raw.get('count', 0))
                    for i in range(cnt):
                        p_list = players_raw.get(str(i), {}).get('player', [])
                        if not p_list:
                            continue
                        pkey, pname, ppos, adp = _parse_player_list(p_list)
                        if pkey:
                            entry = rank_map.setdefault(pkey, {})
                            entry['adp'] = adp
                            entry.setdefault('name', pname)
                            entry.setdefault('pos', ppos)
            except Exception as e:
                logger.warning(f"Direct player lookup: {e}")

    return rank_map


def grade_trade(trade: dict, rank_map: dict, team_names: dict) -> dict:
    trader_key = trade['trader_team_key']
    tradee_key = trade['tradee_team_key']

    trader_name = team_names.get(trader_key, f'Team {trader_key.split(".")[-1]}')
    tradee_name = team_names.get(tradee_key, f'Team {tradee_key.split(".")[-1]}')

    def enrich_players(dest_key):
        out = []
        for p in trade['players']:
            if p['dest'] != dest_key:
                continue
            info = rank_map.get(p['player_key'], {})
            rank = info.get('rank')
            adp = info.get('adp')
            pos = info.get('pos') or p['position']
            base_val = player_value(rank)
            sp_boosted = (pos == 'SP')
            value = round(base_val * SP_MULTIPLIER, 1) if sp_boosted else base_val
            out.append({
                'name': info.get('name') or p['name'],
                'pos': pos,
                'rank': rank,
                'adp': round(adp) if adp else None,
                'value': value,
                'sp_boosted': sp_boosted,
            })
        return out

    def enrich_picks(dest_key):
        out = []
        for pk in trade['picks']:
            if pk['dest'] != dest_key:
                continue
            val = pick_value_for_round(pk['round'])
            out.append({
                'round': pk['round'],
                'original_owner': pk.get('original_owner', ''),
                'value': val,
            })
        return out

    trader_players = enrich_players(trader_key)
    tradee_players = enrich_players(tradee_key)
    trader_picks   = enrich_picks(trader_key)
    tradee_picks   = enrich_picks(tradee_key)

    trader_val = sum(p['value'] for p in trader_players) + sum(pk['value'] for pk in trader_picks)
    tradee_val = sum(p['value'] for p in tradee_players) + sum(pk['value'] for pk in tradee_picks)

    max_val = max(trader_val, tradee_val, 1.0)
    trader_adv = (trader_val - tradee_val) / max_val
    tradee_adv = (tradee_val - trader_val) / max_val

    trader_grade, trader_color = assign_grade(trader_adv)
    tradee_grade, tradee_color = assign_grade(tradee_adv)

    ts = trade.get('timestamp', 0)
    try:
        date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%b %d, %Y')
    except Exception:
        date_str = ''

    return {
        'date': date_str,
        'timestamp': ts,
        'trader': {
            'name': trader_name,
            'players': trader_players,
            'picks': trader_picks,
            'value': round(trader_val, 1),
            'grade': trader_grade,
            'grade_color': trader_color,
            'advantage_pct': round(trader_adv * 100),
        },
        'tradee': {
            'name': tradee_name,
            'players': tradee_players,
            'picks': tradee_picks,
            'value': round(tradee_val, 1),
            'grade': tradee_grade,
            'grade_color': tradee_color,
            'advantage_pct': round(tradee_adv * 100),
        },
    }


def lambda_handler(event, context):
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        yfl.log_execution("serve_trade_grades", "START")

        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(2026, league_id)

        team_names = fetch_team_names(token, league_key)
        trades_raw = fetch_trades(token, league_key)

        if not trades_raw:
            return {
                'statusCode': 200,
                'headers': CORS_HEADERS,
                'body': json.dumps({'trades': [], 'generated': datetime.utcnow().isoformat()}),
            }

        all_keys = list({p['player_key'] for t in trades_raw for p in t['players']})
        rank_map = fetch_player_ranks(token, league_key, all_keys)

        graded = []
        for t in trades_raw:
            try:
                graded.append(grade_trade(t, rank_map, team_names))
            except Exception as e:
                logger.warning(f"Could not grade trade {t.get('key', '')}: {e}")

        graded.sort(key=lambda x: x.get('timestamp', 0), reverse=True)

        result = {'trades': graded, 'generated': datetime.utcnow().isoformat()}
        yfl.log_execution("serve_trade_grades", "SUCCESS", f"{len(graded)} trades graded")

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps(result),
        }

    except Exception as e:
        logger.error(f"serve_trade_grades FAILED: {e}", exc_info=True)
        yfl.log_execution("serve_trade_grades", "FAILED", str(e))
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e), 'trades': []}),
        }
