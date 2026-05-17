"""
Lambda: Pull full roster data + AR ranks daily and store in DynamoDB.
Expands AR fetch to 1000 players. Stores one item per team plus an AR rank map
item (used by serve_trade_grades to avoid live Yahoo rank fetches).

Scheduled: 6am MST = cron(0 13 * * ? *) UTC
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import boto3
import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-RosterData')

YEAR           = 2026
LEAGUE_ID_KEY  = 'YAHOO_LEAGUE_ID_2026'
AR_FETCH_LIMIT  = 1000
ADP_FETCH_LIMIT = 300   # ADP only covers drafted players (~250 in a 12-team 22-round league)
POS_PRIORITY   = ['C', '1B', '2B', 'SS', '3B', 'OF', 'SP', 'RP']
TRACKED        = set(POS_PRIORITY)


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


def _fetch_rank_map(token, league_key, sort: str, limit: int) -> Dict[str, int]:
    rank_map = {}
    CHUNK = 25
    for start in range(0, limit, CHUNK):
        resp = yfl.api_get(
            token,
            f"league/{league_key}/players;start={start};count={CHUNK};sort={sort}",
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
            logger.warning(f"{sort} rank fetch start={start}: {e}")
            break
    return rank_map


def get_ar_rank_map(token, league_key) -> Dict[str, int]:
    return _fetch_rank_map(token, league_key, 'AR', AR_FETCH_LIMIT)


def get_adp_rank_map(token, league_key) -> Dict[str, int]:
    return _fetch_rank_map(token, league_key, 'ADP', ADP_FETCH_LIMIT)


def lambda_handler(event, context):
    try:
        logger.info("pull_roster_data: START")
        secrets    = yfl.get_secrets()
        league_id  = secrets.get(LEAGUE_ID_KEY)
        if not league_id:
            raise ValueError(f"{LEAGUE_ID_KEY} not found in secrets")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(YEAR, league_id)
        timestamp  = datetime.utcnow().isoformat() + 'Z'

        # 1. Teams
        teams_meta = get_teams(token, league_key)
        logger.info(f"Fetched {len(teams_meta)} teams")

        # 2. Rosters
        team_rosters = {}
        for tk, meta in teams_meta.items():
            team_rosters[tk] = get_roster(token, tk)
            logger.info(f"  {meta['name']}: {len(team_rosters[tk])} players")

        # 3. AR rank map (top 1000) + ADP rank map (top 300)
        ar_rank_map  = get_ar_rank_map(token, league_key)
        adp_rank_map = get_adp_rank_map(token, league_key)
        logger.info(f"AR rank map: {len(ar_rank_map)} players, ADP map: {len(adp_rank_map)} players")

        # 4. Store one item per team
        total_players = 0
        for tk, meta in teams_meta.items():
            players = []
            for p in team_rosters[tk]:
                rank = ar_rank_map.get(p['player_key'])
                adp  = adp_rank_map.get(p['player_key'])
                elig = p['eligible']
                pos  = next((pp for pp in POS_PRIORITY if pp in elig), elig[0] if elig else 'Util')
                players.append({
                    'name':       p['name'],
                    'player_key': p['player_key'],
                    'pos':        pos,
                    'eligible':   elig,
                    'rank':       rank,
                    'adp':        adp,
                })
            total_players += len(players)
            table.put_item(Item={
                'Year':        YEAR,
                'TeamName':    meta['name'],
                'TeamId':      meta.get('id', ''),
                'Players':     json.dumps(players),
                'PlayerCount': len(players),
                'Timestamp':   timestamp,
            })

        # 5. Store AR rank map for trade grader (avoids live Yahoo rank fetches)
        table.put_item(Item={
            'Year':        YEAR,
            'TeamName':    '#ar_rank_map',
            'RankMap':     json.dumps(ar_rank_map),
            'PlayerCount': len(ar_rank_map),
            'Timestamp':   timestamp,
        })

        # 6. Meta
        table.put_item(Item={
            'Year':         YEAR,
            'TeamName':     '#meta',
            'TeamCount':    len(teams_meta),
            'TotalPlayers': total_players,
            'ArRankLimit':  AR_FETCH_LIMIT,
            'Timestamp':    timestamp,
        })

        msg = f"Stored rosters: {len(teams_meta)} teams, {total_players} total players, {len(ar_rank_map)} AR-ranked"
        logger.info(f"pull_roster_data: SUCCESS — {msg}")
        return {'statusCode': 200, 'body': msg}

    except Exception as e:
        logger.error(f"pull_roster_data FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
