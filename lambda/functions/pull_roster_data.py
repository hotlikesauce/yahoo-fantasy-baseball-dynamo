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
from boto3.dynamodb.conditions import Key
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

TEAM_ID_TO_NAME_2026 = {}  # overrides for stale Yahoo team names (none needed currently)


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
            canonical = TEAM_ID_TO_NAME_2026.get(team_id)
            out[team_key] = {'name': canonical or team_name, 'id': team_id}
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
    """Fetch AR rank for top 1000 players."""
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
                pkey  = next(
                    (x['player_key'] for x in pdata if isinstance(x, dict) and 'player_key' in x),
                    None,
                )
                if pkey:
                    rank_map[pkey] = start + i + 1
            if count < CHUNK:
                break
        except Exception as e:
            logger.warning(f"AR rank fetch start={start}: {e}")
            break
    return rank_map


def get_yahoo_adp_map(token, league_key) -> Dict[str, int]:
    """Fetch Yahoo global average_pick for all rostered players via draft_analysis.
    Separate from the AR fetch so it can't affect AR rank parsing."""
    adp_map = {}
    CHUNK   = 25
    for start in range(0, 400, CHUNK):
        resp = yfl.api_get(
            token,
            f"league/{league_key}/players;status=T;start={start};count={CHUNK};out=draft_analysis",
            timeout=15,
        )
        if not resp:
            break
        try:
            players_raw = resp['fantasy_content']['league'][1]['players']
            count = int(players_raw.get('count', 0))
            for i in range(count):
                pdata = players_raw[str(i)]['player']
                pkey  = next(
                    (x['player_key'] for x in pdata[0] if isinstance(x, dict) and 'player_key' in x),
                    None,
                )
                if not pkey:
                    continue
                for part in pdata:
                    if isinstance(part, dict) and 'draft_analysis' in part:
                        try:
                            avg = float(part['draft_analysis'].get('average_pick') or 0)
                            if avg > 0:
                                adp_map[pkey] = round(avg)
                        except (ValueError, TypeError):
                            pass
                        break
            if count < CHUNK:
                break
        except Exception as e:
            logger.warning(f"Yahoo ADP fetch start={start}: {e}")
            break
    logger.info(f"Yahoo ADP map: {len(adp_map)} players")
    return adp_map


def get_draft_adp_map(token, league_key) -> Dict[str, int]:
    """Build ADP map from actual league draft results. Overall pick # = ADP."""
    resp = yfl.api_get(token, f"league/{league_key}/draftresults", timeout=15)
    if not resp:
        return {}
    try:
        draft_raw = resp['fantasy_content']['league'][1]['draft_results']
        adp_map   = {}
        count     = int(draft_raw.get('count', 0))
        for i in range(count):
            dr = draft_raw[str(i)].get('draft_result', {})
            if isinstance(dr, list):
                dr = dr[0]
            pick       = int(dr.get('pick', 0))
            player_key = dr.get('player_key', '')
            if player_key and pick:
                adp_map[player_key] = pick
        logger.info(f"Draft ADP map: {len(adp_map)} picks")
        return adp_map
    except Exception as e:
        logger.warning(f"Draft ADP fetch failed: {e}")
        return {}


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

        # 3. AR rank map (top 1000) — clean fetch, no subresources
        ar_rank_map = get_ar_rank_map(token, league_key)
        # 4a. Yahoo global ADP for rostered players (separate fetch, separate endpoint)
        yahoo_adp   = get_yahoo_adp_map(token, league_key)
        # 4b. League-specific ADP from actual draft results (overrides global ADP — most accurate)
        draft_adp    = get_draft_adp_map(token, league_key)
        adp_rank_map = {**yahoo_adp, **draft_adp}
        logger.info(f"AR map: {len(ar_rank_map)}, ADP map: {len(adp_rank_map)} (draft={len(draft_adp)}, yahoo={len(yahoo_adp)})")

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

        # 6. Purge stale team entries (team renames leave orphan rows)
        existing = table.query(
            KeyConditionExpression=Key('Year').eq(YEAR),
            ProjectionExpression='TeamName',
        )['Items']
        current_names = {meta['name'] for meta in teams_meta.values()}
        for row in existing:
            sk = row['TeamName']
            if not sk.startswith('#') and sk not in current_names:
                logger.info(f"Deleting stale team entry: {sk}")
                table.delete_item(Key={'Year': YEAR, 'TeamName': sk})

        # 7. Meta
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
