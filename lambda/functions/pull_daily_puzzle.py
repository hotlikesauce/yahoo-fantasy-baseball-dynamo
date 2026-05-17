"""
Lambda: Pull yesterday's top MLB stat line for a rostered player.
Creates the daily Stat Line Guesser puzzle stored in FantasyBaseball-DailyPuzzle.
Scheduled: 4:30am MST = cron(30 11 * * ? *)
"""

import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Key
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb    = boto3.resource('dynamodb', region_name='us-west-2')
puzzle_tbl  = dynamodb.Table('FantasyBaseball-DailyPuzzle')
roster_tbl  = dynamodb.Table('FantasyBaseball-RosterData')

MLB_API = 'https://statsapi.mlb.com/api/v1'
YEAR    = 2026
MST     = timezone(timedelta(hours=-7))


def yesterday_mst() -> str:
    return (datetime.now(MST) - timedelta(days=1)).strftime('%Y-%m-%d')


def get_game_pks(date_str: str) -> list:
    r = requests.get(f'{MLB_API}/schedule', params={'sportId': 1, 'date': date_str}, timeout=10)
    r.raise_for_status()
    pks = []
    for d in r.json().get('dates', []):
        for g in d.get('games', []):
            if g.get('status', {}).get('abstractGameState') == 'Final':
                pks.append(g['gamePk'])
    return pks


def get_boxscore(pk: int) -> dict:
    r = requests.get(f'{MLB_API}/game/{pk}/boxscore', timeout=10)
    r.raise_for_status()
    return r.json()


def parse_ip(ip_str) -> float:
    try:
        parts = str(ip_str).split('.')
        return int(parts[0]) + (int(parts[1]) / 3 if len(parts) > 1 and parts[1] else 0)
    except Exception:
        return 0.0


def score_batting(s: dict) -> float:
    if s.get('atBats', 0) < 1:
        return 0.0
    return (
        s.get('homeRuns', 0)    * 5   +
        s.get('rbi', 0)         * 1.5 +
        s.get('runs', 0)        * 1   +
        s.get('hits', 0)        * 0.5 +
        s.get('stolenBases', 0) * 3   +
        s.get('doubles', 0)     * 0.5 +
        s.get('triples', 0)     * 1.5
    )


def score_pitching(s: dict) -> float:
    ip = parse_ip(s.get('inningsPitched', '0'))
    if ip < 3:
        return 0.0
    return (
        ip                          * 2   +
        s.get('strikeOuts', 0)      * 1.5 +
        s.get('earnedRuns', 0)      * -2  +
        s.get('wins', 0)            * 4
    )


def extract_players(bs: dict) -> list:
    players = []
    for side in ('home', 'away'):
        team_name = bs.get('teams', {}).get(side, {}).get('team', {}).get('name', '')
        for pid, pd in bs.get('teams', {}).get(side, {}).get('players', {}).items():
            name = pd.get('person', {}).get('fullName', '')
            if not name:
                continue
            pos  = pd.get('position', {}).get('abbreviation', '')
            bat  = pd.get('stats', {}).get('batting', {})
            pit  = pd.get('stats', {}).get('pitching', {})
            bs_  = score_batting(bat)
            ps_  = score_pitching(pit)
            total = max(bs_, ps_)
            if total <= 0:
                continue
            players.append({
                'name':      name,
                'mlbTeam':   team_name,
                'pos':       pos,
                'score':     total,
                'isPitcher': ps_ >= bs_ and ps_ > 0,
                'batting':   bat,
                'pitching':  pit,
            })
    return players


def format_stat_line(p: dict) -> str:
    if p['isPitcher']:
        pit   = p['pitching']
        parts = [
            f"{pit.get('inningsPitched','0')} IP",
            f"{pit.get('hits',0)} H",
            f"{pit.get('earnedRuns',0)} ER",
            f"{pit.get('strikeOuts',0)} K",
        ]
        if pit.get('baseOnBalls', 0): parts.append(f"{pit['baseOnBalls']} BB")
        if pit.get('wins', 0):        parts.append("W")
        return ', '.join(parts)
    else:
        bat   = p['batting']
        h, ab = bat.get('hits', 0), bat.get('atBats', 0)
        parts = [f"{h}-for-{ab}"]
        if bat.get('homeRuns', 0):    parts.append(f"{bat['homeRuns']} HR")
        if bat.get('rbi', 0):         parts.append(f"{bat['rbi']} RBI")
        if bat.get('runs', 0):        parts.append(f"{bat['runs']} R")
        if bat.get('stolenBases', 0): parts.append(f"{bat['stolenBases']} SB")
        if bat.get('baseOnBalls', 0): parts.append(f"{bat['baseOnBalls']} BB")
        return ', '.join(parts)


def get_roster_map() -> dict:
    resp = roster_tbl.query(KeyConditionExpression=Key('Year').eq(YEAR))
    out  = {}
    for item in resp.get('Items', []):
        sk = item.get('TeamName', '')
        if sk.startswith('#'):
            continue
        for p in json.loads(item.get('Players', '[]')):
            out[p['name']] = {
                'teamName': sk,
                'pos':      p.get('pos', 'Util'),
                'rank':     p.get('rank'),
            }
    return out


def lambda_handler(event, context):
    date_str = event.get('date') or yesterday_mst()
    logger.info(f"Building puzzle for {date_str}")

    try:
        if puzzle_tbl.get_item(Key={'Date': date_str}).get('Item'):
            return {'statusCode': 200, 'body': f'Puzzle already exists for {date_str}'}

        roster_map = get_roster_map()
        logger.info(f"Loaded {len(roster_map)} rostered players")

        pks = get_game_pks(date_str)
        logger.info(f"Found {len(pks)} final games on {date_str}")
        if not pks:
            return {'statusCode': 200, 'body': f'No completed games for {date_str}'}

        all_players = []
        for pk in pks:
            try:
                all_players.extend(extract_players(get_boxscore(pk)))
            except Exception as e:
                logger.warning(f"Game {pk}: {e}")

        all_players.sort(key=lambda x: -x['score'])
        logger.info(f"Top performer: {all_players[0]['name']} ({all_players[0]['score']:.1f}) [{all_players[0]['mlbTeam']}]" if all_players else "No players found")

        puzzle_player = None
        roster_info   = None
        for p in all_players:
            if p['name'] in roster_map:
                puzzle_player = p
                roster_info   = roster_map[p['name']]
                break

        if not puzzle_player:
            logger.warning("No rostered player in top performers")
            return {'statusCode': 200, 'body': 'No rostered player found in top performers'}

        managers  = sorted({v['teamName'] for v in roster_map.values()})
        stat_line = format_stat_line(puzzle_player)
        rank      = roster_info.get('rank')

        puzzle_tbl.put_item(Item={
            'Date':       date_str,
            'PlayerName': puzzle_player['name'],
            'MlbTeam':    puzzle_player['mlbTeam'],
            'FantasyTeam': roster_info['teamName'],
            'Position':   roster_info['pos'],
            'Rank':       str(rank) if rank else '',
            'StatLine':   stat_line,
            'IsPitcher':  puzzle_player['isPitcher'],
            'Managers':   json.dumps(managers),
            'Timestamp':  datetime.utcnow().isoformat() + 'Z',
        })

        msg = f"{puzzle_player['name']} ({puzzle_player['mlbTeam']}) owned by {roster_info['teamName']} — {stat_line}"
        logger.info(f"Puzzle created: {msg}")
        return {'statusCode': 200, 'body': msg}

    except Exception as e:
        logger.error(f"pull_daily_puzzle FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
