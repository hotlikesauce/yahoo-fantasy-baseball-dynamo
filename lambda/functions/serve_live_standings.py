"""
Lambda: Serve live 2026 standings + scoreboard as JSON.
Called by the HTML page via Lambda Function URL.
Returns standings and current matchup category breakdowns.
Triggered by: Lambda Function URL (HTTPS) - called from browser every 5 min.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Stat ID -> display name
STAT_MAP = {
    '7': 'R', '8': 'H', '12': 'HR', '13': 'RBI', '16': 'SB', '55': 'OPS',
    '49': 'TB', '26': 'ERA', '27': 'WHIP', '57': 'K/9', '83': 'QS', '89': 'SV+H'
}
SKIP_STATS = {'60', '50'}  # H/AB, IP
LOW_STATS = {'TB', 'ERA', 'WHIP'}
STAT_ORDER = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K/9', 'QS', 'SV+H', 'ERA', 'WHIP', 'TB']


def parse_standings(league_arr):
    """Parse standings from Yahoo API response."""
    league_meta = league_arr[0]
    current_week = int(league_meta.get('current_week', 1))

    standings_data = league_arr[1]['standings'][0]['teams']
    standings = []

    for tidx in standings_data:
        if tidx == 'count':
            continue
        team_list = standings_data[tidx]['team']
        basic = team_list[0]
        team_id = team_name = manager = None
        for item in basic:
            if not isinstance(item, dict):
                continue
            if 'team_id' in item:
                team_id = item['team_id']
            elif 'name' in item:
                team_name = item['name']
            elif 'managers' in item:
                mgrs = item['managers']
                if isinstance(mgrs, list):
                    manager = mgrs[0].get('manager', {}).get('nickname', '?')

        ts = team_list[2].get('team_standings', {})
        ot = ts.get('outcome_totals', {})
        w = int(ot.get('wins', 0))
        l = int(ot.get('losses', 0))
        t = int(ot.get('ties', 0))
        rank = int(ts.get('rank') or 99)
        gp = w + l + t
        pts = w + 0.5 * t
        pct = pts / gp if gp > 0 else 0

        standings.append({
            'rank': rank, 'name': team_name, 'manager': manager,
            'wins': w, 'losses': l, 'ties': t,
            'pts': pts, 'pct': pct, 'gp': gp,
        })

    standings.sort(key=lambda r: r['rank'])
    leader_pts = standings[0]['pts'] if standings else 0
    for row in standings:
        row['gb'] = leader_pts - row['pts']

    return standings, current_week


def parse_team_stats(team_list):
    """Extract name, total score, and individual stat values from scoreboard team."""
    name = '?'
    for item in team_list[0]:
        if isinstance(item, dict) and 'name' in item:
            name = item['name']

    pw = team_list[1]
    total = pw.get('team_points', {}).get('total', '0')
    raw_stats = pw.get('team_stats', {}).get('stats', [])

    stats = {}
    for s in raw_stats:
        stat = s['stat']
        sid = str(stat['stat_id'])
        if sid in SKIP_STATS:
            continue
        cat = STAT_MAP.get(sid)
        if not cat:
            continue
        val = stat.get('value')
        if val is None or val == 'None' or val == '':
            val = None
        else:
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = None
        stats[cat] = val

    return {'name': name, 'total': total, 'stats': stats}


def parse_scoreboard(league_arr):
    """Parse scoreboard matchups from Yahoo API response."""
    scoreboard = league_arr[1].get('scoreboard', {})
    sb_week = scoreboard.get('week', 1)
    matchups_raw = scoreboard['0']['matchups']
    matchups = []

    for midx in matchups_raw:
        if midx == 'count':
            continue
        matchup = matchups_raw[midx]['matchup']
        status = matchup.get('status', '?')
        teams = matchup['0']['teams']

        a = parse_team_stats(teams['0']['team'])
        b = parse_team_stats(teams['1']['team'])

        a_wins = b_wins = ties = 0
        cat_results = []
        for cat in STAT_ORDER:
            av = a['stats'].get(cat)
            bv = b['stats'].get(cat)
            if av is None and bv is None:
                cat_results.append({'cat': cat, 'a': av, 'b': bv, 'winner': 'none'})
                continue

            # Treat None as worst possible
            a_val = av if av is not None else (float('inf') if cat in LOW_STATS else -1)
            b_val = bv if bv is not None else (float('inf') if cat in LOW_STATS else -1)

            if cat in LOW_STATS:
                if a_val < b_val:
                    winner = 'a'; a_wins += 1
                elif b_val < a_val:
                    winner = 'b'; b_wins += 1
                else:
                    winner = 'tie'; ties += 1
            else:
                if a_val > b_val:
                    winner = 'a'; a_wins += 1
                elif b_val > a_val:
                    winner = 'b'; b_wins += 1
                else:
                    winner = 'tie'; ties += 1

            cat_results.append({'cat': cat, 'a': av, 'b': bv, 'winner': winner})

        margin = abs(a_wins - b_wins)
        matchups.append({
            'teamA': a['name'], 'teamB': b['name'],
            'scoreA': int(a['total']), 'scoreB': int(b['total']),
            'cats': cat_results, 'margin': margin, 'status': status,
        })

    matchups.sort(key=lambda m: m['margin'], reverse=True)
    return matchups, sb_week


def lambda_handler(event, context) -> Dict[str, Any]:
    """Lambda handler - returns JSON for the live standings page."""
    try:
        yfl.log_execution("serve_live_standings", "START")

        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(2026, league_id)

        # Pull standings
        standings_resp = yfl.api_get(token, f"league/{league_key}/standings")
        if not standings_resp:
            raise ValueError("Failed to fetch standings")
        standings, current_week = parse_standings(standings_resp['fantasy_content']['league'])

        # Pull scoreboard
        sb_resp = yfl.api_get(token, f"league/{league_key}/scoreboard")
        if not sb_resp:
            raise ValueError("Failed to fetch scoreboard")
        matchups, sb_week = parse_scoreboard(sb_resp['fantasy_content']['league'])

        # Merge live matchup results into standings
        # Yahoo standings only update after a week completes, so we project
        # the current in-progress matchup into the W-L-T record
        live_results = {}  # team_name -> 'W', 'L', or 'T'
        for m in matchups:
            if m['scoreA'] > m['scoreB']:
                live_results[m['teamA']] = 'W'
                live_results[m['teamB']] = 'L'
            elif m['scoreB'] > m['scoreA']:
                live_results[m['teamA']] = 'L'
                live_results[m['teamB']] = 'W'
            else:
                live_results[m['teamA']] = 'T'
                live_results[m['teamB']] = 'T'

        for row in standings:
            result = live_results.get(row['name'])
            if result == 'W':
                row['wins'] += 1
            elif result == 'L':
                row['losses'] += 1
            elif result == 'T':
                row['ties'] += 1
            row['gp'] = row['wins'] + row['losses'] + row['ties']
            row['pts'] = row['wins'] + 0.5 * row['ties']
            row['pct'] = row['pts'] / row['gp'] if row['gp'] > 0 else 0

        # Re-sort by points desc, then pct desc
        standings.sort(key=lambda r: (-r['pts'], -r['pct']))
        # Re-rank and recalculate GB
        leader_pts = standings[0]['pts'] if standings else 0
        for i, row in enumerate(standings):
            row['rank'] = i + 1
            row['gb'] = leader_pts - row['pts']

        result = {
            'standings': standings,
            'matchups': matchups,
            'week': sb_week,
            'currentWeek': current_week,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        yfl.log_execution("serve_live_standings", "SUCCESS",
                          f"Week {sb_week}, {len(standings)} teams, {len(matchups)} matchups")

        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Cache-Control': 'max-age=120',
            },
            'body': json.dumps(result),
        }

    except Exception as e:
        yfl.log_execution("serve_live_standings", "FAILED", str(e))
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({'error': str(e)}),
        }
