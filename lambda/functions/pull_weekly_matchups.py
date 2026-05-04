"""
Lambda: Pull completed week matchup scores from Yahoo API and store in
FantasyBaseball-Matchups2026 (clean 2026-only table).
Runs every Monday at 9:10 AM UTC (after pull-weekly-stats).
Idempotent — safe to re-run; overwrites existing week data.
Triggered by: CloudWatch Events (cron: 10 9 ? * MON *)
"""

import json
import logging
import time
from datetime import datetime
from decimal import Decimal

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

import boto3
dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
matchups_table = dynamodb.Table('FantasyBaseball-Matchups2026')


def pull_week_matchups(token, league_key, week):
    """Pull all matchup scores for a completed week from Yahoo API."""
    data = yfl.api_get(token, f'league/{league_key}/scoreboard;week={week}')
    if not data:
        return []

    try:
        league_arr = data['fantasy_content']['league']
        if len(league_arr) < 2:
            return []

        scoreboard = league_arr[1].get('scoreboard', {})
        matchups = scoreboard.get('0', {}).get('matchups', {})
        results = []

        for idx, val in matchups.items():
            if idx == 'count':
                continue

            matchup = val.get('matchup', {})
            teams = matchup.get('0', {}).get('teams', {})
            if '0' not in teams or '1' not in teams:
                continue

            def parse_team(tlist):
                team_id = name = None
                for item in tlist[0]:
                    if isinstance(item, dict):
                        if 'team_id' in item:
                            team_id = str(item['team_id'])
                        elif 'name' in item:
                            name = item['name']
                pts = int(tlist[1].get('team_points', {}).get('total', 0))
                return team_id, name, pts

            a_id, a_name, a_pts = parse_team(teams['0']['team'])
            b_id, b_name, b_pts = parse_team(teams['1']['team'])

            if not a_id or not b_id:
                continue

            results.append({'team_id': a_id, 'team_name': a_name, 'score': a_pts,
                            'opp_id': b_id,  'opp_name': b_name,  'opp_score': b_pts})
            results.append({'team_id': b_id, 'team_name': b_name, 'score': b_pts,
                            'opp_id': a_id,  'opp_name': a_name,  'opp_score': a_pts})

        return results

    except Exception as e:
        logger.error(f'Error parsing week {week} matchups: {e}')
        return []


def lambda_handler(event, context):
    try:
        yfl.log_execution('pull_weekly_matchups', 'START')

        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError('YAHOO_LEAGUE_ID_2026 not found in secrets')

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError('Failed to get access token')

        league_key = yfl.get_league_key(2026, league_id)

        # Get current week
        standings_data = yfl.api_get(token, f'league/{league_key}/standings')
        if not standings_data:
            raise ValueError('Failed to fetch standings')
        current_week = int(standings_data['fantasy_content']['league'][0]['current_week'])

        # Pull all completed weeks (1 through current_week - 1)
        last_completed = current_week - 1
        if last_completed < 1:
            yfl.log_execution('pull_weekly_matchups', 'SKIP', 'No completed weeks yet')
            return {'statusCode': 200, 'body': json.dumps({'message': 'No completed weeks yet'})}

        logger.info(f'Pulling weeks 1-{last_completed}')
        total_records = 0

        with matchups_table.batch_writer() as batch:
            for week in range(1, last_completed + 1):
                results = pull_week_matchups(token, league_key, week)
                if not results:
                    logger.warning(f'No data for week {week}')
                    continue
                for r in results:
                    batch.put_item(Item={
                        'Week':               week,
                        'TeamNumber':         r['team_id'],
                        'Team':               r['team_name'],
                        'Score':              r['score'],
                        'OpponentTeamNumber': r['opp_id'],
                        'Opponent':           r['opp_name'],
                        'OpponentScore':      r['opp_score'],
                        'Year':               2026,
                    })
                total_records += len(results)
                time.sleep(0.2)

        msg = f'Wrote {total_records} records for weeks 1-{last_completed}'
        yfl.log_execution('pull_weekly_matchups', 'SUCCESS', msg)
        return {'statusCode': 200, 'body': json.dumps({'message': msg, 'weeks': last_completed})}

    except Exception as e:
        yfl.log_execution('pull_weekly_matchups', 'FAILED', str(e))
        logger.error(f'pull_weekly_matchups FAILED: {e}', exc_info=True)
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
