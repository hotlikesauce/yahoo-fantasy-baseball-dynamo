"""
Lambda: Pull live scoreboard (matchups) from Yahoo Fantasy API every 15 minutes.
Updates FantasyBaseball-SeasonTrends with current week matchup scores.
Triggered by: CloudWatch Events (rate: 15 minutes)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def extract_team_data(team_list):
    """Extract team info from matchup response structure."""
    team_id = None
    team_name = None

    basic_info = team_list[0]  # List of basic info dicts
    for item in basic_info:
        if not isinstance(item, dict):
            continue
        if 'team_id' in item:
            team_id = str(item['team_id'])
        elif 'name' in item:
            team_name = item['name']

    # Get points
    stats_wrapper = team_list[1]
    team_points = stats_wrapper.get('team_points', {})
    points = int(team_points.get('total', 0))

    return {'id': team_id, 'name': team_name, 'points': points}


def lambda_handler(event, context) -> Dict[str, Any]:
    """
    Main Lambda handler for pulling live scoreboard.
    """
    try:
        yfl.log_execution("pull_live_scoreboard", "START")

        # Get credentials
        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        # Get fresh access token
        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        # Build league key and fetch scoreboard
        league_key = yfl.get_league_key(2026, league_id)
        scoreboard_data = yfl.api_get(token, f"league/{league_key}/scoreboard")

        if not scoreboard_data:
            raise ValueError("Failed to fetch scoreboard from API")

        # Parse scoreboard
        items_to_write = []
        try:
            fc = scoreboard_data.get('fantasy_content', {})
            league = fc.get('league', [])
            if not league or len(league) < 2:
                raise ValueError("Invalid scoreboard response structure")

            scoreboard = league[1].get('scoreboard', {})
            matchups_wrapper = scoreboard.get('0', {})
            matchups = matchups_wrapper.get('matchups', {})

            # Get current week
            current_week = int(scoreboard.get('week', 1))

            # Process each matchup
            for matchup_idx in matchups.keys():
                if matchup_idx == 'count':
                    continue

                matchup_wrapper = matchups[matchup_idx]
                matchup = matchup_wrapper.get('matchup', {})
                teams_wrapper = matchup.get('0', {})
                teams = teams_wrapper.get('teams', {})

                if '0' not in teams or '1' not in teams:
                    continue

                team_a_list = teams['0']['team']
                team_b_list = teams['1']['team']

                team_a = extract_team_data(team_a_list)
                team_b = extract_team_data(team_b_list)

                if not team_a['id'] or not team_b['id'] or not team_a['name'] or not team_b['name']:
                    continue

                # Create bidirectional records
                for (team, opponent) in [(team_a, team_b), (team_b, team_a)]:
                    item = {
                        'TeamNumber': team['id'],
                        'DataTypeWeek': f"weekly_results#{current_week:02d}",
                        'YearDataType': f"2026#weekly_results",
                        'Year': 2026,
                        'Week': current_week,
                        'Team': team['name'],
                        'Opponent': opponent['name'],
                        'Score': team['points'],
                        'Opponent_Score': opponent['points'],
                        'Timestamp': datetime.utcnow().isoformat(),
                    }
                    items_to_write.append(item)

            # Batch write to DynamoDB
            if items_to_write:
                count = yfl.batch_write_items('FantasyBaseball-SeasonTrends', items_to_write)
                yfl.log_execution("pull_live_scoreboard", "SUCCESS", f"Wrote {count} scoreboard records for week {current_week}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'Updated {count} scoreboard records', 'week': current_week})
                }
            else:
                yfl.log_execution("pull_live_scoreboard", "WARNING", "No scoreboard data parsed")
                return {'statusCode': 204, 'body': json.dumps({'message': 'No scoreboard to update'})}

        except Exception as e:
            yfl.log_execution("pull_live_scoreboard", "ERROR", str(e))
            raise

    except Exception as e:
        yfl.log_execution("pull_live_scoreboard", "FAILED", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
