"""
Lambda: Pull full season schedule from Yahoo Fantasy API.
Stores manager# and schedule of opponents in FantasyBaseball-Schedule table.
Triggered by: Manual invocation (run once at season start)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context) -> Dict[str, Any]:
    """
    Pull full season schedule and store manager-opponent pairings.
    """
    try:
        yfl.log_execution("pull_schedule", "START")

        # Get credentials
        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        # Get fresh access token
        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        # Build league key
        league_key = yfl.get_league_key(2026, league_id)

        # Fetch full schedule by pulling all weeks
        items_to_write = []
        max_weeks = 21  # Regular season

        for week in range(1, max_weeks + 1):
            scoreboard_data = yfl.api_get(token, f"league/{league_key}/scoreboard;week={week}")

            if not scoreboard_data:
                logger.warning(f"Failed to fetch scoreboard for week {week}")
                continue

            try:
                fc = scoreboard_data.get('fantasy_content', {})
                league = fc.get('league', [])
                if not league or len(league) < 2:
                    continue

                scoreboard = league[1].get('scoreboard', {})
                matchups_wrapper = scoreboard.get('0', {})
                matchups = matchups_wrapper.get('matchups', {})

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

                    # Extract team IDs and names
                    for team_list in [team_a_list, team_b_list]:
                        basic_info = team_list[0]
                        team_id = None
                        team_name = None
                        for item in basic_info:
                            if isinstance(item, dict):
                                if 'team_id' in item:
                                    team_id = str(item['team_id'])
                                elif 'name' in item:
                                    team_name = item['name']

                        if not team_id or not team_name:
                            continue

                    # Store schedule
                    for (team_idx, opponent_idx) in [('0', '1'), ('1', '0')]:
                        team_list = teams[team_idx]['team']
                        opp_list = teams[opponent_idx]['team']

                        team_id = None
                        team_name = None
                        opp_name = None

                        # Parse team
                        for item in team_list[0]:
                            if isinstance(item, dict):
                                if 'team_id' in item:
                                    team_id = str(item['team_id'])
                                elif 'name' in item:
                                    team_name = item['name']

                        # Parse opponent
                        for item in opp_list[0]:
                            if isinstance(item, dict) and 'name' in item:
                                opp_name = item['name']

                        if not team_id or not team_name or not opp_name:
                            continue

                        item = {
                            'TeamNumber': team_id,
                            'Week': week,
                            'Opponent': opp_name,
                            'Year': 2026,
                            'Timestamp': datetime.utcnow().isoformat(),
                        }
                        items_to_write.append(item)

            except Exception as e:
                logger.warning(f"Error processing week {week}: {e}")
                continue

        # Write to schedule table
        if items_to_write:
            count = yfl.batch_write_items('FantasyBaseball-Schedule', items_to_write)
            yfl.log_execution("pull_schedule", "SUCCESS", f"Wrote {count} schedule records")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': f'Stored schedule with {count} records'})
            }
        else:
            yfl.log_execution("pull_schedule", "WARNING", "No schedule data parsed")
            return {'statusCode': 204, 'body': json.dumps({'message': 'No schedule data'})}

    except Exception as e:
        yfl.log_execution("pull_schedule", "FAILED", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
