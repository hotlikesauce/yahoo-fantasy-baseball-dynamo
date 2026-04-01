"""
Lambda: Pull live standings from Yahoo Fantasy API every 5 minutes.
Updates FantasyBaseball-SeasonTrends with current week power scores and rankings.
Triggered by: CloudWatch Events (rate: 5 minutes)
"""

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any

# Import shared library (from Lambda layer)
import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context) -> Dict[str, Any]:
    """
    Main Lambda handler for pulling live standings.
    """
    try:
        yfl.log_execution("pull_live_standings", "START")

        # Get credentials
        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        # Get fresh access token
        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        # Build league key and fetch standings
        league_key = yfl.get_league_key(2026, league_id)
        standings_data = yfl.api_get(token, f"league/{league_key}/standings")

        if not standings_data:
            raise ValueError("Failed to fetch standings from API")

        # Parse standings
        items_to_write = []
        try:
            fc = standings_data.get('fantasy_content', {})
            league = fc.get('league', [])
            if not league or len(league) < 2:
                raise ValueError("Invalid standings response structure")

            # Get current week from league metadata
            current_week = int(league[0].get('current_week', 1))

            # standings is a list - first element contains the teams dict
            standings_raw = league[1].get('standings', [])
            if isinstance(standings_raw, list) and standings_raw:
                teams = standings_raw[0].get('teams', {})
            elif isinstance(standings_raw, dict):
                teams = standings_raw.get('0', {}).get('teams', {})
            else:
                teams = {}

            # Process each team
            for team_key in teams.keys():
                if team_key == 'count':
                    continue

                team_data = teams[team_key].get('team', [])
                if not team_data or len(team_data) < 2:
                    continue

                # Extract team info from basic info list
                team_basic = team_data[0]
                team_id = None
                team_name = None
                for info in team_basic:
                    if isinstance(info, dict):
                        if 'team_id' in info:
                            team_id = str(info['team_id'])
                        elif 'name' in info:
                            team_name = info['name']

                if not team_id or not team_name:
                    continue

                # Get rank and points from team_standings
                team_stats = team_data[1]
                rank = None
                points = None

                if isinstance(team_stats, dict):
                    rank = team_stats.get('team_standings', {}).get('rank')
                    points = team_stats.get('team_points', {}).get('total')
                elif isinstance(team_stats, list):
                    for item in team_stats:
                        if isinstance(item, dict):
                            if 'team_standings' in item:
                                rank = item['team_standings'].get('rank')
                            if 'team_points' in item:
                                points = item['team_points'].get('total')

                # Pre-season: rank/points may not exist yet
                if rank is None and points is None:
                    logger.info(f"Team {team_name} (#{team_id}): no rank/points yet (pre-season)")
                    continue

                # Write to DynamoDB
                item = {
                    'TeamNumber': team_id,
                    'DataType#Week': f"power_ranks_live#{current_week}",
                    'DataTypeWeek': f"power_ranks_live#{current_week}",
                    'YearDataType': f"2026#power_ranks_live",
                    'Year': 2026,
                    'Week': current_week,
                    'Team': team_name,
                    'Rank': int(rank) if rank else 0,
                    'Score': Decimal(str(points)) if points else Decimal('0'),
                    'Timestamp': datetime.utcnow().isoformat(),
                }
                items_to_write.append(item)

            # Batch write to DynamoDB
            if items_to_write:
                count = yfl.batch_write_items('FantasyBaseball-SeasonTrends', items_to_write)

                # Also write a team_names#current meta item so the static site
                # can always fetch the latest team names (team names change often)
                team_name_map = {item['TeamNumber']: item['Team'] for item in items_to_write}
                yfl.put_item('FantasyBaseball-SeasonTrends', {
                    'TeamNumber': '0',
                    'DataType#Week': 'team_names#current',
                    'Year': 2026,
                    'Teams': team_name_map,
                    'Timestamp': datetime.utcnow().isoformat(),
                })

                yfl.log_execution("pull_live_standings", "SUCCESS", f"Wrote {count} standings records for week {current_week}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'Updated {count} standings', 'week': current_week})
                }
            else:
                yfl.log_execution("pull_live_standings", "WARNING", "No standings data parsed (pre-season?)")
                return {'statusCode': 204, 'body': json.dumps({'message': 'No standings to update (pre-season?)'})}

        except Exception as e:
            yfl.log_execution("pull_live_standings", "ERROR", str(e))
            raise

    except Exception as e:
        yfl.log_execution("pull_live_standings", "FAILED", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
