"""
Lambda: Pull weekly category stats from Yahoo Fantasy API.
Runs every Monday at 3 AM MST (9 AM UTC) to capture previous week's stats.
Updates FantasyBaseball-SeasonTrends with weekly_stats data.
Triggered by: CloudWatch Events (cron: 0 9 ? * MON *)
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# All 12 H2H scoring categories
ALL_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K9', 'QS', 'SVH', 'ERA', 'WHIP', 'TB']


def extract_team_stats(team_list, week: int) -> Dict[str, Any]:
    """Extract team stats from team response."""
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
        return None

    # Extract stats from team_stats
    stats = team_list[1].get('team_stats', {})
    coverage = stats.get('coverage', {})
    stat_categories = coverage.get('stat', [])

    team_stats = {'id': team_id, 'name': team_name, 'week': week, 'stats': {}}

    for stat_dict in stat_categories:
        stat_id = stat_dict.get('stat_id')
        # Map stat_id to category name
        stat_map = {
            '6': 'R',      # Runs
            '8': 'H',      # Hits
            '12': 'HR',    # Home Runs
            '13': 'RBI',   # RBI
            '16': 'SB',    # Stolen Bases
            '50': 'OPS',   # On-base Plus Slugging
            '58': 'K9',    # Strikeouts per 9 IP
            '54': 'QS',    # Quality Starts
            '56': 'SVH',   # Saves + Holds
            '28': 'ERA',   # ERA
            '11': 'WHIP',  # WHIP
            '101': 'TB',   # Total Bases (allowed)
        }

        cat = stat_map.get(stat_id)
        if cat:
            value = stat_dict.get('value')
            if value is not None:
                team_stats['stats'][cat] = float(value)

    return team_stats


def lambda_handler(event, context) -> Dict[str, Any]:
    """
    Pull weekly stats for previous week.
    """
    try:
        yfl.log_execution("pull_weekly_stats", "START")

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

        # Fetch current week info
        standings_data = yfl.api_get(token, f"league/{league_key}/standings")
        if not standings_data:
            raise ValueError("Failed to fetch standings")

        fc = standings_data.get('fantasy_content', {})
        league = fc.get('league', [])
        if not league:
            raise ValueError("Invalid standings response")

        current_week = int(league[0].get('current_week', 1))
        # Pull stats for previous week (if we're past week 1)
        stats_week = max(1, current_week - 1)

        # Fetch team stats for the week
        teams_response = yfl.api_get(token, f"league/{league_key}/teams;coverage_type=week;coverage_value={stats_week}")
        if not teams_response:
            raise ValueError(f"Failed to fetch team stats for week {stats_week}")

        # Parse team stats
        items_to_write = []
        try:
            fc = teams_response.get('fantasy_content', {})
            league = fc.get('league', [])
            if not league or len(league) < 2:
                raise ValueError("Invalid teams response structure")

            teams = league[1].get('teams', {})

            for team_key in teams.keys():
                if team_key == 'count':
                    continue

                team_data = teams[team_key].get('team')
                if not team_data or len(team_data) < 2:
                    continue

                team_stats = extract_team_stats(team_data, stats_week)
                if not team_stats:
                    continue

                # Write to DynamoDB
                item = {
                    'TeamNumber': team_stats['id'],
                    'DataTypeWeek': f"weekly_stats#{stats_week:02d}",
                    'YearDataType': f"2026#weekly_stats",
                    'Year': 2026,
                    'Week': stats_week,
                    'Team': team_stats['name'],
                    'Timestamp': datetime.utcnow().isoformat(),
                }

                # Add all category stats
                for cat in ALL_CATS:
                    if cat in team_stats['stats']:
                        item[cat] = team_stats['stats'][cat]

                items_to_write.append(item)

            # Batch write to DynamoDB
            if items_to_write:
                count = yfl.batch_write_items('FantasyBaseball-SeasonTrends', items_to_write)
                yfl.log_execution("pull_weekly_stats", "SUCCESS", f"Wrote {count} stats records for week {stats_week}")
                return {
                    'statusCode': 200,
                    'body': json.dumps({'message': f'Updated {count} stats records', 'week': stats_week})
                }
            else:
                yfl.log_execution("pull_weekly_stats", "WARNING", "No team stats parsed")
                return {'statusCode': 204, 'body': json.dumps({'message': 'No stats to update'})}

        except Exception as e:
            yfl.log_execution("pull_weekly_stats", "ERROR", str(e))
            raise

    except Exception as e:
        yfl.log_execution("pull_weekly_stats", "FAILED", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
