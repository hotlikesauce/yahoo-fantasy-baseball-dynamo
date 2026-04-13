"""
Lambda: Serve live 2027 draft capital as JSON.
Pulls current pick ownership from Yahoo Fantasy API (includes traded picks).
Triggered by: Lambda Function URL (HTTPS) - called from browser on page load.
"""

import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'max-age=120',
}

TOTAL_ROUNDS  = 22
NUM_TEAMS     = 12
KEEPER_ROUNDS = 2
STANDARD_CAPITAL = 1000
DECAY = 0.98

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]

DRAFT_ROUNDS = TOTAL_ROUNDS - KEEPER_ROUNDS   # 20 tradeable rounds
TOTAL_PICKS  = DRAFT_ROUNDS * NUM_TEAMS        # 240

total_raw = sum(DECAY ** i for i in range(TOTAL_PICKS))
SCALE = STANDARD_CAPITAL * NUM_TEAMS / total_raw


def overall_pick(rnd, pos):
    """Tradeable round + pick position → overall pick number (1-240)."""
    return (rnd - KEEPER_ROUNDS - 1) * NUM_TEAMS + pos


def pick_value(overall):
    raw = SCALE * DECAY ** (overall - 1)
    return round(raw) if raw >= 0.5 else 0


def avg_round_value(rnd):
    """Average pick value across all 12 slots in a tradeable round."""
    base = overall_pick(rnd, 1)
    return sum(pick_value(base + i) for i in range(NUM_TEAMS)) / NUM_TEAMS


# Sample values for display
p1_val   = pick_value(1)
p12_val  = pick_value(12)
p49_val  = pick_value(overall_pick(7, 1))
p240_val = pick_value(240)


def compute_capital(pick_counts):
    """Given a 22-element pick_counts list, return draft capital score."""
    return round(sum(
        pick_counts[rnd - 1] * avg_round_value(rnd)
        for rnd in range(KEEPER_ROUNDS + 1, TOTAL_ROUNDS + 1)
    ))


def build_result(pick_counts_by_team):
    """Build the full JSON result given {team_name: [picks_per_round]}."""
    team_data = []
    for name, picks in pick_counts_by_team.items():
        capital = compute_capital(picks)
        total_picks = sum(picks)
        extra_high = sum(max(0, picks[i] - 1) for i in range(10))
        traded_away = sum(1 for c in picks if c == 0)
        team_data.append({
            'name': name,
            'picks': picks,
            'totalPicks': total_picks,
            'capital': capital,
            'capitalVsStd': capital - STANDARD_CAPITAL,
            'extraHigh': extra_high,
            'tradedAway': traded_away,
        })

    team_data.sort(key=lambda x: x['capital'], reverse=True)

    # Assign colors sorted by capital rank
    for i, t in enumerate(team_data):
        t['color'] = COLORS[i % len(COLORS)]

    return {
        'generated': datetime.utcnow().isoformat() + 'Z',
        'totalRounds': TOTAL_ROUNDS,
        'keeperRounds': KEEPER_ROUNDS,
        'standardCapital': STANDARD_CAPITAL,
        'pickValues': {
            'p1': p1_val, 'p12': p12_val, 'p49': p49_val, 'p240': p240_val,
        },
        'teams': team_data,
    }


def fetch_picks_from_yahoo(token, league_key):
    """
    Pull draft pick ownership from Yahoo Fantasy API.
    Returns {team_name: [pick_count_per_round]} for all 22 rounds.
    """
    # Fetch all teams with draftpicks sub-resource
    resp = yfl.api_get(token, f"league/{league_key}/teams;out=draftpicks")
    if not resp:
        raise ValueError("Failed to fetch teams/draftpicks from Yahoo API")

    fc = resp.get('fantasy_content', {})
    league = fc.get('league', [])
    if not league or len(league) < 2:
        raise ValueError("Unexpected league response structure")

    teams_data = league[1].get('teams', {})
    if not teams_data:
        raise ValueError("No teams found in response")

    # Initialize pick counts: all teams start with 0 picks in all rounds
    # We'll tally picks per team per round
    pick_counts = {}  # team_name -> [count] * TOTAL_ROUNDS

    for key, val in teams_data.items():
        if key == 'count':
            continue

        team_wrapper = val.get('team', [])
        if not team_wrapper or len(team_wrapper) < 2:
            continue

        # Parse team name
        basic_info = team_wrapper[0]
        team_name = None
        for item in basic_info:
            if isinstance(item, dict) and 'name' in item:
                team_name = item['name']
                break

        if not team_name:
            continue

        # Initialize this team's picks to all zeros
        counts = [0] * TOTAL_ROUNDS

        # Parse draft picks — Yahoo returns them under team_wrapper[1]['draft_picks']
        # or sometimes in a sub-key. Handle both structures.
        picks_section = None
        for section in team_wrapper:
            if isinstance(section, dict):
                if 'draft_picks' in section:
                    picks_section = section['draft_picks']
                    break

        if picks_section is None:
            # Try alternate structure: team_wrapper[1] is the picks dict
            if len(team_wrapper) > 1 and isinstance(team_wrapper[1], dict):
                picks_section = team_wrapper[1].get('draft_picks', {})

        if picks_section:
            picks_list = picks_section.get('draft_picks', [])
            if not picks_list:
                # Yahoo sometimes nests differently
                picks_list = picks_section if isinstance(picks_section, list) else []

            for pick_entry in picks_list:
                if not isinstance(pick_entry, dict):
                    continue
                pick = pick_entry.get('draft_pick', pick_entry)
                rnd = pick.get('round')
                if rnd is None:
                    continue
                try:
                    rnd = int(rnd)
                except (ValueError, TypeError):
                    continue
                if 1 <= rnd <= TOTAL_ROUNDS:
                    counts[rnd - 1] += 1

        pick_counts[team_name] = counts

    if not pick_counts:
        raise ValueError("No pick data parsed from Yahoo API response")

    logger.info(f"Fetched picks for {len(pick_counts)} teams")
    return pick_counts


def lambda_handler(event, context) -> Dict[str, Any]:
    try:
        # Handle OPTIONS preflight
        if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
            return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

        yfl.log_execution("serve_draft_capital", "START")

        secrets = yfl.get_secrets()
        league_id = secrets.get('YAHOO_LEAGUE_ID_2026')
        if not league_id:
            raise ValueError("YAHOO_LEAGUE_ID_2026 not found in secrets")

        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get access token")

        league_key = yfl.get_league_key(2026, league_id)

        pick_counts = fetch_picks_from_yahoo(token, league_key)
        result = build_result(pick_counts)

        yfl.log_execution("serve_draft_capital", "SUCCESS",
                          f"{len(result['teams'])} teams")

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps(result),
        }

    except Exception as e:
        logger.error(f"serve_draft_capital FAILED: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': str(e)}),
        }
