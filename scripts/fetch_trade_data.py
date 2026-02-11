"""
Fetch roster and player stat data from Yahoo Fantasy API for trade analysis.

Pulls all 12 team rosters with player stats, computes z-score valuations,
and outputs to docs/data/trade_data.json for the trade analyzer HTML page.

Usage:
  python scripts/fetch_trade_data.py

Requires YAHOO_CLIENT_ID and YAHOO_CLIENT_SECRET in .env file.
First run will open a browser for Yahoo OAuth consent.
"""

import json, os, sys, io, math
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / '.env')

from yfpy.query import YahooFantasySportsQuery

# yfpy expects YAHOO_CONSUMER_KEY/SECRET but our .env uses CLIENT_ID/SECRET
consumer_key = os.environ.get('YAHOO_CLIENT_ID') or os.environ.get('YAHOO_CONSUMER_KEY')
consumer_secret = os.environ.get('YAHOO_CLIENT_SECRET') or os.environ.get('YAHOO_CONSUMER_SECRET')

if not consumer_key or not consumer_secret:
    print("ERROR: YAHOO_CLIENT_ID and YAHOO_CLIENT_SECRET must be set in .env")
    sys.exit(1)

LEAGUE_ID = "8614"
GAME_CODE = "mlb"

from team_config import get_manager

# H2H stat categories
# HIGH = higher is better, LOW = lower is better
BATTING_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB']
PITCHING_CATS_HIGH = ['K9', 'QS', 'SVH']
PITCHING_CATS_LOW = ['ERA', 'WHIP']
ALL_CATS = BATTING_CATS + PITCHING_CATS_HIGH + PITCHING_CATS_LOW

# Draft pick value formula (from gen_draft_picks.py)
TOTAL_ROUNDS = 22
def pick_value(round_num):
    return round((TOTAL_ROUNDS + 1 - round_num) ** 1.5)


class SafeEncoder(json.JSONEncoder):
    """Handle bytes, Decimal, and other non-serializable types from Yahoo API."""
    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        if isinstance(obj, Decimal):
            return float(obj)
        if hasattr(obj, '__dict__'):
            return str(obj)
        return super().default(obj)


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def extract_player_data(player_obj):
    """Extract useful fields from a yfpy Player object."""
    p = player_obj

    # Name
    name_obj = getattr(p, 'name', None)
    if name_obj:
        full_name = getattr(name_obj, 'full', '') or ''
    else:
        full_name = str(getattr(p, 'editorial_player_key', ''))

    # Positions
    eligible = getattr(p, 'eligible_positions', None)
    positions = []
    if eligible:
        if isinstance(eligible, list):
            for pos in eligible:
                pos_str = getattr(pos, 'position', str(pos)) if not isinstance(pos, str) else pos
                positions.append(pos_str)
        elif hasattr(eligible, 'position'):
            positions.append(eligible.position)

    display_pos = getattr(p, 'display_position', ','.join(positions) if positions else 'Util')

    # Player key for stats lookup
    player_key = getattr(p, 'player_key', '') or getattr(p, 'player_id', '')

    # Stats
    stats = {}
    player_stats = getattr(p, 'player_stats', None)
    if player_stats:
        stat_list = getattr(player_stats, 'stats', []) or []
        for s in stat_list:
            stat_id = str(getattr(s, 'stat_id', ''))
            stat_val = getattr(s, 'value', '0')
            # Ensure string values (some come back as bytes)
            if isinstance(stat_val, bytes):
                stat_val = stat_val.decode('utf-8', errors='replace')
            stats[stat_id] = stat_val

    # Selected position (where they're slotted in lineup)
    selected_pos_obj = getattr(p, 'selected_position', None)
    selected_pos = ''
    if selected_pos_obj:
        selected_pos = getattr(selected_pos_obj, 'position', '') or ''

    return {
        'name': full_name,
        'player_key': str(player_key),
        'positions': positions,
        'display_position': display_pos,
        'selected_position': selected_pos,
        'raw_stats': stats,
    }


def map_stat_ids(stat_categories):
    """Build stat_id -> {name, display_name, sort_order} from league settings."""
    mapping = {}
    for cat in stat_categories:
        stat_id = str(getattr(cat, 'stat_id', ''))
        name = getattr(cat, 'display_name', '') or getattr(cat, 'name', '')
        sort_order = getattr(cat, 'sort_order', '1')  # 1 = higher is better, 0 = lower
        mapping[stat_id] = {
            'name': name,
            'sort_order': str(sort_order),
        }
    return mapping


def compute_zscores(all_players, stat_id_map):
    """Compute z-scores for each player across all stat categories.

    Returns dict: player_key -> {cat_name: z_score, ..., 'total': sum}
    """
    # Collect stat values per category
    cat_values = {}  # cat_name -> [(player_key, value), ...]

    for player in all_players:
        for stat_id, val in player['raw_stats'].items():
            if stat_id not in stat_id_map:
                continue
            cat_name = stat_id_map[stat_id]['name']
            if cat_name not in ALL_CATS:
                continue
            fval = safe_float(val)
            # Skip pitchers with 0 IP for rate stats
            if cat_name in ('ERA', 'WHIP') and fval == 0:
                continue
            cat_values.setdefault(cat_name, []).append((player['player_key'], fval))

    # Compute mean and std for each category
    cat_stats = {}
    for cat_name, vals in cat_values.items():
        values = [v for _, v in vals]
        if len(values) < 2:
            continue
        mean = sum(values) / len(values)
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))
        if std == 0:
            std = 1
        cat_stats[cat_name] = (mean, std)

    # Compute z-scores per player
    player_zscores = {}  # player_key -> {cat: z, ...}
    for cat_name, vals in cat_values.items():
        if cat_name not in cat_stats:
            continue
        mean, std = cat_stats[cat_name]
        is_lower_better = cat_name in PITCHING_CATS_LOW

        for pkey, val in vals:
            if pkey not in player_zscores:
                player_zscores[pkey] = {}
            if is_lower_better:
                player_zscores[pkey][cat_name] = (mean - val) / std
            else:
                player_zscores[pkey][cat_name] = (val - mean) / std

    # Sum z-scores for total value
    for pkey, zscores in player_zscores.items():
        batting_z = sum(zscores.get(c, 0) for c in BATTING_CATS)
        pitching_z = sum(zscores.get(c, 0) for c in PITCHING_CATS_HIGH + PITCHING_CATS_LOW)
        zscores['batting_value'] = round(batting_z, 2)
        zscores['pitching_value'] = round(pitching_z, 2)
        zscores['total_value'] = round(batting_z + pitching_z, 2)

    return player_zscores


def main():
    print("Connecting to Yahoo Fantasy API...")

    # First, figure out the correct game_id for the current/most recent season
    # During offseason, "current" season may not have rosters yet
    query = YahooFantasySportsQuery(
        league_id=LEAGUE_ID,
        game_code=GAME_CODE,
        yahoo_consumer_key=consumer_key,
        yahoo_consumer_secret=consumer_secret,
        env_file_location=PROJECT_ROOT,
        save_token_data_to_env_file=True,
    )

    # Try to detect the right season - check if current season has data
    try:
        game_info = query.get_current_game_metadata()
        game_id = getattr(game_info, 'game_id', None)
        season = getattr(game_info, 'season', None)
        print(f"  Current game: id={game_id}, season={season}")
    except Exception as e:
        print(f"  Could not get current game info: {e}")
        game_id = None
        season = None

    # 1. Get league settings (stat categories, roster positions)
    print("Fetching league settings...")
    settings = query.get_league_settings()

    stat_categories = getattr(settings, 'stat_categories', None)
    stat_cat_list = []
    stat_id_map = {}
    if stat_categories:
        cats = getattr(stat_categories, 'stats', []) or []
        stat_id_map = map_stat_ids(cats)
        for stat_id, info in stat_id_map.items():
            stat_cat_list.append({
                'stat_id': stat_id,
                'name': info['name'],
                'higher_is_better': info['sort_order'] == '1',
            })

    roster_positions = getattr(settings, 'roster_positions', []) or []
    roster_pos_list = []
    for rp in roster_positions:
        pos = getattr(rp, 'position', '') or ''
        count = int(getattr(rp, 'count', 0) or 0)
        pos_type = getattr(rp, 'position_type', '') or ''
        roster_pos_list.append({'position': pos, 'count': count, 'type': pos_type})

    print(f"  {len(stat_cat_list)} stat categories, {len(roster_pos_list)} roster positions")

    # 2. Get all teams
    print("Fetching teams...")
    teams = query.get_league_teams()
    print(f"  Found {len(teams)} teams")

    # 3. Get rosters with player stats for each team
    print("Fetching rosters and player stats...")
    all_players = []  # flat list for z-score computation
    team_data = []

    for team_obj in teams:
        team_id = int(getattr(team_obj, 'team_id', 0))
        team_name_raw = getattr(team_obj, 'name', '') or f'Team {team_id}'
        # Decode bytes team names from Yahoo API
        if isinstance(team_name_raw, bytes):
            team_name = team_name_raw.decode('utf-8', errors='replace')
        else:
            team_name = str(team_name_raw)

        # Look up manager by team number (stable within a season)
        manager_name = get_manager(2026, team_id)

        print(f"  Team {team_id}: {team_name} ({manager_name})")

        try:
            roster = query.get_team_roster_player_stats(team_id)
        except Exception as e:
            print(f"    ERROR fetching roster: {e}")
            roster = []

        players = []
        for player_obj in (roster or []):
            pdata = extract_player_data(player_obj)
            pdata['team_id'] = team_id
            players.append(pdata)
            all_players.append(pdata)

        team_data.append({
            'team_id': team_id,
            'name': team_name,
            'manager': manager_name,
            'players': players,
        })

    # 4. Compute z-scores across all rostered players
    print("Computing z-score valuations...")
    zscores = compute_zscores(all_players, stat_id_map)

    # Attach z-scores to players and compute team category totals
    for team in team_data:
        category_totals = {cat: 0.0 for cat in ALL_CATS}
        for player in team['players']:
            pkey = player['player_key']
            pz = zscores.get(pkey, {})
            player['value'] = pz.get('total_value', 0)
            player['batting_value'] = pz.get('batting_value', 0)
            player['pitching_value'] = pz.get('pitching_value', 0)

            # Map raw stat IDs to category names for display
            named_stats = {}
            for stat_id, val in player['raw_stats'].items():
                if stat_id in stat_id_map:
                    cat_name = stat_id_map[stat_id]['name']
                    named_stats[cat_name] = safe_float(val)
                    if cat_name in category_totals:
                        category_totals[cat_name] += safe_float(val)
            player['stats'] = named_stats
            del player['raw_stats']  # don't need stat IDs in output

        # Sort players by value descending
        team['players'].sort(key=lambda p: p.get('value', 0), reverse=True)

        # Round category totals
        team['category_totals'] = {k: round(v, 3) for k, v in category_totals.items()}

    # Rank teams per category
    for cat in ALL_CATS:
        is_lower_better = cat in PITCHING_CATS_LOW
        sorted_teams = sorted(team_data, key=lambda t: t['category_totals'].get(cat, 0),
                              reverse=not is_lower_better)
        for rank, team in enumerate(sorted_teams, 1):
            team.setdefault('category_ranks', {})[cat] = rank

    # 5. Build output
    output = {
        'fetched_at': datetime.now().isoformat(),
        'league': {
            'stat_categories': stat_cat_list,
            'roster_positions': roster_pos_list,
        },
        'teams': team_data,
    }

    # Write to docs/data/trade_data.json
    out_path = PROJECT_ROOT / 'docs' / 'data' / 'trade_data.json'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=SafeEncoder)

    print(f"\nWrote {out_path}")
    print(f"  {len(team_data)} teams, {len(all_players)} total players")


if __name__ == '__main__':
    main()
