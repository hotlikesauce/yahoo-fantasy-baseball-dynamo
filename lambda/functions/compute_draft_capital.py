"""
Lambda: Compute 2027 draft pick ownership from the Yahoo Fantasy API.

We rebuild every team's pick ownership from the official trade ledger
(transactions;type=trade) rather than scraping the showtradedpicks web page —
that page redirects to login (302/401) for an API OAuth token and never worked.

Each team starts owning its own pick in every round. Each successful trade's
`picks` move a pick (identified by original_team + round) to its destination
team; applying them in chronological order yields current ownership.

Team identity is the team NUMBER (from the .t.N team key); display names come
from the team_names#current table so they're always current.

Stores computed draft capital in DynamoDB.
Triggered by: CloudWatch Events - run nightly (cron(0 8 * * ? *))
"""

import json
import logging
import re
import boto3
from datetime import datetime

import yahoo_fantasy_lib as yfl

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

NUM_TEAMS     = 12
TOTAL_ROUNDS  = 22
KEEPER_ROUNDS = 2
STANDARD_CAPITAL = 1000
DECAY = 0.98

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]

DRAFT_ROUNDS = TOTAL_ROUNDS - KEEPER_ROUNDS
TOTAL_PICKS  = DRAFT_ROUNDS * NUM_TEAMS
total_raw = sum(DECAY ** i for i in range(TOTAL_PICKS))
SCALE = STANDARD_CAPITAL * NUM_TEAMS / total_raw


def overall_pick(rnd, pos):
    return (rnd - KEEPER_ROUNDS - 1) * NUM_TEAMS + pos


def pick_value(overall):
    raw = SCALE * DECAY ** (overall - 1)
    return round(raw) if raw >= 0.5 else 0


def avg_round_value(rnd):
    base = overall_pick(rnd, 1)
    return sum(pick_value(base + i) for i in range(NUM_TEAMS)) / NUM_TEAMS


p1_val   = pick_value(1)
p12_val  = pick_value(12)
p49_val  = pick_value(overall_pick(7, 1))
p240_val = pick_value(240)


def team_number_from_key(team_key):
    """'469.l.8614.t.8' -> '8'."""
    m = re.search(r'\.t\.(\d+)$', team_key or '')
    return m.group(1) if m else None


def get_live_team_names():
    """{team_number: current_team_name} from the live standings meta row."""
    meta = table.get_item(Key={'TeamNumber': '0', 'DataType#Week': 'team_names#current'})
    item = meta.get('Item')
    if item and item.get('Teams'):
        return {str(tn): name for tn, name in item['Teams'].items()}
    return {}


def fetch_pick_moves(token, league_key):
    """Return [(timestamp, original_tn, round, dest_tn)] for every traded pick."""
    data = yfl.api_get(token, f"league/{league_key}/transactions;type=trade")
    if not data:
        raise ValueError("Failed to fetch trade transactions")

    txns = data['fantasy_content']['league'][1]['transactions']
    count = int(txns.get('count', 0))
    moves = []
    for i in range(count):
        tx = txns[str(i)]['transaction']
        head = tx[0] if isinstance(tx, list) else tx
        if head.get('status') != 'successful':
            continue
        picks = head.get('picks')
        if not picks:
            continue
        ts = int(head.get('timestamp', 0))
        items = picks if isinstance(picks, list) else [v for k, v in picks.items() if k != 'count']
        for entry in items:
            p = entry.get('pick', {})
            orig = team_number_from_key(p.get('original_team_key'))
            dest = team_number_from_key(p.get('destination_team_key'))
            try:
                rnd = int(p.get('round'))
            except (TypeError, ValueError):
                continue
            if orig and dest and 1 <= rnd <= TOTAL_ROUNDS:
                moves.append((ts, orig, rnd, dest))
    return moves


def ownership_counts(moves):
    """{team_number: [count per round]} after applying all pick moves chronologically."""
    owner = {}
    for tn in (str(i) for i in range(1, NUM_TEAMS + 1)):
        for rnd in range(1, TOTAL_ROUNDS + 1):
            owner[(tn, rnd)] = tn
    for ts, orig, rnd, dest in sorted(moves, key=lambda m: m[0]):
        owner[(orig, rnd)] = dest

    counts = {str(i): [0] * TOTAL_ROUNDS for i in range(1, NUM_TEAMS + 1)}
    for (orig, rnd), own in owner.items():
        if own in counts:
            counts[own][rnd - 1] += 1
    return counts


def compute_capital(picks):
    return round(sum(
        picks[rnd - 1] * avg_round_value(rnd)
        for rnd in range(KEEPER_ROUNDS + 1, TOTAL_ROUNDS + 1)
    ))


def build_result(counts_by_tn, names):
    team_data = []
    for tn, picks in counts_by_tn.items():
        capital = compute_capital(picks)
        team_data.append({
            'tn': tn,
            'name': names.get(tn, f'Team {tn}'),
            'picks': picks,
            'totalPicks': sum(picks),
            'capital': capital,
            'capitalVsStd': capital - STANDARD_CAPITAL,
            'extraHigh': sum(max(0, picks[i] - 1) for i in range(10)),
            'tradedAway': sum(1 for c in picks if c == 0),
        })

    team_data.sort(key=lambda x: x['capital'], reverse=True)
    for i, t in enumerate(team_data):
        t['color'] = COLORS[i % len(COLORS)]

    return {
        'generated': datetime.utcnow().isoformat() + 'Z',
        'totalRounds': TOTAL_ROUNDS,
        'keeperRounds': KEEPER_ROUNDS,
        'standardCapital': STANDARD_CAPITAL,
        'pickValues': {'p1': p1_val, 'p12': p12_val, 'p49': p49_val, 'p240': p240_val},
        'teams': team_data,
    }


def lambda_handler(event, context):
    try:
        secrets = yfl.get_secrets()
        token = yfl.get_access_token(secrets)
        if not token:
            raise ValueError("Failed to get Yahoo OAuth access token")

        league_key = yfl.get_league_key(2026, secrets['YAHOO_LEAGUE_ID_2026'])
        moves = fetch_pick_moves(token, league_key)
        logger.info(f"Applied {len(moves)} traded-pick moves")

        counts = ownership_counts(moves)
        names = get_live_team_names()
        result = build_result(counts, names)

        table.put_item(Item={
            'TeamNumber': '0',
            'DataType#Week': 'computed#draft_capital',
            'DataTypeWeek': 'computed#draft_capital',
            'YearDataType': '2026#computed',
            'Year': 2026,
            'Data': json.dumps(result, ensure_ascii=False),
            'Timestamp': datetime.utcnow().isoformat(),
        })

        logger.info(f"Stored draft capital for {len(result['teams'])} teams")
        return {'statusCode': 200, 'body': f"Computed draft capital: {len(result['teams'])} teams"}

    except Exception as e:
        logger.error(f"compute_draft_capital FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
