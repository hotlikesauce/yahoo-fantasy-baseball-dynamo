"""
Manually load a week of team category totals into FantasyBaseball-SeasonTrends.

Fallback for when the Yahoo API is unavailable: paste the week's totals into a
JSON file and load them here instead of running pull_weekly_stats.

Input JSON shape:
    {
      "week": 18,
      "teams": [
        {"team": "M*A*S*H", "R": 47, "H": 88, "HR": 12, "RBI": 39, "SB": 4,
         "OPS": 0.746, "K9": 10.2, "QS": 4, "SVH": 7, "ERA": 3.11,
         "WHIP": 1.09, "TB": 111},
        ...12 teams...
      ]
    }

"team" may be a team name (matched against team_names#current) or a team number.

Usage:
    python scripts/load_weekly_stats_manual.py week18.json           # preview only
    python scripts/load_weekly_stats_manual.py week18.json --write   # write to Dynamo
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3

YEAR = 2026
TABLE_NAME = 'FantasyBaseball-SeasonTrends'
ALL_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K9', 'QS', 'SVH', 'ERA', 'WHIP', 'TB']

# Miss this and Yahoo forfeits all six of your pitching categories.
MIN_IP = 50.0

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table(TABLE_NAME)


def ip_to_float(value):
    """Baseball innings: 55.1 means 55 and one third, not 55.1."""
    whole, _, outs = str(value).strip().partition('.')
    return int(whole) + int(outs or 0) / 3.0


def load_team_names():
    """team_names#current is the canonical team number -> display name map."""
    resp = table.get_item(Key={'TeamNumber': '0', 'DataType#Week': 'team_names#current'})
    item = resp.get('Item')
    if not item or 'Teams' not in item:
        sys.exit("ERROR: team_names#current not found - cannot resolve team numbers")
    return {str(tn): name for tn, name in item['Teams'].items()}


def resolve_team_number(value, tn_to_name):
    """Accept a team number or a team name (exact, then case-insensitive)."""
    s = str(value).strip()
    if s in tn_to_name:
        return s
    name_to_tn = {v: k for k, v in tn_to_name.items()}
    if s in name_to_tn:
        return name_to_tn[s]
    lowered = {v.lower(): k for k, v in tn_to_name.items()}
    if s.lower() in lowered:
        return lowered[s.lower()]
    return None


def build_items(payload, tn_to_name):
    week = int(payload['week'])
    rows = payload['teams']
    items = []
    seen = set()

    for row in rows:
        raw = row.get('team', row.get('Team', row.get('TeamNumber')))
        tn = resolve_team_number(raw, tn_to_name)
        if tn is None:
            sys.exit(f"ERROR: could not match team {raw!r} to a team number.\n"
                     f"Known teams: {json.dumps(tn_to_name, ensure_ascii=False, indent=2)}")
        if tn in seen:
            sys.exit(f"ERROR: team number {tn} ({tn_to_name[tn]}) appears twice in the input")
        seen.add(tn)

        missing = [c for c in ALL_CATS if c not in row]
        if missing:
            sys.exit(f"ERROR: {tn_to_name[tn]} is missing categories: {', '.join(missing)}")

        item = {
            'TeamNumber': tn,
            'DataType#Week': f"weekly_stats#{week:02d}",
            'DataTypeWeek': f"weekly_stats#{week:02d}",
            'YearDataType': f"{YEAR}#weekly_stats",
            'Year': YEAR,
            'Week': week,
            'Team': tn_to_name[tn],
            'Timestamp': datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        }
        for cat in ALL_CATS:
            item[cat] = Decimal(str(row[cat]))

        # IP is not a scoring category, but it decides whether the pitching cats
        # counted at all - store it and the flag the compute Lambdas read.
        raw_ip = row.get('IP', row.get('_ip'))
        if raw_ip is not None:
            item['IP'] = str(raw_ip).strip()
            item['MinIPForfeit'] = ip_to_float(raw_ip) < MIN_IP
        items.append(item)

    missing_teams = set(tn_to_name) - seen
    if missing_teams:
        names = ', '.join(f"{tn}={tn_to_name[tn]}" for tn in sorted(missing_teams, key=int))
        sys.exit(f"ERROR: only {len(seen)} of {len(tn_to_name)} teams supplied. Missing: {names}")

    items.sort(key=lambda i: int(i['TeamNumber']))
    return week, items


def preview(week, items):
    print(f"\nWeek {week} - {len(items)} teams\n")
    header = f"{'TN':>3}  {'Team':<24}" + ''.join(f"{c:>8}" for c in ALL_CATS) + f"{'IP':>8}"
    print(header)
    print('-' * len(header))
    for item in items:
        cells = ''.join(f"{str(item[c]):>8}" for c in ALL_CATS)
        ip = item.get('IP', '-')
        flag = '  <-- under %g IP, forfeits pitching' % MIN_IP if item.get('MinIPForfeit') else ''
        print(f"{item['TeamNumber']:>3}  {item['Team'][:24]:<24}{cells}{str(ip):>8}{flag}")
    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('input', help='JSON file with the week of totals')
    ap.add_argument('--write', action='store_true', help='write to DynamoDB (default is preview only)')
    args = ap.parse_args()

    with open(args.input, encoding='utf-8') as f:
        payload = json.load(f)

    tn_to_name = load_team_names()
    week, items = build_items(payload, tn_to_name)
    preview(week, items)

    if not args.write:
        print("Preview only - re-run with --write to upload.")
        return

    # Must filter on Year. Legacy pre-2026 rows use a different sort key
    # (weekly_stats#18#204) but still carry the unsuffixed weekly_stats#18 in the
    # GSI attribute, so an unfiltered query reports collisions that cannot happen.
    existing = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=boto3.dynamodb.conditions.Key('DataTypeWeek').eq(f"weekly_stats#{week:02d}"),
        FilterExpression=boto3.dynamodb.conditions.Attr('Year').eq(YEAR),
    ).get('Items', [])
    if existing:
        print(f"NOTE: {YEAR} week {week} already has {len(existing)} rows - they will be overwritten.")

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
    print(f"Wrote {len(items)} weekly_stats rows for week {week}.")
    print("Next: invoke the compute_season_trends Lambda to refresh the site data.")


if __name__ == '__main__':
    main()
