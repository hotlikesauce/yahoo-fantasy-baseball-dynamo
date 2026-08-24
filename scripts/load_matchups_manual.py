"""
Manually load a week of matchup results into FantasyBaseball-Matchups2026.

Companion to load_weekly_stats_manual.py, for when the Yahoo API is unavailable.
This table is what compute_luck_analysis gates on: a week missing here is skipped
entirely, even when its weekly_stats exist.

Reads the same JSON file as the stats loader, using its "_matchups" key:
    "_matchups": [[tn_a, tn_b, cats_won_a, cats_won_b, ties], ...]   # 6 pairs

Scores are category wins. Ties are implied by the schema (Score + OpponentScore
+ ties = 12) and are not stored as their own field, matching existing rows.

Usage:
    python scripts/load_matchups_manual.py week18.json           # preview only
    python scripts/load_matchups_manual.py week18.json --write
"""

import argparse
import json
import sys

import boto3

YEAR = 2026
NUM_CATS = 12

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
trends = dynamodb.Table('FantasyBaseball-SeasonTrends')
matchups = dynamodb.Table('FantasyBaseball-Matchups2026')


def load_team_names():
    resp = trends.get_item(Key={'TeamNumber': '0', 'DataType#Week': 'team_names#current'})
    item = resp.get('Item')
    if not item or 'Teams' not in item:
        sys.exit("ERROR: team_names#current not found")
    return {str(tn): name for tn, name in item['Teams'].items()}


def build_items(payload, names):
    week = int(payload['week'])
    pairs = payload.get('_matchups')
    if not pairs:
        sys.exit("ERROR: input JSON has no '_matchups' key")

    seen = set()
    items = []
    for a, b, sa, sb, ties in pairs:
        a, b = str(a), str(b)
        for tn in (a, b):
            if tn not in names:
                sys.exit(f"ERROR: unknown team number {tn}")
            if tn in seen:
                sys.exit(f"ERROR: team {tn} ({names[tn]}) appears in two matchups")
            seen.add(tn)
        if sa + sb + ties != NUM_CATS:
            sys.exit(f"ERROR: {names[a]} vs {names[b]} scores {sa}-{sb} with {ties} ties "
                     f"sum to {sa + sb + ties}, expected {NUM_CATS}")
        # Item shape and attribute types mirror lambda/functions/pull_weekly_matchups.py
        # (the Lambda that normally writes this table) - check there, not a scan dump,
        # when anything changes. Week/Score/OpponentScore/Year are numeric (Week is the
        # numeric partition key); TeamNumber and OpponentTeamNumber are strings.
        items.append({'Week': week, 'TeamNumber': a, 'Year': YEAR,
                      'Team': names[a], 'Opponent': names[b],
                      'OpponentTeamNumber': b,
                      'Score': sa, 'OpponentScore': sb})
        items.append({'Week': week, 'TeamNumber': b, 'Year': YEAR,
                      'Team': names[b], 'Opponent': names[a],
                      'OpponentTeamNumber': a,
                      'Score': sb, 'OpponentScore': sa})

    missing = set(names) - seen
    if missing:
        sys.exit(f"ERROR: teams not in any matchup: "
                 f"{', '.join(f'{tn}={names[tn]}' for tn in sorted(missing, key=int))}")
    items.sort(key=lambda i: int(i['TeamNumber']))
    return week, items


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('input')
    ap.add_argument('--write', action='store_true')
    args = ap.parse_args()

    with open(args.input, encoding='utf-8') as f:
        payload = json.load(f)

    names = load_team_names()
    week, items = build_items(payload, names)

    print(f"\nWeek {week} - {len(items)} rows ({len(items) // 2} matchups)\n")
    for item in items:
        if int(item['TeamNumber']) < int(item['OpponentTeamNumber']):
            ties = NUM_CATS - int(item['Score']) - int(item['OpponentScore'])
            tie_note = f"  ({ties} tie)" if ties else ""
            print(f"  {item['Team'][:26]:<26} {item['Score']:>2} - {item['OpponentScore']:<2} "
                  f"{item['Opponent'][:26]:<26}{tie_note}")
    print()

    if not args.write:
        print("Preview only - re-run with --write to upload.")
        return

    existing = matchups.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('Week').eq(week)
    ).get('Items', [])
    if existing:
        print(f"NOTE: week {week} already has {len(existing)} rows - they will be overwritten.")

    with matchups.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
    print(f"Wrote {len(items)} matchup rows for week {week}.")
    print("Next: invoke the compute-luck-analysis Lambda.")


if __name__ == '__main__':
    main()
