"""
Scheduled snapshot of the playoff race.

EventBridge Scheduler fires this at midnight, noon, 3pm, 6pm and 9pm league
time (America/Denver, DST handled by the scheduler's timezone). Each run
scrapes the public Yahoo league page, runs the same clinch maths and Monte
Carlo the site uses, and writes two rows to FantasyBaseball-PlayoffOdds:

  Slot = "current"          the whole live picture, for the page to render
  Slot = "2026-08-25T18"    one immutable point on the odds-over-time chart

docs/live_standings_2026.html is a static shell that fetches both of those from
serve-playoff-odds at load time, so nothing here renders or publishes HTML.

The slot row is written once per boundary and never rewritten, so a retry, a
catch-up run or a manual invoke cannot bend a point that is already on the
chart.
"""

import os
import json
from decimal import Decimal

import boto3

import playoff_core as pc

TABLE = os.environ.get('ODDS_TABLE', 'FantasyBaseball-PlayoffOdds')
REGION = os.environ.get('AWS_REGION', 'us-west-2')


def league_id():
    lid = os.environ.get('YAHOO_LEAGUE_ID_2026', '').strip()
    if not lid:
        raise RuntimeError('YAHOO_LEAGUE_ID_2026 is not set on the function')
    return lid


def to_dynamo(obj):
    """DynamoDB has no float type - round-trip through Decimal."""
    return json.loads(json.dumps(obj, ensure_ascii=False), parse_float=Decimal)


def lambda_handler(event, context):
    year = str(pc.YEAR)
    table = boto3.resource('dynamodb', region_name=REGION).Table(TABLE)

    r = pc.collect(league_id())
    teams, spots = r['teams'], r['spots']

    # ---- who the chart follows, decided once and then left alone ----------
    meta_row = table.get_item(Key={'Year': year, 'Slot': 'meta'}).get('Item') or {}
    tracked = [int(t) for t in meta_row.get('tracked', [])]
    if not tracked:
        tracked = pc.pick_tracked(teams, spots)
        table.put_item(Item=to_dynamo({
            'Year': year, 'Slot': 'meta',
            'tracked': tracked,
            'slot_hours': list(pc.SLOT_HOURS),
            'spots': spots,
        }))

    order = sorted(teams.values(), key=lambda t: -(t['pts'] + t['live']))
    table.put_item(Item=to_dynamo({
        'Year': year, 'Slot': 'current',
        'week': r['week'],
        'status': r['meta']['status'],
        'yahoo_updated': r['meta']['yahoo_updated'],
        'league_name': r['meta']['league_name'],
        'playoff_spots': spots,
        'playoff_format': r['settings']['raw'],
        'last_regular_week': r['last_regular_week'],
        'week_progress': round(r['progress'], 4),
        'sims': pc.SIMS,
        'source': r['base'],
        'scraped_at': pc.now_local().isoformat(timespec='seconds'),
        'teams': [{k: v for k, v in t.items() if k != 'seed_counts'} for t in order],
        'matchups': r['matchups'],
    }))

    # ---- one immutable point per boundary --------------------------------
    slot = pc.current_slot()
    key = slot.strftime('%Y-%m-%dT%H')
    written = False
    try:
        table.put_item(
            Item=to_dynamo({
                'Year': year, 'Slot': key,
                'label': pc.slot_label(slot),
                'recorded_at': pc.now_local().isoformat(timespec='seconds'),
                'week': r['week'],
                # raw, not the table's capped value - see chart_value()
                'odds': {str(tid): round(pc.chart_value(t), 4)
                         for tid, t in teams.items()},
            }),
            ConditionExpression='attribute_not_exists(#s)',
            ExpressionAttributeNames={'#s': 'Slot'},
        )
        written = True
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        pass    # this boundary is already on the chart - leave it as it stands

    summary = {
        'slot': key,
        'snapshot_written': written,
        'week': r['week'],
        'status': r['meta']['status'],
        'tracked': [teams[t]['name'] for t in tracked if t in teams],
        'odds': {teams[t]['name']: round(pc.chart_value(teams[t]), 3)
                 for t in tracked if t in teams},
    }
    print(json.dumps(summary, ensure_ascii=False))
    return {'statusCode': 200, 'body': json.dumps(summary, ensure_ascii=False)}
