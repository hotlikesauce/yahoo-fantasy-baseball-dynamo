"""
Scheduled snapshot of the playoff race.

EventBridge Scheduler fires this at midnight, noon, 3pm, 6pm and 9pm league
time (America/Denver, DST handled by the scheduler's timezone). Each run
scrapes the public Yahoo league page, runs the same clinch maths and Monte
Carlo the site uses, and writes two rows to FantasyBaseball-PlayoffOdds:

  Slot = "current"          the whole live picture, for the page to render
  Slot = "2026-08-25T18"    one immutable point on the odds-over-time chart

The slot row is written once per boundary and never rewritten, so a retry, a
catch-up run or a manual invoke cannot bend a point that is already on the
chart. All the parsing and maths lives in playoff_core so this and the local
generator can never drift apart.
"""

import os
import json
from datetime import datetime, timezone
from decimal import Decimal

import boto3

import playoff_core as pc
import github_publish
from playoff_render import render

TABLE = os.environ.get('ODDS_TABLE', 'FantasyBaseball-PlayoffOdds')
REGION = os.environ.get('AWS_REGION', 'us-west-2')
PAGE_PATH = os.environ.get('PAGE_PATH', f'docs/live_standings_{pc.YEAR}.html')
DATA_PATH = os.environ.get('DATA_PATH', f'docs/data/live_standings_{pc.YEAR}.json')


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
                'odds': {str(tid): round(pc.odds_value(t), 2)
                         for tid, t in teams.items()},
            }),
            ConditionExpression='attribute_not_exists(#s)',
            ExpressionAttributeNames={'#s': 'Slot'},
        )
        written = True
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        pass    # this boundary is already on the chart - leave it as it stands

    # ---- render and publish the page ------------------------------------
    # Read the history back rather than reusing what we just wrote: this is the
    # one place that proves the chart the reader sees is the chart the table
    # holds, condition-check and all.
    hist = pc.history_from_dynamo()
    payload = {
        'year': pc.YEAR,
        'week': r['week'],
        'status': r['meta']['status'],
        'yahoo_updated': r['meta']['yahoo_updated'],
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'source': r['base'],
        'playoff_spots': spots,
        'playoff_format': r['settings']['raw'],
        'last_regular_week': r['last_regular_week'],
        'sims': pc.SIMS,
        'week_progress': round(r['progress'], 4),
        'teams': [{k: v for k, v in t.items() if k != 'seed_counts'} for t in order],
        'matchups': r['matchups'],
    }

    published = {}
    if os.environ.get('PUBLISH', '1') == '1':
        page = render(payload, teams, spots, r['week'], r['meta'],
                      r['settings']['raw'], r['matchups'], r['progress'], hist)
        msg = f'live standings: week {r["week"]}, {pc.slot_label(slot)}'
        try:
            published[PAGE_PATH] = github_publish.publish(PAGE_PATH, page, msg)
            published[DATA_PATH] = github_publish.publish(
                DATA_PATH, json.dumps(payload, indent=2, ensure_ascii=False) + '\n', msg)
        except Exception as e:
            # a publishing failure must not cost us the snapshot, which is
            # already safely in DynamoDB by this point
            published['error'] = f'{type(e).__name__}: {e}'
            print(f'PUBLISH FAILED: {published["error"]}')

    summary = {
        'slot': key,
        'snapshot_written': written,
        'week': r['week'],
        'status': r['meta']['status'],
        'points_on_chart': len(hist['points']),
        'published': published,
        'tracked': [teams[t]['name'] for t in tracked if t in teams],
        'odds': {teams[t]['name']: round(pc.odds_value(teams[t]), 2)
                 for t in tracked if t in teams},
    }
    print(json.dumps(summary, ensure_ascii=False))
    return {'statusCode': 200, 'body': json.dumps(summary, ensure_ascii=False)}
