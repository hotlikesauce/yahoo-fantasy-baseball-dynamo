"""
Scheduled snapshot of the playoff race.

Every run scrapes the public Yahoo league page AND all six matchup pages, so it
has every category value and each team's innings pitched. It then computes the
whole picture twice:

  now       the scoreboard exactly as Yahoo shows it
  adjusted  with the minimum-innings rule applied - a team short of MIN_IP
            forfeits every pitching category it currently leads

Rows written to FantasyBaseball-PlayoffOdds:

  Slot = "current"              both scenarios in full, for the page to render
  Slot = "meta"                 which teams the chart tracks
  Slot = "2026-08-30T1445"      one immutable tracking point, 15-min slots
                                between 11am and 10pm league time

The tracking row is written once per slot and never rewritten, so a retry or a
manual invoke cannot bend a point already on the chart.
"""

import os
import json
from decimal import Decimal

import boto3

import playoff_core as pc

TABLE = os.environ.get('ODDS_TABLE', 'FantasyBaseball-PlayoffOdds')
REGION = os.environ.get('AWS_REGION', 'us-west-2')

# fields the page needs per team; the rest of the sim state is not worth storing
TEAM_FIELDS = (
    'team_id', 'name', 'manager', 'wins', 'losses', 'ties', 'pts', 'live',
    'floor', 'ceiling', 'projected', 'odds', 'clinched', 'eliminated',
    'magic', 'need_alive', 'remaining', 'opponent', 'games',
    'ip', 'ip_raw', 'ip_short', 'meets_min_ip', 'cats', 'cats_led',
)


def league_id():
    lid = os.environ.get('YAHOO_LEAGUE_ID_2026', '').strip()
    if not lid:
        raise RuntimeError('YAHOO_LEAGUE_ID_2026 is not set on the function')
    return lid


def to_dynamo(obj):
    """DynamoDB has no float type - round-trip through Decimal."""
    return json.loads(json.dumps(obj, ensure_ascii=False), parse_float=Decimal)


def slim(teams):
    out = []
    for t in sorted(teams.values(), key=lambda x: -(x['pts'] + x['live'])):
        out.append({k: t[k] for k in TEAM_FIELDS if k in t})
    return out


def lambda_handler(event, context):
    year = str(pc.YEAR)
    table = boto3.resource('dynamodb', region_name=REGION).Table(TABLE)

    r = pc.collect_both(league_id())
    spots = r['spots']
    now_t = r['scenarios']['now']['teams']
    adj_t = r['scenarios']['adjusted']['teams']

    # ---- who the chart follows ------------------------------------------
    meta_row = table.get_item(Key={'Year': year, 'Slot': 'meta'}).get('Item') or {}
    tracked = [int(t) for t in meta_row.get('tracked', [])]
    if not tracked:
        tracked = pc.pick_tracked(now_t, spots)
        table.put_item(Item=to_dynamo({
            'Year': year, 'Slot': 'meta', 'tracked': tracked,
            'slot_minutes': 15,
            'window': [pc.SNAP_START_HOUR, pc.SNAP_END_HOUR],
            'spots': spots,
        }))

    # ---- the live picture, both ways ------------------------------------
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
        'min_ip': r['min_ip'],
        'batting_cats': pc.BATTING_CATS,
        'pitching_cats': pc.PITCHING_CATS,
        'lower_is_better': sorted(pc.LOWER_IS_BETTER),
        'source': r['base'],
        'scraped_at': pc.now_local().isoformat(timespec='seconds'),
        'now': {'teams': slim(now_t), 'matchups': r['scenarios']['now']['matchups']},
        'adjusted': {'teams': slim(adj_t),
                     'matchups': r['scenarios']['adjusted']['matchups']},
    }))

    # ---- one immutable tracking point per 15-minute slot ----------------
    slot = pc.current_slot_15()
    written, key = False, None
    if slot is not None:
        key = slot.strftime('%Y-%m-%dT%H%M')
        point = {'Year': year, 'Slot': key,
                 'label': pc.slot_label_15(slot),
                 'recorded_at': pc.now_local().isoformat(timespec='seconds'),
                 'week': r['week'], 'now': {}, 'adjusted': {}}
        for name, teams in (('now', now_t), ('adjusted', adj_t)):
            for tid, t in teams.items():
                point[name][str(tid)] = {
                    'odds': round(pc.chart_value(t), 4),
                    'pts': t['pts'] + t['live'],
                    'live': t['live'],
                }
        try:
            table.put_item(Item=to_dynamo(point),
                           ConditionExpression='attribute_not_exists(#s)',
                           ExpressionAttributeNames={'#s': 'Slot'})
            written = True
        except table.meta.client.exceptions.ConditionalCheckFailedException:
            pass

    summary = {
        'slot': key or 'outside 11am-10pm window',
        'point_written': written,
        'week': r['week'],
        'status': r['meta']['status'],
        'short_of_min_ip': [t['name'] for t in now_t.values() if not t['meets_min_ip']],
        'odds': {now_t[t]['name']: {'now': round(now_t[t]['odds'], 2),
                                    'adj': round(adj_t[t]['odds'], 2)}
                 for t in tracked if t in now_t},
    }
    print(json.dumps(summary, ensure_ascii=False))
    return {'statusCode': 200, 'body': json.dumps(summary, ensure_ascii=False)}
