"""
Lambda: the championship hub's scheduled jobs.

  capture   Record each finalist's real lineup for the league day just ended,
            plus their acquisitions so far, into the captures table. Week one
            is scored from these, because Yahoo's roster history cannot show a
            player who was dropped later. Never overwritten, never reset.

  reset     Reload the week-one playground from Yahoo, wiping every test move
            made through the hub. Refuses once the date is past week one, and
            refuses if the table is in production mode - two independent guards.

  freeze    The final load for week two: rosters, the whole free agent pool,
            league-wide ownership and week-one acquisitions, in production mode
            with week two's dates. After this, reset can never run again.

Event: {"job": "capture" | "reset" | "nightly" | "freeze"}
       nightly = capture, then reset. {"day": "YYYY-MM-DD"} overrides the
       captured date; {"dry_run": true} reports without writing.
"""
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from champ_hub import capture, config, yahoo_pub as Y

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = 'us-west-2'
DDB = boto3.resource('dynamodb', region_name=REGION)
HUB = DDB.Table(config.TABLE)
CAPTURES = DDB.Table(config.CAPTURE_TABLE)

# Everything a reset is allowed to delete. Tokens and the audit log survive.
MUTABLE_PREFIXES = ('LINEUP#', 'TEAM#', 'PLAYER#', 'CHANGES', 'REQ#')


def daterange(start, end):
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


def current_mode():
    snap = HUB.get_item(Key={'pk': 'META', 'sk': 'SNAPSHOT'}).get('Item') or {}
    return snap.get('mode')


# ── capture ───────────────────────────────────────────────────────────────
def job_capture(day=None, dry_run=False):
    day = day or (date.fromisoformat(config.league_today()) - timedelta(days=1)).isoformat()
    if not (config.WEEK1_START <= day <= config.WEEK1_END):
        return {'skipped': True, 'reason': '{} is not a week-one day'.format(day)}

    stamp = datetime.now(timezone.utc).isoformat()
    report = {'day': day, 'teams': {}}
    for side, tid in config.FINALISTS.items():
        lineup = capture.lineup_for(tid, day)
        adds = capture.acquisitions(tid, config.WEEK1_START, day)
        report['teams'][side] = {'team_id': tid, 'players': len(lineup), 'adds_to_date': len(adds)}
        if dry_run:
            continue
        record = {
            'pk': 'CAPTURE#{}'.format(tid), 'sk': day,
            'lineup': json.dumps(lineup), 'adds_to_date': json.dumps(adds),
            'captured_at': stamp,
        }
        try:
            # The first capture of a day is the one closest to it; keep it.
            CAPTURES.put_item(Item=record, ConditionExpression='attribute_not_exists(pk)')
            report['teams'][side]['stored'] = 'primary'
        except ClientError as e:
            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise
            record['sk'] = '{}#retry#{}'.format(day, stamp)
            CAPTURES.put_item(Item=record)
            report['teams'][side]['stored'] = 'retry (primary already present)'
    return report


# ── load (shared by reset and freeze) ─────────────────────────────────────
def wipe_mutable():
    deleted = 0
    kwargs = {'ProjectionExpression': 'pk, sk'}
    with HUB.batch_writer() as batch:
        while True:
            page = HUB.scan(**kwargs)
            for it in page.get('Items', []):
                if it['pk'].startswith(MUTABLE_PREFIXES):
                    batch.delete_item(Key={'pk': it['pk'], 'sk': it['sk']})
                    deleted += 1
            if 'LastEvaluatedKey' not in page:
                break
            kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']
    return deleted


def put_meta(**fields):
    item = {'pk': 'META', 'sk': 'SNAPSHOT'}
    item.update(fields)
    HUB.put_item(Item=item)


def player_info(p):
    return {
        'name': p.get('name'),
        'team_abbr': p.get('team_abbr'),
        'position_type': p.get('position_type'),
        'eligible_positions': p.get('eligible_positions') or [],
        'display_position': p.get('display_position'),
        'status': p.get('status'),
        'mlb_id': p.get('mlb_id'),
        'mlb_team_id': p.get('mlb_team_id'),
        'stat_side': p.get('stat_side'),
    }


def load(mode, dates, adds_through, dry_run=False):
    """Read everything from Yahoo first, write only once it is all in hand."""
    t0 = time.time()
    slots = Y.roster_positions()

    rosters, unscoreable = {}, {}
    for side, tid in config.FINALISTS.items():
        matched, unmatched = capture.bridge(Y.team_roster(tid))
        # A player with no MLB record cannot score. The load must still succeed -
        # a freeze that crashed on Sept 20 would leave week two with no rosters -
        # so he is loaded without an id and reported loudly instead.
        unscoreable[side] = [u['name'] for u in unmatched]
        rosters[side] = matched + [dict(u, mlb_id=None, mlb_team_id=None) for u in unmatched]

    owned = capture.ownership()
    pool = capture.full_free_agent_pool()
    overlap = [p['player_key'] for p in pool if p['player_key'] in owned]
    if overlap:
        raise RuntimeError('{} players are both free agents and rostered'.format(len(overlap)))

    import champ_hub.players as P
    index, abbr = P.mlb_player_index(), P.team_abbr_to_id()
    pool_matched, pool_unmatched = P.match(pool, index, abbr, use_search=False)
    pool_all = pool_matched + [dict(u, mlb_id=None, mlb_team_id=None) for u in pool_unmatched]

    adds = {side: capture.acquisitions(tid, config.WEEK1_START, adds_through)
            for side, tid in config.FINALISTS.items()}

    summary = {
        'mode': mode, 'dates': [dates[0], dates[-1]], 'roster_slots': slots,
        'finalist_players': {s: len(r) for s, r in rosters.items()},
        'unscoreable_finalist_players': unscoreable,
        'rostered_league_wide': len(owned), 'free_agents': len(pool_all),
        'free_agents_without_mlb_id': len(pool_unmatched),
        'week1_adds': {s: len(a) for s, a in adds.items()},
        'read_seconds': round(time.time() - t0),
    }
    if len(pool_all) < 1000:
        raise RuntimeError('free agent pool looks truncated ({})'.format(len(pool_all)))
    if dry_run:
        return summary

    put_meta(mode=config.MODE_LOADING, loading_since=datetime.now(timezone.utc).isoformat())
    summary['wiped'] = wipe_mutable()

    finalist_side = {p['player_key']: side for side, r in rosters.items() for p in r}
    with HUB.batch_writer() as batch:
        for side, roster in rosters.items():
            batch.put_item(Item={
                'pk': 'TEAM#' + side, 'sk': 'STATE',
                'team_id': config.FINALISTS[side],
                'adds_used': len(adds[side]), 'max_adds': config.MAX_ADDS,
                'week1_adds': json.dumps(adds[side]),
            })
            for p in roster:
                batch.put_item(Item={'pk': 'TEAM#' + side, 'sk': 'ROSTER#' + p['player_key']})
                for d in dates:
                    batch.put_item(Item={'pk': 'LINEUP#{}#{}'.format(side, d), 'sk': p['player_key'],
                                         'slot': p.get('selected_position') or 'BN'})

        for p in [p for r in rosters.values() for p in r] + pool_all:
            key = p['player_key']
            info = player_info(p)
            info.update({'pk': 'PLAYER#' + key, 'sk': 'INFO'})
            batch.put_item(Item=_clean(info))
            owner = finalist_side.get(key) or ('other' if key in owned else 'fa')
            batch.put_item(Item={'pk': 'PLAYER#' + key, 'sk': 'OWNER', 'owner': owner})

        # Rostered by the other ten teams: known, so they can never be claimed.
        for key, tid in owned.items():
            if key not in finalist_side:
                batch.put_item(Item={'pk': 'PLAYER#' + key, 'sk': 'OWNER', 'owner': 'other',
                                     'team_id': tid})

    put_meta(
        mode=mode, play_dates=dates, roster_slots=slots,
        sides={s: {'team_id': tid, 'manager': config.FINALIST_MANAGERS[tid]}
               for s, tid in config.FINALISTS.items()},
        loaded_at=datetime.now(timezone.utc).isoformat(),
        summary=json.dumps(summary),
    )
    summary['total_seconds'] = round(time.time() - t0)
    return summary


def _clean(d):
    return {k: v for k, v in d.items() if v is not None and v != ''}


def job_reset(dry_run=False):
    today = config.league_today()
    if today > config.WEEK1_END:
        raise RuntimeError('refusing to reset: {} is past week one'.format(today))
    if current_mode() == config.MODE_PRODUCTION:
        raise RuntimeError('refusing to reset: the table is in production mode')
    # Before week one starts, today is opened as a test day too, so live stats,
    # probable starters and locks can be checked on a real slate.
    dates = sorted(set([d for d in daterange(config.WEEK1_START, config.WEEK1_END) if d >= today] + [today]))
    return load(config.MODE_SANDBOX, dates, adds_through=today, dry_run=dry_run)


def job_freeze(dry_run=False):
    # Take Sunday's lineups now, while Yahoo is certainly still answering. The
    # 3:30am capture is the retry, not the only chance.
    try:
        early = job_capture(config.WEEK1_END, dry_run)
    except Exception as e:
        logger.exception('freeze-time capture failed')
        early = {'error': str(e)}
    out = load(config.MODE_PRODUCTION, daterange(config.WEEK2_START, config.WEEK2_END),
               adds_through=config.WEEK1_END, dry_run=dry_run)
    out['week1_final_day_capture'] = early
    return out


def lambda_handler(event, context):
    event = event or {}
    job = event.get('job', 'nightly')
    dry = bool(event.get('dry_run'))
    out = {'job': job, 'started': datetime.now(timezone.utc).isoformat(), 'league_today': config.league_today()}
    try:
        if job == 'capture':
            out['capture'] = job_capture(event.get('day'), dry)
        elif job == 'reset':
            out['reset'] = job_reset(dry)
        elif job == 'nightly':
            # Capture first and on its own: a failed reset must never cost a
            # day of week-one lineups.
            try:
                out['capture'] = job_capture(event.get('day'), dry)
            except Exception as e:
                logger.exception('capture failed')
                out['capture_error'] = str(e)
            out['reset'] = job_reset(dry)
        elif job == 'freeze':
            out['freeze'] = job_freeze(dry)
        else:
            raise ValueError('unknown job {}'.format(job))
        out['ok'] = 'capture_error' not in out
    except Exception as e:
        logger.exception('job failed')
        out['ok'] = False
        out['error'] = str(e)
    logger.info(json.dumps(out, default=str))
    if not out['ok']:
        # Fail loudly so the scheduler records it and the alarm fires.
        raise RuntimeError(json.dumps(out, default=str))
    return out
