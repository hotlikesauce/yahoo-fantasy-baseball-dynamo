"""
Lambda: the week-two write API for the championship hub.

Yahoo stops running the league once week one ends, so from Sept 21 this is the
only authority over the two finalists' rosters. Everything a manager does comes
through here and is decided on the server:

  * Who you are is a secret token, stored only as a SHA-256 hash.
  * A free agent can be claimed exactly once. The claim, the drop, the add
    count and both rosters change in a single DynamoDB transaction guarded by
    conditions, so two managers tapping the same player at the same instant
    cannot both get him, and the 20-acquisition limit cannot be overrun.
  * Locks are checked against the server clock and MLB's schedule - never the
    phone's clock. If MLB cannot be reached, lineup changes are refused rather
    than waved through.
  * Every request, accepted or refused, is written to an audit log.
  * A retried request (same request_id) returns the original answer instead of
    acting twice.

Triggered by: Lambda Function URL (HTTPS).
"""
import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from urllib.request import urlopen

import boto3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from champ_hub import config, locks, rules

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = 'us-west-2'
TABLE = boto3.resource('dynamodb', region_name=REGION).Table(config.TABLE)
DDB = boto3.client('dynamodb', region_name=REGION)
SER = TypeSerializer()

MLB = 'https://statsapi.mlb.com/api'
SCHEDULE_TTL = 60                       # seconds a schedule read is reused
REQUEST_TTL_DAYS = 14

CORS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Cache-Control': 'no-store',
}


# ── plumbing ──────────────────────────────────────────────────────────────
class Refused(Exception):
    def __init__(self, status, reason):
        super().__init__(reason)
        self.status = status
        self.reason = reason


def respond(status, body):
    return {'statusCode': status, 'headers': CORS, 'body': json.dumps(body, default=_json_default)}


def _json_default(o):
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    raise TypeError(type(o).__name__)


def now_utc():
    return datetime.now(timezone.utc)


def item(**attrs):
    return {k: SER.serialize(v) for k, v in attrs.items() if v is not None}


_SCHEDULE = {}


def schedule_games(date):
    """MLB's games for a date, cached briefly. Raises if MLB is unreachable, so
    callers fail closed."""
    hit = _SCHEDULE.get(date)
    if hit and time.time() - hit[0] < SCHEDULE_TTL:
        return hit[1]
    raw = urlopen('{}/v1/schedule?sportId=1&date={}'.format(MLB, date), timeout=8).read()
    games = [g for d in json.loads(raw).get('dates', []) for g in d.get('games', [])]
    _SCHEDULE[date] = (time.time(), games)
    return games


# ── reads ─────────────────────────────────────────────────────────────────
def meta():
    snap = TABLE.get_item(Key={'pk': 'META', 'sk': 'SNAPSHOT'}).get('Item')
    if not snap:
        raise Refused(503, 'The championship rosters have not been loaded yet.')
    if snap.get('mode') == config.MODE_LOADING:
        raise Refused(503, 'Rosters are being reloaded - try again in a few minutes.')
    return snap


def authenticate(token):
    if not token:
        raise Refused(401, 'Missing sign-in link.')
    digest = hashlib.sha256(token.encode('utf-8')).hexdigest()
    found = TABLE.get_item(Key={'pk': 'TOKEN#' + digest, 'sk': 'AUTH'}).get('Item')
    if not found:
        raise Refused(401, 'That sign-in link is not valid.')
    return found['role']                                   # 'a', 'b' or 'commish'


def side_for(role, body):
    # The commissioner trusts the two finalists: any valid sign-in may change
    # either roster (2026-09-13). The link exists to keep strangers off a
    # public write endpoint, not to wall the managers off from each other.
    side = body.get('side')
    if side is None and role in ('a', 'b'):
        return role
    if side not in ('a', 'b'):
        raise Refused(400, 'Say which roster to change.')
    return side


def read_lineup(side, date):
    out, kwargs = {}, {'KeyConditionExpression': boto3.dynamodb.conditions.Key('pk').eq(
        'LINEUP#{}#{}'.format(side, date))}
    while True:
        page = TABLE.query(**kwargs)
        for it in page.get('Items', []):
            out[it['sk']] = it['slot']
        if 'LastEvaluatedKey' not in page:
            return out
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def read_players(keys):
    keys = list(dict.fromkeys(keys))
    out = {}
    for i in range(0, len(keys), 100):
        batch = [{'pk': 'PLAYER#' + k, 'sk': 'INFO'} for k in keys[i:i + 100]]
        res = boto3.resource('dynamodb', region_name=REGION).batch_get_item(
            RequestItems={config.TABLE: {'Keys': batch}})
        for it in res['Responses'].get(config.TABLE, []):
            out[it['pk'][len('PLAYER#'):]] = it
    missing = [k for k in keys if k not in out]
    if missing:
        raise Refused(400, 'Unknown player: {}'.format(missing[0]))
    return out


def play_dates(snap):
    return list(snap.get('play_dates') or snap.get('week2_dates') or [])


def capacity_from(snap):
    return {k: int(v) for k, v in snap['roster_slots'].items()}


# ── writes ────────────────────────────────────────────────────────────────
def audit(side, action, body, outcome, reason, rid, when):
    try:
        TABLE.put_item(Item={
            'pk': 'AUDIT#' + config.league_today(),
            'sk': '{}#{}'.format(when.isoformat(), rid),
            'side': side, 'action': action, 'outcome': outcome, 'reason': reason,
            'request': json.dumps(body, default=str)[:4000],
        })
    except Exception:                                      # the log must never block play
        logger.exception('audit write failed')


def claim_request(rid):
    """Returns a stored response for a request already handled, else None."""
    try:
        TABLE.put_item(
            Item={'pk': 'REQ#' + rid, 'sk': 'RESULT', 'state': 'pending',
                  'expires': int(time.time()) + REQUEST_TTL_DAYS * 86400},
            ConditionExpression='attribute_not_exists(pk)')
        return None
    except ClientError as e:
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
    prior = TABLE.get_item(Key={'pk': 'REQ#' + rid, 'sk': 'RESULT'}).get('Item') or {}
    if prior.get('state') == 'done':
        return json.loads(prior['response'])
    raise Refused(409, 'That request is still being processed.')


def finish_request(rid, status, body):
    TABLE.update_item(
        Key={'pk': 'REQ#' + rid, 'sk': 'RESULT'},
        # "response" and "state" are DynamoDB reserved words, so both are aliased.
        UpdateExpression='SET #s = :d, #r = :r',
        ExpressionAttributeNames={'#s': 'state', '#r': 'response'},
        ExpressionAttributeValues={':d': 'done', ':r': json.dumps({'status': status, 'body': body},
                                                                  default=_json_default)})


def record_quietly(rid, status, body):
    try:
        finish_request(rid, status, body)
    except Exception:
        logger.exception('could not record request %s', rid)


def run_transaction(ops):
    try:
        DDB.transact_write_items(TransactItems=ops)
    except ClientError as e:
        if e.response['Error']['Code'] == 'TransactionCanceledException':
            reasons = [r.get('Code') for r in e.response.get('CancellationReasons', [])]
            logger.info('transaction cancelled: %s', reasons)
            raise Refused(409, 'Someone else changed this first - refresh and try again.')
        raise


# ── actions ───────────────────────────────────────────────────────────────
def act_set_slot(side, body, snap, when):
    date = body.get('date')
    dates = play_dates(snap)
    if date not in dates:
        raise Refused(400, 'That date is not open for lineups.')

    lineup = read_lineup(side, date)
    mover = body.get('player_key')
    displaced = body.get('displaced_key')
    players = read_players([k for k in (mover, displaced) if k] + list(lineup))
    try:
        change = rules.plan_move(lineup, capacity_from(snap), players, mover,
                                 body.get('slot'), displaced)
    except rules.MoveError as e:
        raise Refused(400, str(e))
    if not change:
        return {'ok': True, 'changed': {}}

    try:
        games = schedule_games(date)
    except Exception:
        raise Refused(503, 'Cannot confirm game times with MLB right now, so no lineup changes.')
    for key in change:
        lock = lock_for(players[key], date, games, when)
        if lock['locked']:
            raise Refused(409, '{} is locked ({}).'.format(players[key].get('name'), lock['reason']))

    before = {k: lineup[k] for k in change}
    later = {d: read_lineup(side, d) for d in dates if d > date}
    later[date] = lineup
    apply_to = rules.propagation_dates(later, dates, date, before)

    ops = []
    for d in apply_to:
        for key, new_slot in change.items():
            ops.append({'Update': {
                'TableName': config.TABLE,
                'Key': item(pk='LINEUP#{}#{}'.format(side, d), sk=key),
                'UpdateExpression': 'SET slot = :new, updated_at = :t',
                'ConditionExpression': 'slot = :old',
                'ExpressionAttributeValues': item(**{':new': new_slot, ':old': before[key],
                                                     ':t': when.isoformat()}),
            }})
    run_transaction(ops)
    return {'ok': True, 'changed': change, 'dates': apply_to}


def act_add_drop(side, body, snap, when, with_add=True):
    today = config.league_today()
    dates = play_dates(snap)
    if today not in dates:
        raise Refused(409, 'Roster moves are not open today.')

    add_key = body.get('add_key') if with_add else None
    drop_key = body.get('drop_key')
    if not drop_key or (with_add and not add_key):
        raise Refused(400, 'Say who is being added and who is being dropped.')
    if add_key == drop_key:
        raise Refused(400, 'A player cannot be added and dropped at once.')

    lineup = read_lineup(side, today)
    if drop_key not in lineup:
        raise Refused(400, 'That player is not on your roster.')
    players = read_players([k for k in (add_key, drop_key) if k])
    if with_add and players[add_key].get('mlb_id') in (None, ''):
        raise Refused(400, "{} has no MLB record this season, so he can't score. Pick someone else.".format(
            players[add_key].get('name')))

    try:
        games = schedule_games(today)
    except Exception:
        raise Refused(503, 'Cannot confirm game times with MLB right now, so no roster moves.')
    lock = lock_for(players[drop_key], today, games, when)
    if lock['locked']:
        raise Refused(409, '{} is locked and cannot be dropped until tomorrow.'.format(
            players[drop_key].get('name')))

    stamp = when.isoformat()
    lockout = (when + timedelta(hours=config.DROP_LOCKOUT_HOURS)).isoformat()
    forward = [d for d in dates if d >= today]
    ops = [
        # The dropped player must really be ours; he returns to the pool, held
        # out for the lockout so he cannot be streamed back.
        {'Update': {
            'TableName': config.TABLE,
            'Key': item(pk='PLAYER#' + drop_key, sk='OWNER'),
            'UpdateExpression': 'SET #o = :fa, lockout_until = :lock, changed_at = :t',
            'ConditionExpression': '#o = :side',
            'ExpressionAttributeNames': {'#o': 'owner'},
            'ExpressionAttributeValues': item(**{':fa': 'fa', ':side': side, ':lock': lockout, ':t': stamp}),
        }},
        {'Delete': {
            'TableName': config.TABLE,
            'Key': item(pk='TEAM#' + side, sk='ROSTER#' + drop_key),
            'ConditionExpression': 'attribute_exists(pk)',
        }},
    ]
    for d in forward:
        ops.append({'Delete': {'TableName': config.TABLE,
                               'Key': item(pk='LINEUP#{}#{}'.format(side, d), sk=drop_key)}})

    if with_add:
        ops += [
            # Only an unowned player whose lockout has passed can be claimed.
            # This condition is what makes a double pickup impossible.
            {'Update': {
                'TableName': config.TABLE,
                'Key': item(pk='PLAYER#' + add_key, sk='OWNER'),
                'UpdateExpression': 'SET #o = :side, changed_at = :t REMOVE lockout_until',
                'ConditionExpression': '#o = :fa AND (attribute_not_exists(lockout_until) OR lockout_until < :t)',
                'ExpressionAttributeNames': {'#o': 'owner'},
                'ExpressionAttributeValues': item(**{':fa': 'fa', ':side': side, ':t': stamp}),
            }},
            {'Update': {
                'TableName': config.TABLE,
                'Key': item(pk='TEAM#' + side, sk='STATE'),
                'UpdateExpression': 'SET adds_used = adds_used + :one',
                'ConditionExpression': 'adds_used < max_adds',
                'ExpressionAttributeValues': item(**{':one': 1}),
            }},
            {'Put': {
                'TableName': config.TABLE,
                'Item': item(pk='TEAM#' + side, sk='ROSTER#' + add_key, added_at=stamp),
                'ConditionExpression': 'attribute_not_exists(pk)',
            }},
        ]
        for d in forward:
            ops.append({'Put': {'TableName': config.TABLE,
                                'Item': item(pk='LINEUP#{}#{}'.format(side, d), sk=add_key,
                                             slot='BN', updated_at=stamp)}})

    ops.append({'Put': {'TableName': config.TABLE, 'Item': item(
        pk='CHANGES', sk='{}#{}'.format(stamp, body.get('request_id')), side=side,
        added=add_key, dropped=drop_key)}})

    try:
        run_transaction(ops)
    except Refused:
        # Say which rule stopped it rather than a generic conflict.
        owner = TABLE.get_item(Key={'pk': 'PLAYER#' + add_key, 'sk': 'OWNER'}).get('Item', {}) if with_add else {}
        state = TABLE.get_item(Key={'pk': 'TEAM#' + side, 'sk': 'STATE'}).get('Item', {})
        if with_add and owner.get('owner') not in (None, 'fa'):
            raise Refused(409, '{} has already been claimed.'.format(players[add_key].get('name')))
        if with_add and owner.get('lockout_until') and owner['lockout_until'] >= stamp:
            raise Refused(409, '{} was dropped recently and cannot be added until {}.'.format(
                players[add_key].get('name'), owner['lockout_until']))
        if with_add and int(state.get('adds_used', 0)) >= int(state.get('max_adds', 0)):
            raise Refused(409, 'No acquisitions left.')
        raise
    return {'ok': True, 'added': add_key, 'dropped': drop_key}


def read_players_soft(keys):
    """Like read_players, but skips unknown keys instead of refusing."""
    keys = list(dict.fromkeys(keys))
    out = {}
    res_api = boto3.resource('dynamodb', region_name=REGION)
    for i in range(0, len(keys), 100):
        batch = [{'pk': 'PLAYER#' + k, 'sk': 'INFO'} for k in keys[i:i + 100]]
        res = res_api.batch_get_item(RequestItems={config.TABLE: {'Keys': batch}})
        for it in res['Responses'].get(config.TABLE, []):
            out[it['pk'][len('PLAYER#'):]] = {k: v for k, v in it.items() if k not in ('pk', 'sk')}
    return out


def act_state(role, snap):
    dates = play_dates(snap)
    out = {'role': role, 'mode': snap.get('mode'), 'play_dates': dates,
           'loaded_at': snap.get('loaded_at'), 'server_time': now_utc().isoformat(),
           'roster_slots': snap.get('roster_slots'), 'teams': snap.get('sides'), 'sides': {}}
    keys = set()
    for side in ('a', 'b'):
        state = TABLE.get_item(Key={'pk': 'TEAM#' + side, 'sk': 'STATE'}).get('Item', {})
        lineups = {d: read_lineup(side, d) for d in dates}
        for day in lineups.values():
            keys.update(day.keys())
        out['sides'][side] = {
            'adds_used': state.get('adds_used', 0),
            'max_adds': state.get('max_adds', config.MAX_ADDS),
            'lineups': lineups,
        }
    changes = TABLE.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('pk').eq('CHANGES')).get('Items', [])
    out['changes'] = changes

    stamp = now_utc().isoformat()
    lockouts = {}
    for key in {c.get('dropped') for c in changes if c.get('dropped')}:
        own = TABLE.get_item(Key={'pk': 'PLAYER#' + key, 'sk': 'OWNER'}).get('Item') or {}
        if own.get('owner') == 'fa' and own.get('lockout_until', '') > stamp:
            lockouts[key] = own['lockout_until']
        keys.add(key)
    out['lockouts'] = lockouts
    out['players'] = read_players_soft(keys)
    return out


def lock_for(player, date, games, when):
    # A player with no MLB record has no games to lock on and cannot score, so
    # there is nothing to protect: he stays movable and droppable. The strict
    # "unknown club means locked" rule is for real players we failed to place.
    if player.get('mlb_id') in (None, ''):
        if date < config.league_today():
            return {'locked': True, 'lock_at': None, 'reason': 'day_over'}
        return {'locked': False, 'lock_at': None, 'reason': 'no_mlb_record'}
    return locks.player_lock(date, _int(player.get('mlb_team_id')), games, when)


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


ACTIONS = {
    'set_slot': lambda side, body, snap, when: act_set_slot(side, body, snap, when),
    'add_drop': lambda side, body, snap, when: act_add_drop(side, body, snap, when, with_add=True),
    'drop': lambda side, body, snap, when: act_add_drop(side, body, snap, when, with_add=False),
}


def lambda_handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', 'GET')
    if method == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS, 'body': ''}

    when = now_utc()
    body, side, action, rid = {}, None, None, None
    try:
        if method == 'GET':
            # Lineups are not secret in this league: reading needs no sign-in,
            # every change does.
            params = event.get('queryStringParameters') or {}
            role = 'viewer'
            if params.get('token'):
                role = authenticate(params['token'])
            return respond(200, act_state(role, meta()))

        body = json.loads(event.get('body') or '{}')
        action = body.get('action')
        rid = body.get('request_id')
        if action not in ACTIONS:
            raise Refused(400, 'Unknown action.')
        if not rid or len(rid) > 80:
            raise Refused(400, 'Every change needs a request_id.')

        role = authenticate(body.get('token'))
        side = side_for(role, body)
        snap = meta()

        prior = claim_request(rid)
        if prior is not None:
            return respond(prior['status'], prior['body'])

        try:
            result = ACTIONS[action](side, body, snap, when)
        except Refused as r:
            record_quietly(rid, r.status, {'ok': False, 'reason': r.reason})
            raise
        # The change is committed. Nothing after this point may turn it into an
        # error: telling a manager "nothing was changed" about a move that did
        # happen is exactly the dispute this server exists to prevent.
        record_quietly(rid, 200, result)
        audit(side, action, _redact(body), 'accepted', '', rid, when)
        return respond(200, result)

    except Refused as r:
        audit(side, action, _redact(body), 'refused', r.reason, rid or '-', when)
        return respond(r.status, {'ok': False, 'reason': r.reason})
    except Exception:
        logger.exception('unhandled')
        audit(side, action, _redact(body), 'error', 'server error', rid or '-', when)
        return respond(500, {'ok': False, 'reason': 'Server error - refresh to see whether the change went through.'})


def _redact(body):
    b = dict(body or {})
    b.pop('token', None)
    return b
