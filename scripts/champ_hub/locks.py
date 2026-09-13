"""When a player's week-two lineup slot is frozen.

This is the rule money rides on, so it lives in one place and the server - never
the browser - is the one that applies it. The commissioner's rulings
(2026-09-13, see config):

  * A player locks at his game's SCHEDULED first pitch. A rain delay does not
    push the lock back. MLB keeps the original time in the schedule's
    `gameDate` even when first pitch slips (verified: a 2026-09-09 game was
    scheduled 17:10Z and started 18:50Z), and it reports a delayed game as
    abstract state Preview - so the lock keys off `gameDate`, never the state.
  * A game MLB marks Postponed or Cancelled (coded state D or C) does not lock
    anyone. Its abstract state is Final even though nobody played, so a check
    on "is it Final" would wrongly freeze the player. If every game his team
    has that day is postponed or cancelled, he is free to be swapped.
  * On a doubleheader the earliest scheduled game locks him for the whole day.
  * A day that is over is locked. A day not yet started is open.

Everything is compared against a UTC `now` the caller supplies, so the server
clock decides - a manager's phone clock never does.
"""
from datetime import datetime, timezone

from . import config

POSTPONED_CODES = ('D', 'C')        # Postponed, Cancelled


def _utc(ts):
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(ts).replace('Z', '+00:00'))


def is_postponed(game):
    status = game.get('status') or {}
    code = status.get('codedGameState')
    if code in POSTPONED_CODES:
        return True
    detail = status.get('detailedState', '')
    return detail.startswith('Postponed') or detail.startswith('Cancelled')


def team_games(games, mlb_team_id):
    """A club's games from a schedule payload's list of games."""
    out = []
    for g in games:
        teams = g.get('teams') or {}
        ids = {((teams.get(side) or {}).get('team') or {}).get('id') for side in ('home', 'away')}
        if mlb_team_id in ids:
            out.append(g)
    return out


def player_lock(date, mlb_team_id, games, now_utc, today=None):
    """Whether a player is locked on `date`.

    games: the schedule's games for `date` (each with gameDate, status, teams).
    Returns a dict: locked, lock_at (ISO scheduled first pitch or None), reason.
    """
    today = today or config.league_today()
    now_utc = _utc(now_utc)

    if date < today:
        return {'locked': True, 'lock_at': None, 'reason': 'day_over'}
    if date > today:
        return {'locked': False, 'lock_at': None, 'reason': 'future_day'}

    if mlb_team_id is None:
        # A player we could not tie to an MLB club cannot be proven unlocked.
        return {'locked': True, 'lock_at': None, 'reason': 'unknown_team'}

    mine = team_games(games, mlb_team_id)
    if not mine:
        return {'locked': False, 'lock_at': None, 'reason': 'no_game'}

    played = [g for g in mine if not is_postponed(g)]
    if not played:
        if config.UNLOCK_ON_POSTPONEMENT:
            return {'locked': False, 'lock_at': None, 'reason': 'postponed'}
        first = min(_utc(g['gameDate']) for g in mine)
        return {'locked': now_utc >= first, 'lock_at': first.isoformat(), 'reason': 'postponed_held'}

    first = min(_utc(g['gameDate']) for g in played)
    return {
        'locked': now_utc >= first,
        'lock_at': first.isoformat(),
        'reason': 'scheduled_first_pitch' if now_utc >= first else 'before_first_pitch',
    }


def move_allowed(date, moving, displaced, games, now_utc, today=None):
    """A lineup move needs every player it touches to be unlocked.

    moving / displaced: player dicts carrying mlb_team_id (displaced may be None).
    Returns (allowed, reason).
    """
    for who, p in (('moving', moving), ('displaced', displaced)):
        if p is None:
            continue
        lock = player_lock(date, p.get('mlb_team_id'), games, now_utc, today)
        if lock['locked']:
            return False, '{} player {} is locked ({})'.format(who, p.get('name'), lock['reason'])
    return True, 'ok'
