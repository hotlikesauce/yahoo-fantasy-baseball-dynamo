"""Roster rules for week two, shared by the server and its tests.

The browser shows these same rules so a manager can see what is legal, but the
server is what applies them: a request the page would never send is refused
anyway. Keep this file and the page's eligibleFor / landingSpot in step.
"""
BAT_SLOTS = ('C', '1B', '2B', '3B', 'SS', 'OF', 'Util')
PIT_SLOTS = ('SP', 'RP', 'P')
BENCH = ('BN', 'IL', 'NA')
INACTIVE = ('IL', 'NA')


class MoveError(ValueError):
    """A move a manager is not allowed to make. The message is shown to them."""


def is_batter(player):
    return player.get('position_type') != 'P'


def eligible(player, pos):
    """Batters and pitchers never cross. Util takes any batter; P any pitcher;
    SP and RP need real eligibility (a reliever cannot start)."""
    if pos == 'BN':
        return True
    if pos == 'IL':
        # IL10 / IL15 / IL60 - read live from MLB by the API. Day-to-day or NA
        # (minor leaguer) does not qualify.
        return str(player.get('status') or '').startswith('IL')
    if pos == 'NA':
        return False
    pitcher = not is_batter(player)
    if pos in PIT_SLOTS:
        if not pitcher:
            return False
        return pos == 'P' or pos in (player.get('eligible_positions') or [])
    if pitcher:
        return False
    if pos == 'Util':
        return True
    return pos in (player.get('eligible_positions') or [])


def active_limit(capacity):
    """How many players may sit outside IL and NA: every starting slot plus the bench."""
    return sum(int(n) for pos, n in capacity.items() if pos not in INACTIVE)


def active_count(lineup):
    return sum(1 for slot in lineup.values() if slot not in INACTIVE)


def check_roster_size(lineup, change, capacity, players=None):
    """Refuses a change that takes the active roster past its limit. A roster
    already over (as Yahoo handed it over) is not forced down, but may not grow."""
    after = dict(lineup)
    after.update(change)
    before_n, after_n, limit = active_count(lineup), active_count(after), active_limit(capacity)
    if after_n > limit and after_n > before_n:
        names = [((players or {}).get(k) or {}).get('name') for k, s in change.items()
                 if s not in INACTIVE and lineup.get(k) in INACTIVE]
        who = next((n for n in names if n), 'That player')
        raise MoveError('Roster full ({} active max). Drop someone before activating {}.'.format(limit, who))


def plan_move(lineup, capacity, players, mover_key, target_pos, displaced_key=None):
    """Work out a move on one day's lineup without changing anything.

    lineup: {player_key: slot} for the day. capacity: {slot: count}.
    players: {player_key: info}. displaced_key names who is in the target slot
    when the manager drops onto an occupied spot.

    Returns {player_key: new_slot} for every player whose slot changes, or {}
    for a no-op. Raises MoveError otherwise.
    """
    if mover_key not in lineup:
        raise MoveError('That player is not on this roster.')
    if target_pos != 'BN' and target_pos not in capacity:
        raise MoveError('There is no {} slot.'.format(target_pos))

    mover = players[mover_key]
    if not eligible(mover, target_pos):
        raise MoveError('{} cannot play {}.'.format(mover.get('name'), target_pos))

    source = lineup[mover_key]
    if displaced_key is None:
        if source == target_pos:
            return {}
        if target_pos != 'BN':
            taken = sum(1 for k, s in lineup.items() if s == target_pos and k != mover_key)
            if taken >= capacity.get(target_pos, 0):
                raise MoveError('{} is full. Drop him onto the player he replaces.'.format(target_pos))
        check_roster_size(lineup, {mover_key: target_pos}, capacity, players)
        return {mover_key: target_pos}

    if displaced_key == mover_key:
        raise MoveError('A player cannot replace himself.')
    if lineup.get(displaced_key) != target_pos:
        raise MoveError('That player is no longer in {} - refresh and try again.'.format(target_pos))

    # The man being replaced takes the slot just vacated only if he can play
    # it; otherwise he goes to the bench. The bench may run long, because the
    # vacated starting slot is now empty and the roster size is unchanged.
    displaced = players[displaced_key]
    landing = source if eligible(displaced, source) else 'BN'
    change = {mover_key: target_pos, displaced_key: landing}
    check_roster_size(lineup, change, capacity, players)
    return change


def propagation_dates(lineups_by_date, dates, start_date, before):
    """The days a change made on start_date applies to.

    Always start_date itself. Then each following day, for as long as every
    player involved still sits exactly where he sat on start_date before the
    change. The first later day a manager had set differently is where carrying
    it forward stops - so a lineup they prepared for tomorrow is never
    overwritten by a tweak made today.
    """
    out, started = [], False
    for d in dates:
        if d == start_date:
            started = True
            out.append(d)
            continue
        if not started:
            continue
        day = lineups_by_date.get(d, {})
        if all(day.get(k) == slot for k, slot in before.items()):
            out.append(d)
        else:
            break
    return out
