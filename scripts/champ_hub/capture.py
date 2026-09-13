"""Capture what Yahoo knows before Yahoo stops knowing it.

Two jobs, both against deadlines that cannot be re-run afterwards:

  * Daily, through week one (Sept 14-20): each finalist's lineup for the day
    just finished. Yahoo's roster history cannot show a player who has since
    been dropped, so reconstructing week one on Sept 20 would silently lose any
    mid-week drop's stats - and with them the week-one components the
    cumulative ratio categories are built from. The day's lineup has to be read
    the morning after, before a later drop erases it.

  * Once, at the freeze: every roster in the league (so ownership is known and
    nobody can add a player a non-finalist owns), each finalist's full roster
    and slots, the entire free agent pool with no page cap, and each
    finalist's week-one acquisitions against the two-week limit.
"""
import time
from datetime import date, datetime, timedelta, timezone

from . import config, players as P, yahoo_pub as Y


def _league_tz():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(config.LEAGUE_TZ)
    except Exception:
        return timezone(timedelta(hours=-4))       # EDT: all of September 2026


def lineup_for(team_id, day):
    """A finalist's roster exactly as Yahoo recorded it for one date."""
    roster = Y.team_roster(team_id, date=day)
    return [{
        'player_key': p['player_key'],
        'name': p['name'],
        'team_abbr': p.get('team_abbr'),
        'position_type': p.get('position_type'),
        'eligible_positions': p.get('eligible_positions', []),
        'slot': p.get('selected_position'),
    } for p in roster]


def acquisitions(team_id, start_day, end_day):
    """Every player a team added between two league dates, inclusive.

    Counted per player added, so an add/drop is one acquisition. Waiver claims
    are adds too. Day boundaries are league (Eastern) midnights.
    """
    tz = _league_tz()
    lo = datetime.combine(date.fromisoformat(start_day), datetime.min.time(), tz)
    hi = datetime.combine(date.fromisoformat(end_day) + timedelta(days=1), datetime.min.time(), tz)
    team_key = '{}.t.{}'.format(Y.LEAGUE_KEY, team_id)

    lg = Y.get('{}/league/{}/transactions?format=json'.format(Y.PUB_API, Y.LEAGUE_KEY))['fantasy_content']['league']
    feed = lg[1]['transactions']
    adds = []
    for k in sorted((k for k in feed if k.isdigit()), key=int):
        t = feed[k]['transaction']
        meta = Y.flatten(t[0] if isinstance(t[0], list) else [t[0]])
        if meta.get('status') != 'successful':
            continue
        when = datetime.fromtimestamp(int(meta.get('timestamp', 0)), timezone.utc)
        if not (lo <= when < hi):
            continue
        players = t[1].get('players') if len(t) > 1 and isinstance(t[1], dict) else None
        if not isinstance(players, dict):
            continue                                  # commissioner rows carry no players
        for pk in sorted((x for x in players if x.isdigit()), key=int):
            pl = players[pk]['player']
            td = pl[1].get('transaction_data')
            td = td[0] if isinstance(td, list) else td
            if td.get('type') == 'add' and td.get('destination_team_key') == team_key:
                pm = Y.flatten(pl[0])
                adds.append({
                    'player_key': pm.get('player_key'),
                    'name': (pm.get('name') or {}).get('full'),
                    'at': when.isoformat(),
                    'transaction_type': meta.get('type'),
                })
    return adds


def full_free_agent_pool(page=25, pause=0.25, hard_stop=6000):
    """The whole pool. The hub's display cap must never limit the snapshot."""
    out, start = [], 0
    while start < hard_stop:
        url = '{}/league/{}/players;status=FA;start={};count={}?format=json'.format(
            Y.PUB_API, Y.LEAGUE_KEY, start, page)
        players = Y.get(url)['fantasy_content']['league'][1]['players']
        if not isinstance(players, dict) or int(players.get('count', 0)) == 0:
            break
        chunk = [Y.parse_player(p['player']) for p in Y._iter_numeric(players)]
        out.extend(chunk)
        if len(chunk) < page:
            break
        start += page
        time.sleep(pause)
    else:
        raise RuntimeError('free agent pool exceeded {} - refusing a partial snapshot'.format(hard_stop))
    return out


def ownership():
    """{player_key: team_id} for every rostered player in the league."""
    owned = {}
    for tid in range(1, 13):
        for p in Y.team_roster(tid):
            owned[p['player_key']] = tid
    return owned


def bridge(yahoo_players, index=None, abbr_ids=None):
    """Attach MLB ids. Returns (matched, unmatched) - unmatched must be reviewed,
    because a player with no MLB id cannot be scored."""
    index = index if index is not None else P.mlb_player_index()
    abbr_ids = abbr_ids if abbr_ids is not None else P.team_abbr_to_id()
    return P.match(yahoo_players, index, abbr_ids, use_search=True)
