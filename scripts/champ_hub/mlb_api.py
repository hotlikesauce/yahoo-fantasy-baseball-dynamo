"""MLB StatsAPI: the stat layer for the 2-week championship.

Yahoo's public API gives us the fantasy layer (rosters, eligibility, the free
agent pool) but its per-date player stats are pre-rounded - a day of OPS comes
back as "2.000" with no way to recover the plate appearances behind it. Ratio
categories have to be aggregated from raw components, so the stats themselves
come from MLB directly. No auth, no key.
"""
import json
import time
from urllib.request import Request, urlopen

API = 'https://statsapi.mlb.com/api/v1'
UA = 'summertime-sadness-championship-hub'


def get(url, tries=3):
    for attempt in range(tries):
        try:
            req = Request(url, headers={'User-Agent': UA})
            with urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode('utf-8', 'replace'))
        except Exception as e:
            if attempt == tries - 1:
                raise
            print('    retry {} after {}: {}'.format(attempt + 1, type(e).__name__, e))
            time.sleep(2 * (attempt + 1))


def ip_to_float(value):
    """Baseball innings: 6.1 is six and one third, not six point one."""
    whole, _, outs = str(value or 0).strip().partition('.')
    try:
        return int(whole or 0) + int(outs or 0) / 3.0
    except ValueError:
        return 0.0


def schedule(date):
    """Every regular-season game on a date, with its start time.

    Start times drive the per-player lineup lock: a player is frozen once his
    team's game begins, exactly like Yahoo.
    """
    data = get('{}/schedule?sportId=1&date={}'.format(API, date))
    games = []
    for day in data.get('dates', []):
        for g in day.get('games', []):
            teams = g.get('teams', {})
            games.append({
                'game_pk': g.get('gamePk'),
                'start_utc': g.get('gameDate'),
                'status': g.get('status', {}).get('detailedState'),
                'home_id': teams.get('home', {}).get('team', {}).get('id'),
                'away_id': teams.get('away', {}).get('team', {}).get('id'),
            })
    return games


_ABBR = {}


def team_abbr(team_id):
    """MLB team abbreviation by id. The schedule feed omits it."""
    if not _ABBR:
        for t in get('{}/teams?sportId=1'.format(API)).get('teams', []):
            _ABBR[t['id']] = t.get('abbreviation')
    return _ABBR.get(team_id)


def probable_pitchers(date):
    """{mlb_person_id: {opponent, home}} for everyone announced to start.

    A starting pitcher is the one lineup decision that actually swings a week,
    so the hub marks who is taking the ball rather than making a manager check
    elsewhere. MLB announces these a day or two out; an unannounced game simply
    contributes nobody.
    """
    data = get('{}/schedule?sportId=1&date={}&hydrate=probablePitcher'.format(API, date))
    out = {}
    for day in data.get('dates', []):
        for g in day.get('games', []):
            teams = g.get('teams', {})
            for side, other in (('home', 'away'), ('away', 'home')):
                pp = teams.get(side, {}).get('probablePitcher')
                if not pp or not pp.get('id'):
                    continue
                opp = teams.get(other, {}).get('team', {})
                out[pp['id']] = {
                    'name': pp.get('fullName'),
                    'opponent': team_abbr(opp.get('id')) or opp.get('name'),
                    'home': side == 'home',
                    'start_utc': g.get('gameDate'),
                }
    return out


def first_pitch_by_team(date):
    """{mlb_team_id: earliest start time} - doubleheaders lock on game one."""
    out = {}
    for g in schedule(date):
        for side in ('home_id', 'away_id'):
            tid = g[side]
            if tid is None:
                continue
            if tid not in out or g['start_utc'] < out[tid]:
                out[tid] = g['start_utc']
    return out


def _batting_line(s):
    ab = int(s.get('atBats', 0))
    bb = int(s.get('baseOnBalls', 0))
    hbp = int(s.get('hitByPitch', 0))
    sf = int(s.get('sacFlies', 0))
    h = int(s.get('hits', 0))
    return {
        'R': int(s.get('runs', 0)),
        'H': h,
        'HR': int(s.get('homeRuns', 0)),
        'RBI': int(s.get('rbi', 0)),
        'SB': int(s.get('stolenBases', 0)),
        # OPS components, summed now and divided only at the very end.
        'AB': ab, 'BB': bb, 'HBP': hbp, 'SF': sf,
        'TB_bat': int(s.get('totalBases', 0)),
    }


def _pitching_line(s):
    h = int(s.get('hits', 0))
    doubles = int(s.get('doubles', 0))
    triples = int(s.get('triples', 0))
    hr = int(s.get('homeRuns', 0))
    singles = max(h - doubles - triples - hr, 0)
    ip = ip_to_float(s.get('inningsPitched', 0))
    er = int(s.get('earnedRuns', 0))
    started = int(s.get('gamesStarted', 0)) > 0
    return {
        # TB in this league is Total Bases ALLOWED, and lower is better.
        'TB': singles + 2 * doubles + 3 * triples + 4 * hr,
        'IP': ip,
        'ER': er,
        'H_ALLOWED': h,
        'BB_ALLOWED': int(s.get('baseOnBalls', 0)),
        'K': int(s.get('strikeOuts', 0)),
        'QS': 1 if (started and ip >= 6.0 and er <= 3) else 0,
        'SVH': int(s.get('saves', 0)) + int(s.get('holds', 0)),
    }


def boxscore_lines(date):
    """{mlb_person_id: {'bat': {...}, 'pit': {...}}} for every player on a date.

    A player who appeared twice in a doubleheader has both games summed.
    """
    lines = {}
    for game in schedule(date):
        if game['game_pk'] is None:
            continue
        box = get('{}/game/{}/boxscore'.format(API, game['game_pk']))
        for side in ('home', 'away'):
            for entry in box.get('teams', {}).get(side, {}).get('players', {}).values():
                pid = entry.get('person', {}).get('id')
                if pid is None:
                    continue
                stats = entry.get('stats', {})
                slot = lines.setdefault(pid, {'bat': {}, 'pit': {}})
                if stats.get('batting'):
                    for k, v in _batting_line(stats['batting']).items():
                        slot['bat'][k] = slot['bat'].get(k, 0) + v
                if stats.get('pitching'):
                    for k, v in _pitching_line(stats['pitching']).items():
                        slot['pit'][k] = slot['pit'].get(k, 0) + v
    return lines
