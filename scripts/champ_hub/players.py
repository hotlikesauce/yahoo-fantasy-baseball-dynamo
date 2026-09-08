"""Bridge Yahoo player keys to MLB person ids.

The two APIs share no identifier, so the hub matches on normalized name,
disambiguates duplicates by MLB team, and falls back to MLB's people search for
anyone the active-roster feed omits (prospects, season-ending IL). Every
unmatched player is reported loudly - a silent miss would mean a championship
starter quietly scoring zero.
"""
import re
import unicodedata
import urllib.parse

from . import mlb_api

SUFFIXES = {'jr', 'sr', 'ii', 'iii', 'iv', 'v'}

# Yahoo abbreviations that differ from MLB's.
ABBR_FIXES = {'CHW': 'CWS', 'WSH': 'WSN', 'SD': 'SDP', 'SF': 'SFG',
              'TB': 'TBR', 'KC': 'KCR', 'AZ': 'ARI', 'ARZ': 'ARI'}

# Yahoo carries Shohei Ohtani as two roster spots. They share one MLB id, so
# each entry has to be told which half of his box score it owns.
TWO_WAY_SPLIT = {'(pitcher)': 'pit', '(batter)': 'bat'}


def normalize(name):
    """'Vladimir Guerrero Jr.' -> 'vladimir guerrero'"""
    s = unicodedata.normalize('NFKD', str(name or ''))
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'\((?:pitcher|batter)\)', '', s, flags=re.I)
    s = re.sub(r"[.'`-]", '', s.lower())
    parts = [p for p in re.split(r'\s+', s) if p and p not in SUFFIXES]
    return ' '.join(parts)


def stat_side(name):
    """'Shohei Ohtani (Pitcher)' -> 'pit'. None means score both halves."""
    low = str(name or '').lower()
    for marker, side in TWO_WAY_SPLIT.items():
        if marker in low:
            return side
    return None


def team_abbr_to_id():
    data = mlb_api.get('{}/teams?sportId=1'.format(mlb_api.API))
    return {t['abbreviation']: t['id'] for t in data.get('teams', [])}


def mlb_player_index(season=2026):
    """Active MLB players this season, indexed by normalized name.

    Note this feed omits currentTeam.abbreviation, so entries carry team_id and
    disambiguation happens on the id.
    """
    data = mlb_api.get('{}/sports/1/players?season={}'.format(mlb_api.API, season))
    index = {}
    for p in data.get('people', []):
        index.setdefault(normalize(p.get('fullName')), []).append({
            'mlb_id': p.get('id'),
            'name': p.get('fullName'),
            'team_id': (p.get('currentTeam') or {}).get('id'),
            'position': (p.get('primaryPosition') or {}).get('abbreviation'),
        })
    return index


def search_player(name):
    """MLB's people search - finds prospects and IL guys the roster feed drops."""
    q = urllib.parse.quote(str(name or ''))
    people = mlb_api.get('{}/people/search?names={}'.format(mlb_api.API, q)).get('people', [])
    return [{'mlb_id': p.get('id'), 'name': p.get('fullName'),
             'team_id': (p.get('currentTeam') or {}).get('id'),
             'position': (p.get('primaryPosition') or {}).get('abbreviation')}
            for p in people]


def match(yahoo_players, index=None, abbr_ids=None, season=2026, use_search=True):
    """Attach mlb_id to each Yahoo player. Returns (matched, unmatched)."""
    index = index if index is not None else mlb_player_index(season)
    abbr_ids = abbr_ids if abbr_ids is not None else team_abbr_to_id()
    matched, unmatched = [], []
    for yp in yahoo_players:
        key = normalize(yp.get('name'))
        want_id = abbr_ids.get(ABBR_FIXES.get(yp.get('team_abbr'), yp.get('team_abbr')))
        pick = _choose(index.get(key, []), want_id)
        if pick is None and use_search:
            pick = _choose(search_player(yp.get('name')), want_id, allow_single=True)
        if pick:
            matched.append(dict(yp, mlb_id=pick['mlb_id'], mlb_team_id=pick['team_id'],
                                stat_side=stat_side(yp.get('name'))))
        else:
            unmatched.append(dict(yp, mlb_candidates=len(index.get(key, []))))
    return matched, unmatched


def _choose(cands, want_team_id, allow_single=True):
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0] if allow_single else None
    by_team = [c for c in cands if want_team_id and c['team_id'] == want_team_id]
    return by_team[0] if len(by_team) == 1 else None
