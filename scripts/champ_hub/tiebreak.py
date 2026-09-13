"""The 2026 regular-season head-to-head that settles a level championship.

Computed once from Yahoo and frozen to JSON before week two, so the tiebreak is
public and fixed before a pitch is thrown - and so nothing about it depends on
Yahoo still answering after the league locks.
"""
import json
from itertools import product
from pathlib import Path

from . import config, yahoo_pub as Y

OUT = Path(__file__).resolve().parents[2] / 'docs' / 'data' / 'championship_h2h_2026.json'


def _week_matchups(week):
    lg = Y.get('{}/league/{}/scoreboard;week={}?format=json'.format(
        Y.PUB_API, Y.LEAGUE_KEY, week))['fantasy_content']['league']
    board = lg[1]['scoreboard']['0']['matchups']
    out = []
    for k in sorted((k for k in board if k.isdigit()), key=int):
        m = board[k]['matchup']
        teams = {}
        for j in sorted((j for j in m['0']['teams'] if j.isdigit()), key=int):
            meta = Y.flatten(m['0']['teams'][j]['team'][0])
            teams[meta['team_key']] = int(str(meta['team_key']).rsplit('.', 1)[-1])
        won = {tid: 0 for tid in teams.values()}
        for sw in m.get('stat_winners', []) or []:
            s = sw.get('stat_winner', {})
            if str(s.get('is_tied', '0')) == '1':
                continue
            tid = teams.get(s.get('winner_team_key'))
            if tid is not None:
                won[tid] += 1
        out.append({'week': week, 'status': m.get('status'), 'won': won})
    return out


def season_h2h(team_ids, weeks=None):
    """{(a, b): record} for every pair drawn from team_ids that met."""
    weeks = weeks or config.REGULAR_SEASON_WEEKS
    wanted = set(team_ids)
    records = {}
    for wk in weeks:
        for m in _week_matchups(wk):
            ids = sorted(m['won'])
            if len(ids) != 2 or not set(ids) <= wanted:
                continue
            if m['status'] != 'postevent':
                raise RuntimeError('week {} matchup {} not final'.format(wk, ids))
            a, b = ids
            r = records.setdefault((a, b), {
                'a': a, 'b': b, 'meetings': [],
                'a_matchups': 0, 'b_matchups': 0, 'tied_matchups': 0,
                'a_cats': 0, 'b_cats': 0,
            })
            ac, bc = m['won'][a], m['won'][b]
            r['meetings'].append({'week': wk, 'a_cats': ac, 'b_cats': bc})
            r['a_cats'] += ac
            r['b_cats'] += bc
            if ac > bc:
                r['a_matchups'] += 1
            elif bc > ac:
                r['b_matchups'] += 1
            else:
                r['tied_matchups'] += 1
    return records


def resolve(record):
    """Apply config.TIEBREAK_ORDER. Returns ('a'|'b'|'split', step that decided)."""
    if record is None:
        return 'split', 'no_meetings'
    for step in config.TIEBREAK_ORDER:
        if step == 'h2h_matchups':
            if record['a_matchups'] != record['b_matchups']:
                return ('a' if record['a_matchups'] > record['b_matchups'] else 'b'), step
        elif step == 'h2h_categories':
            if record['a_cats'] != record['b_cats']:
                return ('a' if record['a_cats'] > record['b_cats'] else 'b'), step
        elif step == 'split':
            return 'split', step
    return 'split', 'exhausted'


def freeze(bracket_a, bracket_b, path=OUT):
    """Resolve every possible final between the two semifinal brackets."""
    records = season_h2h(list(bracket_a) + list(bracket_b))
    finals = []
    for x, y in product(bracket_a, bracket_b):
        a, b = sorted((x, y))
        rec = records.get((a, b))
        side, step = resolve(rec)
        finals.append({
            'a': a, 'b': b,
            'winner_team_id': a if side == 'a' else (b if side == 'b' else None),
            'outcome': side, 'decided_by': step, 'record': rec,
        })
    payload = {
        'rule': list(config.TIEBREAK_ORDER),
        'regular_season_weeks': [min(config.REGULAR_SEASON_WEEKS), max(config.REGULAR_SEASON_WEEKS)],
        'finals': finals,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1), encoding='utf-8')
    return payload


def lookup(payload, team_a, team_b):
    """The frozen ruling for a specific final, oriented to (team_a, team_b)."""
    a, b = sorted((team_a, team_b))
    for f in payload['finals']:
        if (f['a'], f['b']) == (a, b):
            return f
    return None
