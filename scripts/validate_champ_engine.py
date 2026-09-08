"""Prove the championship scoring engine by replaying a finished Yahoo week.

Reconstructs each team's daily lineups from Yahoo, scores them from MLB box
scores, and diffs the twelve categories against Yahoo's own published totals.
If this matches, the engine can be trusted to run week two on its own.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Team names carry emoji and kaomoji; a cp1252 console would crash on them.
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except AttributeError:
    pass

from champ_hub import mlb_api, players as P, scoring, yahoo_pub as Y

STAT_IDS = {7: 'R', 8: 'H', 12: 'HR', 13: 'RBI', 16: 'SB', 55: 'OPS',
            50: 'IP', 49: 'TB', 26: 'ERA', 27: 'WHIP', 57: 'K9', 83: 'QS', 89: 'SVH'}


def yahoo_week_totals(week):
    """Yahoo's own category totals for every matchup in a week."""
    lg = Y.get('{}/league/{}/scoreboard;week={}?format=json'.format(
        Y.PUB_API, Y.LEAGUE_KEY, week))['fantasy_content']['league']
    out = {}
    matchups = lg[1]['scoreboard']['0']['matchups']
    for m in (matchups[k] for k in sorted((k for k in matchups if k.isdigit()), key=int)):
        for t in (m['matchup']['0']['teams'][k]
                  for k in sorted((k for k in m['matchup']['0']['teams'] if k.isdigit()), key=int)):
            meta = Y.flatten(t['team'][0])
            tid = int(str(meta['team_key']).rsplit('.', 1)[-1])
            stats = {}
            for s in t['team'][1]['team_stats']['stats']:
                name = STAT_IDS.get(int(s['stat']['stat_id']))
                if name:
                    stats[name] = s['stat']['value']
            out[tid] = {'name': meta.get('name'), 'stats': stats}
    return out


def week_dates(week):
    """Ask Yahoo for the week's real boundaries.

    Computing them is a trap: the All-Star break shifts every second-half week,
    so the dates come straight from the scoreboard instead.
    """
    lg = Y.get('{}/league/{}/scoreboard;week={}?format=json'.format(
        Y.PUB_API, Y.LEAGUE_KEY, week))['fantasy_content']['league']
    m = lg[1]['scoreboard']['0']['matchups']['0']['matchup']
    start = date.fromisoformat(m['week_start'])
    end = date.fromisoformat(m['week_end'])
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def replay(team_ids, dates, index, abbr_ids, verbose=True):
    """Score a set of teams over a date range from their real daily lineups."""
    totals = {tid: scoring.empty_totals() for tid in team_ids}
    player_index = {}
    for d in dates:
        iso = d.isoformat()
        if verbose:
            print('  {} ...'.format(iso), end='', flush=True)
        day_lines = mlb_api.boxscore_lines(iso)
        for tid in team_ids:
            roster = Y.team_roster(tid, date=iso)
            matched, unmatched = P.match(roster, index, abbr_ids, use_search=False)
            for p in matched:
                player_index[p['player_key']] = p
            lineup = {p['player_key']: p.get('selected_position') for p in matched}
            scoring.accumulate(totals[tid], day_lines, lineup, player_index)
            for u in unmatched:
                if u.get('selected_position') not in scoring.BENCH_SLOTS:
                    print('\n    WARNING unmatched starter: {} ({})'.format(u['name'], iso))
        if verbose:
            print(' {} player lines'.format(len(day_lines)))
    return totals


def main():
    week = int(sys.argv[1]) if len(sys.argv) > 1 else 23
    dates = week_dates(week)
    print('Validating week {}: {} -> {}'.format(week, dates[0], dates[-1]))

    actual = yahoo_week_totals(week)
    team_ids = sorted(actual)[:2] if len(sys.argv) <= 2 else [int(x) for x in sys.argv[2].split(',')]
    print('Teams: {}'.format(', '.join('{} ({})'.format(t, actual[t]['name']) for t in team_ids)))

    print('Loading MLB player index ...')
    index = P.mlb_player_index()
    abbr_ids = P.team_abbr_to_id()

    totals = replay(team_ids, dates, index, abbr_ids)

    print('\n{:<6} {:>10} {:>12} {:>12} {:>8}'.format('CAT', 'TEAM', 'ENGINE', 'YAHOO', 'DIFF'))
    print('-' * 52)
    ok = True
    for tid in team_ids:
        final = scoring.finalize(totals[tid], weeks=1)
        for cat in scoring.CATS + ['IP']:
            mine = final[cat]
            theirs = actual[tid]['stats'].get(cat)
            try:
                theirs_f = float(theirs)
            except (TypeError, ValueError):
                theirs_f = None
            if cat in ('OPS', 'ERA', 'WHIP', 'K9'):
                # Yahoo displays ratios to two decimals, so compare at its precision.
                mine = round(float(mine), 2)
                match = theirs_f is not None and abs(mine - round(theirs_f, 2)) <= 0.005
            elif cat == 'IP':
                mine = round(float(mine), 2)
                match = theirs_f is not None and abs(mine - mlb_api.ip_to_float(theirs)) <= 0.35
            else:
                match = theirs_f is not None and float(mine) == theirs_f
            ok = ok and match
            print('{:<6} {:>10} {:>12} {:>12} {:>8}'.format(
                cat, tid, mine, theirs if theirs is not None else '-', 'ok' if match else 'MISMATCH'))
        print('-' * 52)
    print('\nRESULT: {}'.format('ENGINE MATCHES YAHOO' if ok else 'DISCREPANCIES ABOVE'))


if __name__ == '__main__':
    main()
