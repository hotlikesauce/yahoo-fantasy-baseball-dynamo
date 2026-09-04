"""
Build docs/data/playoff_hub_2026.json - seeds, bracket, matchup predictions
and title odds.

Re-runnable at any point in the postseason:

    python scripts/fetch_league_activity.py
    python scripts/gen_playoff_hub.py

Before the regular season closes the seeds are projected from the live final
week and the bracket is the one those seeds imply. The moment Yahoo posts real
playoff pairings this switches to them, and finished rounds stop being
simulated at all.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import superlatives_core as sc
import playoff_predict as pp

OUT = sc.DATA / 'playoff_hub_2026.json'


def profile(rows, weeks, tid, window):
    """Average category line over a window, for the matchup comparison table."""
    pool = pp.week_pool(rows, tid, window)
    if not pool:
        return {}
    out = {c: sum(r['stats'][c] for r in pool) / len(pool) for c in sc.CATS}
    out['IP'] = sum(r['ip'] for r in pool) / len(pool)
    out['forfeit_rate'] = sum(1 for r in pool if r['forfeit']) / len(pool)
    return out


def main():
    data = sc.load()
    res = sc.compute(data)
    rows, weeks = res['rows'], res['weeks']
    meta = data['meta']

    seeding = pp.seed_teams(data, res, meta['num_playoff_teams'])
    seeds = seeding['seeds']
    by_id = {s['team_id']: s for s in seeds}

    first = meta['playoff_start_week']
    playoff_weeks = [first, first + 1, first + 2]

    # Yahoo's own pairings win the moment they exist - but only if they are the
    # right shape. Every one of the twelve teams plays in week 23, so a
    # half-parsed round is a real possibility, and adopting one would delete a
    # bye. Six teams and two byes means exactly two bracket matchups leaving
    # exactly two bracket teams unpaired; anything else is not a round we
    # understand, so the projection is kept and the page says so.
    field = {s['team_id'] for s in seeds if s['in']}
    bracket = pp.default_bracket(seeds, playoff_weeks)
    expected_games = (len(field) - len(bracket['byes'])) // 2
    real_q = pp.real_bracket_round(data, playoff_weeks[0], field)

    playing = {t for m in real_q for t in (m['a'], m['b'])}
    byes = [s['team_id'] for s in seeds if s['in'] and s['team_id'] not in playing]
    shape_ok = (len(real_q) == expected_games
                and len(playing) == 2 * expected_games
                and len(byes) == len(field) - 2 * expected_games)
    if real_q and shape_ok:
        bracket['quarters'] = [{'week': m['week'], 'a': m['a'], 'b': m['b']} for m in real_q]
        bracket['byes'] = byes
        bracket['source'] = 'yahoo'
    else:
        bracket['source'] = 'projected'
        if real_q:
            print('  NOTE: week %d posted %d bracket matchup(s) covering %d teams; expected '
                  '%d covering %d. Keeping the projected bracket.'
                  % (playoff_weeks[0], len(real_q), len(playing),
                     expected_games, 2 * expected_games))

    wins = pp.windows(weeks)
    preds = {name: pp.Predictor(rows, weeks, win) for name, win in wins.items()}
    preds['blend'] = pp.BlendPredictor(rows, weeks)

    # Every pairing that could happen among the six, so the page can show a
    # prediction for a semi-final before anyone knows who is in it.
    order = [s['team_id'] for s in seeds if s['in']]
    matrix = {}
    for i, a in enumerate(order):
        for b in order[i + 1:]:
            entry = {}
            for name, p in preds.items():
                r = p.matchup(a, b)
                entry[name] = {
                    'win': round(r['win'], 4),
                    'tie': round(r['tie'], 4),
                    'exp_a': round(r['exp_pts'], 2),
                    'exp_b': round(sc.N_CATS - r['exp_pts'], 2),
                    'cats': {c: round(v, 3) for c, v in r['cats'].items()},
                }
            matrix['%d-%d' % (a, b)] = entry

    # A bracket week already underway gets a fifth, live window: the same twelve
    # categories finished from where they actually stand. This is the one the
    # page leads with while the week is running, because by Thursday the
    # season-strength number is answering a question nobody is asking.
    live_pairs, live_rows = pp.live_bracket_week(data, playoff_weeks[0], field, rows)
    live_week = None
    if live_pairs:
        live_week = pp.LiveWeek(rows, weeks, live_rows)
        for a, b in live_pairs:
            key = '%d-%d' % (a, b)
            flip = key not in matrix
            if flip:
                key = '%d-%d' % (b, a)
            x, y = (b, a) if flip else (a, b)
            r = live_week.matchup(preds['blend'], x, y)
            matrix[key]['live'] = {
                'win': round(r['win'], 4),
                'tie': round(r['tie'], 4),
                'exp_a': round(r['exp_pts'], 2),
                'exp_b': round(sc.N_CATS - r['exp_pts'], 2),
                'cats': {c: round(v, 3) for c, v in r['cats'].items()},
            }

    # The league breaks a tied week on the season series, counted in categories.
    h2h = pp.head_to_head(rows, weeks)
    odds = pp.simulate_bracket(preds['blend'], seeds, bracket, data, h2h,
                               rows=rows, weeks=weeks)

    # Flatten the pairs the page needs, oriented both ways so the card can look
    # one up without knowing which id happens to be lower.
    h2h_out = {}
    for i, a in enumerate(order):
        for b in order[i + 1:]:
            rec = h2h.get((a, b) if a < b else (b, a))
            if not rec or not rec['n']:
                continue
            ca, cb = (rec['a'], rec['b']) if a < b else (rec['b'], rec['a'])
            winner = a if ca > cb else b if cb > ca else None
            h2h_out['%d-%d' % (a, b)] = {
                'a': round(ca, 1), 'b': round(cb, 1),
                'meetings': rec['n'], 'winner': winner,
            }

    profiles = {}
    for tid in order:
        profiles[str(tid)] = {name: profile(rows, weeks, tid, win)
                              for name, win in wins.items()}

    payload = {
        'generated_at': data['fetched_at'],
        'league': meta['league_name'],
        'season': meta['season'],
        'through_week': max(weeks),
        'weeks_counted': weeks,
        'playoff_weeks': playoff_weeks,
        'playoff_teams': meta['num_playoff_teams'],
        'reseeding': meta['reseeding'],
        'seeds_final': seeding['final'],
        'live_week': seeding['live_week'],
        'bracket_source': bracket['source'],
        'blend_weights': pp.BLEND,
        'window_weeks': {k: v for k, v in wins.items()},
        'has_live': bool(live_pairs),
        'cats': sc.CATS,
        'lower_is_better': sorted(sc.LOWER_IS_BETTER),
        'pitching_cats': sc.PITCHING,
        'seeds': seeds,
        'bracket': bracket,
        'results': {str(w): pp.real_bracket_round(data, w, field) for w in playoff_weeks},
        'matrix': matrix,
        'h2h': h2h_out,
        'odds': {str(k): {kk: round(vv, 4) for kk, vv in v.items()} for k, v in odds.items()},
        'profiles': profiles,
    }

    OUT.write_text(json.dumps(payload, indent=1, ensure_ascii=False), encoding='utf-8')
    print('Wrote %s (%.0f KB)' % (OUT, OUT.stat().st_size / 1024))
    n = sc.embed(sc.DATA.parent / 'playoff_hub_2026.html', payload)
    print('Embedded %.0f KB into playoff_hub_2026.html' % (n / 1024))

    tag = 'FINAL' if seeding['final'] else 'PROJECTED (week %s still live)' % seeding['live_week']
    print('\nSeeds - %s\n' % tag)
    for s in seeds:
        print(' %s%2d  %-8s %-28s %6.1f  (%.1f banked %+.1f live)' % (
            '*' if s['in'] else ' ', s['seed'], s['manager'], s['name'][:28],
            s['pts'], s['banked'], s['live']))

    print('\nBracket (%s) - weeks %s' % (bracket['source'], playoff_weeks))
    print('  byes: %s' % ', '.join(by_id[t]['manager'] for t in bracket['byes']))
    for q in bracket['quarters']:
        a, b = q['a'], q['b']
        m = matrix.get('%d-%d' % (a, b)) or matrix.get('%d-%d' % (b, a))
        flip = '%d-%d' % (a, b) not in matrix
        # Quote the live projection once the week is under way - the
        # season-strength number is not what anyone is asking about by then.
        win = m.get('live') or m['blend']
        pa = (1 - win['win'] - win['tie']) if flip else win['win']
        # Two-way, same as the page: a 6-6 week is not a third outcome, the
        # league tiebreak sends it to a known team, so give it to that team.
        tie_to = pp.h2h_winner(h2h, a, b)
        if tie_to is None:
            tie_to = a if by_id[a]['seed'] < by_id[b]['seed'] else b
        pa += win['tie'] if tie_to == a else 0.0
        print('  wk%d  %-8s vs %-8s   %s %.0f%% / %s %.0f%%%s' % (
            q['week'], by_id[a]['manager'], by_id[b]['manager'],
            by_id[a]['manager'], pa * 100,
            by_id[b]['manager'], (1 - pa) * 100,
            '  (live)' if 'live' in m else ''))

    print('\nTitle odds (blended)\n')
    print('  %-8s %6s %6s %6s' % ('mgr', 'semi', 'final', 'title'))
    for tid, o in sorted(odds.items(), key=lambda kv: -kv[1]['title']):
        print('  %-8s %5.1f%% %5.1f%% %5.1f%%' % (
            by_id[tid]['manager'], o['semi'] * 100, o['final'] * 100, o['title'] * 100))


if __name__ == '__main__':
    main()
