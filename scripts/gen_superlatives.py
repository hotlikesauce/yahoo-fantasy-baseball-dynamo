"""
Pick the season superlatives and write docs/data/superlatives_2026.json.

The page (docs/superlatives_2026.html) is static and reads that file, so
refreshing the board is two commands and no HTML edit:

    python scripts/fetch_league_activity.py
    python scripts/gen_superlatives.py

Every award names the number it was won on and the runner-up, because an award
nobody can check is just an opinion with a trophy on it.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import superlatives_core as sc

OUT = sc.DATA / 'superlatives_2026.json'


def rank_by(teams, key, reverse=True, fmt=None, note=None):
    """Order every manager on one metric, carrying a formatted value and a note."""
    order = sorted(teams.values(), key=lambda t: t[key], reverse=reverse)
    return [{
        'manager': t['manager'],
        'team': t['name'],
        'team_id': t['team_id'],
        'value': t[key],
        'display': (fmt or (lambda v, _t: '%g' % round(v, 2)))(t[key], t),
        'note': note(t) if note else '',
    } for t in order]


def award(key, title, subtitle, metric, teams, sort_key, reverse=True,
          fmt=None, note=None, tone='neutral', caveat=''):
    board = rank_by(teams, sort_key, reverse, fmt, note)
    return {
        'key': key,
        'title': title,
        'subtitle': subtitle,
        'metric': metric,
        'tone': tone,
        'caveat': caveat,
        'winner': board[0],
        'runner_up': board[1] if len(board) > 1 else None,
        'board': board,
    }


def pct(v, _t):
    return '.%03d' % round(v * 1000)


def signed(v, _t):
    return '%+.1f' % v


def signed_pts(v, _t):
    """A win-rate change reads far better as points of rate than as '+0.2'."""
    return '%+.0f' % (v * 1000)


def build(res):
    teams = res['teams']
    weeks = res['weeks']
    awards = []

    # ---- Best manager -------------------------------------------------------
    awards.append(award(
        'best', 'Best Manager', 'All-play record',
        'Win rate against the whole league, every week',
        teams, 'allplay_pct', fmt=pct,
        note=lambda t: '%g-%g all-play, finished %d%s in the standings' % (
            t['allplay_w'], t['allplay_l'], t['rank'], ordinal(t['rank'])),
        tone='good',
        caveat='Every week replayed against all eleven other teams on the twelve '
               'categories - %d games instead of %d, so the schedule stops mattering.'
               % (len(weeks) * 11, len(weeks))))

    # ---- Most active --------------------------------------------------------
    awards.append(award(
        'active', 'Most Active Manager', 'Roster moves',
        'Adds and waiver claims made all season',
        teams, 'moves',
        fmt=lambda v, t: '%d' % v,
        note=lambda t: '%d adds, %d drops - %d bats, %d arms; busiest day %s (%d moves)' % (
            t['adds'], t['drops'], t['bat_adds'], t['pit_adds'],
            t['busiest_day'] or 'n/a', t['busiest_day_moves']),
        tone='good'))

    # ---- Least active (the flip side is too good to leave out) --------------
    awards.append(award(
        'absent', 'Least Active Manager', 'Roster moves',
        'The set-and-forget award',
        teams, 'moves', reverse=False,
        fmt=lambda v, t: '%d' % v,
        note=lambda t: 'one move every %.1f weeks; the league leader made %dx as many' % (
            len(weeks) / t['moves'] if t['moves'] else 0,
            round(max(x['moves'] for x in teams.values()) / t['moves']) if t['moves'] else 0),
        tone='bad'))

    # ---- Most tradable ------------------------------------------------------
    awards.append(award(
        'trades', 'Most Tradable Manager', 'Completed trades',
        'Trades agreed and ratified',
        teams, 'trades',
        fmt=lambda v, t: '%d' % v,
        note=lambda t: '%d of them moved players (%d players changed hands); the rest were picks only'
                       % (t['trades_with_players'], t['players_moved_in_trades'])
                       if t['trades'] else 'never made a deal',
        tone='good',
        caveat='Yahoo\'s own trade count, which includes deals for draft picks alone. '
               'Trading closed 13 August.'))

    # ---- Missed minimum IP --------------------------------------------------
    ip_weeks = sum(1 for w in weeks if res['week_meta'][w]['counts_ip'])
    awards.append(award(
        'ip', 'Most Missed Minimums', 'Weeks under 50 IP',
        'Weeks that forfeited all five pitching categories',
        teams, 'ip_forfeits',
        fmt=lambda v, t: '%d' % v,
        note=lambda t: ('weeks %s - %g innings short in total, low of %g' % (
            ', '.join(str(w) for w in t['ip_forfeit_weeks']),
            t['ip_short_total'], t['min_ip_week'])
            if t['ip_forfeits'] else 'never missed; lowest week was %g IP' % t['min_ip_week']),
        tone='bad',
        caveat='Week 1 ran Wednesday to Sunday and Yahoo waived the minimum - all twelve '
               'teams finished under 50 and every one still won pitching categories - so it '
               'is excluded. %d weeks counted.' % ip_weeks))

    # ---- Luckiest / unluckiest ---------------------------------------------
    awards.append(award(
        'lucky', 'Luckiest Manager', 'Wins above expected',
        'Matchups won beyond what their play deserved',
        teams, 'luck', fmt=signed,
        note=lambda t: '%d-%d-%d in real matchups; an all-play team this good wins %.1f' % (
            t['matchup_w'], t['matchup_l'], t['matchup_t'], t['expected_wins']),
        tone='good',
        caveat='Expected wins = all-play win rate x weeks played. Positive means the '
               'schedule handed them matchups their categories had not earned.'))

    awards.append(award(
        'unlucky', 'Unluckiest Manager', 'Wins below expected',
        'Beaten by the schedule, not the league',
        teams, 'luck', reverse=False, fmt=signed,
        note=lambda t: '%d-%d-%d in real matchups; an all-play team this good wins %.1f' % (
            t['matchup_w'], t['matchup_l'], t['matchup_t'], t['expected_wins']),
        tone='bad'))

    # ---- Upsets -------------------------------------------------------------
    awards.append(award(
        'upsets', 'Most Upsets', 'Wins over stronger teams',
        'Beat a team with a better all-play record',
        teams, 'upsets',
        fmt=lambda v, t: '%d' % v,
        note=lambda t: ('%d of %d wins came against a better team; biggest was week %s over %s, %s'
                        % (t['upsets'], t['matchup_w'],
                           t['upset_detail'][0]['week'], t['upset_detail'][0]['opp'],
                           t['upset_detail'][0]['score'])
                        if t['upset_detail'] else 'no wins over a stronger team'),
        tone='good',
        caveat='Weaker teams get more chances at an upset by definition, so read this '
               'next to the all-play column rather than on its own.'))

    # ---- Plays up / plays down ---------------------------------------------
    awards.append(award(
        'playsup', 'Rises to the Occasion', 'Slope vs league',
        'Loses least ground as the opponent gets better',
        teams, 'slope_rel', fmt=signed,
        note=lambda t: '%.1f cats vs the top half, %.1f vs the bottom half (%+.1f)' % (
            t['vs_strong'], t['vs_weak'], t['split']),
        tone='good',
        caveat='Each manager\'s weekly category points regressed on their opponent\'s '
               'all-play rate. Everyone\'s slope is negative - good teams beat you - so '
               'this is measured against the league average slope of %.1f.'
               % res['league_slope']))

    awards.append(award(
        'playsdown', 'Plays Down to the Competition', 'Slope vs league',
        'Falls apart against good teams',
        teams, 'slope_rel', reverse=False, fmt=signed,
        note=lambda t: '%.1f cats vs the top half, %.1f vs the bottom half (%+.1f), r = %+.2f' % (
            t['vs_strong'], t['vs_weak'], t['split'], t['slope_r']),
        tone='bad'))

    # ---- A few more that fell out of the same data --------------------------
    awards.append(award(
        'streaky', 'Most Volatile', 'Week-to-week swing',
        'Standard deviation of weekly category points',
        teams, 'stdev_pts',
        fmt=lambda v, t: '%.2f' % v,
        note=lambda t: 'high of %g in week %d, low of %g in week %d' % (
            t['best_week_pts'], t['best_week'], t['worst_week_pts'], t['worst_week']),
        tone='neutral'))

    awards.append(award(
        'improved', 'Most Improved', 'Second half vs first',
        'All-play rate gained after the halfway mark, in points of .000',
        teams, 'h2_delta', fmt=signed_pts,
        note=lambda t: '%s in the first %d weeks, %s in the last %d' % (
            pct(t['allplay_pct_h1'], t), len(weeks) // 2,
            pct(t['allplay_pct_h2'], t), len(weeks) - len(weeks) // 2),
        tone='good'))

    awards.append(award(
        'faded', 'Biggest Fade', 'Second half vs first',
        'The other end of the same number',
        teams, 'h2_delta', reverse=False, fmt=signed_pts,
        note=lambda t: '%s in the first %d weeks, %s in the last %d' % (
            pct(t['allplay_pct_h1'], t), len(weeks) // 2,
            pct(t['allplay_pct_h2'], t), len(weeks) - len(weeks) // 2),
        tone='bad'))

    return awards


def ordinal(n):
    return 'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')


def main():
    data = sc.load()
    res = sc.compute(data)
    teams = res['teams']
    for t in teams.values():
        t['h2_delta'] = t['allplay_pct_h2'] - t['allplay_pct_h1']

    awards = build(res)

    payload = {
        'generated_at': data['fetched_at'],
        'season': data['meta']['season'],
        'league': data['meta']['league_name'],
        'weeks_counted': res['weeks'],
        'through_week': max(res['weeks']),
        'league_slope': res['league_slope'],
        'awards': awards,
        'records': res['records'],
        'teams': [{
            k: t[k] for k in (
                'team_id', 'manager', 'name', 'rank', 'cat_w', 'cat_l', 'cat_t',
                'cat_pts', 'matchup_w', 'matchup_l', 'matchup_t',
                'allplay_w', 'allplay_l', 'allplay_pct', 'allplay_pct_l6',
                'allplay_pct_l2', 'allplay_pct_h1', 'allplay_pct_h2',
                'avg_pts', 'stdev_pts', 'luck', 'expected_wins', 'upsets',
                'ip_forfeits', 'ip_forfeit_weeks', 'avg_ip', 'min_ip_week',
                'moves', 'adds', 'drops', 'trades', 'slope', 'slope_rel',
                'slope_r', 'vs_strong', 'vs_weak', 'split', 'weekly')
        } for t in sorted(teams.values(), key=lambda x: -x['allplay_pct'])],
    }

    OUT.write_text(json.dumps(payload, indent=1, ensure_ascii=False), encoding='utf-8')
    print('Wrote %s (%.0f KB)' % (OUT, OUT.stat().st_size / 1024))
    n = sc.embed(sc.DATA.parent / 'superlatives_2026.html', payload)
    print('Embedded %.0f KB into superlatives_2026.html' % (n / 1024))
    print('\nThrough week %d - %d weeks counted\n' % (payload['through_week'], len(res['weeks'])))
    for a in awards:
        w = a['winner']
        r = a['runner_up']
        print('%-28s %-8s %-8s   (next: %s %s)' % (
            a['title'], w['display'], w['manager'],
            r['manager'] if r else '-', r['display'] if r else ''))


if __name__ == '__main__':
    main()
