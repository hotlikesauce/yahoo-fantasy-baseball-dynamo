"""
Season-long manager metrics, computed from the public-API cache.

Everything here reads docs/data/league_activity_2026.json (written by
fetch_league_activity.py) and nothing else, so the numbers are reproducible
offline and every award can be traced back to a week and a category.

The two ideas the rest of the file leans on:

  * ALL-PLAY - replay every week as if each team had played all eleven others
    on that week's twelve categories. Twenty-one weeks x eleven opponents is a
    231-game season, which is enough for the schedule to stop mattering. This
    is the strength number; the real W-L is strength plus who you drew.

  * OPPONENT STRENGTH - a team's all-play rate is also the honest measure of
    how hard it was to face them, and it is what "upset" and "plays up or down"
    are both measured against.
"""

import json
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path

DATA = Path(__file__).resolve().parent.parent / 'docs' / 'data'
CACHE = DATA / 'league_activity_2026.json'

CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']
BATTING = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB']
PITCHING = ['ERA', 'WHIP', 'K9', 'QS', 'SVH']
# Lower is better in these two; everything else scored here is higher-is-better.
LOWER_IS_BETTER = {'ERA', 'WHIP'}
N_CATS = 12
MIN_IP = 50.0

# TeamInfo-2026 carries these; hard-coding them keeps the generator runnable
# without AWS credentials. 'Michael' on team 10 is Mikey - Mike is team 12,
# the perennial shrug - and 'kurtis varga' is trimmed to a first name.
MANAGERS = {
    1: 'Taylor', 2: 'James', 3: 'Josh', 4: 'Bryant', 5: 'Kurtis', 6: 'Mark',
    7: 'Eric', 8: 'Austin', 9: 'Greg', 10: 'Mikey', 11: 'Kevin', 12: 'Mike',
}


def load(path=CACHE):
    return json.loads(Path(path).read_text(encoding='utf-8'))


DATA_BEGIN = '<!--DATA:BEGIN-->'
DATA_END = '<!--DATA:END-->'


def embed(html_path, payload, element_id='pageData'):
    """
    Write the page's data straight into its HTML, between the DATA markers.

    The pages originally only fetched docs/data/*.json, which works when the
    site is served but fails the moment anyone opens the file from disk:
    Chrome refuses fetch() on a file:// URL outright ('URL scheme "file" is not
    supported'), so the page showed nothing but its own error box. Every other
    generator in this repo emits self-contained HTML; these now do too, and the
    fetch stays in the page as a fallback for a hand-edited JSON.

    '</' is escaped because a team name containing '</script>' would otherwise
    close the block early and inject markup - the one genuine hazard of
    embedding JSON in a page.
    """
    path = Path(html_path)
    html = path.read_text(encoding='utf-8')
    if DATA_BEGIN not in html or DATA_END not in html:
        raise SystemExit('ERROR: %s has no <!--DATA:BEGIN--> / <!--DATA:END--> markers'
                         % path.name)
    blob = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).replace('</', '<\\/')
    block = ('%s\n<script id="%s" type="application/json">%s</script>\n%s'
             % (DATA_BEGIN, element_id, blob, DATA_END))
    start = html.index(DATA_BEGIN)
    end = html.index(DATA_END) + len(DATA_END)
    path.write_text(html[:start] + block + html[end:], encoding='utf-8')
    return len(blob)


def to_float(cat, raw):
    """Yahoo hands back strings, and OPS as '.863'. Missing means zero."""
    try:
        return float(str(raw).strip() or 0)
    except ValueError:
        return 0.0


def full_week(matchup):
    """
    Did the 50-inning minimum apply?

    Week 1 ran Wed-Sun (25-29 March) and Yahoo waived the minimum: all twelve
    teams finished under 50 and every one of them still won pitching
    categories. Judging anyone for 'missing' a minimum that was never enforced
    would be the single biggest error on the board, so short weeks are excluded
    by their own dates rather than by a hard-coded week number.
    """
    try:
        s = date.fromisoformat(matchup['week_start'])
        e = date.fromisoformat(matchup['week_end'])
    except (TypeError, ValueError):
        return True
    return (e - s).days >= 6


# ==============================================================
# Per-week table
# ==============================================================
def build_weeks(data, through=None):
    """
    Flatten the scoreboard into one row per team per completed week.

    A week counts only when Yahoo has marked it postevent - week 22 is still
    running while this is first generated, and half a week of categories would
    poison every rate on the board.
    """
    rows = defaultdict(dict)          # week -> team_id -> row
    meta = {}
    for wk, matchups in sorted(data['weeks'].items(), key=lambda kv: int(kv[0])):
        w = int(wk)
        if through is not None and w > through:
            continue
        if not matchups or any(m['status'] != 'postevent' for m in matchups):
            continue
        meta[w] = {
            'week': w,
            'start': matchups[0].get('week_start'),
            'end': matchups[0].get('week_end'),
            'counts_ip': full_week(matchups[0]),
            'playoffs': matchups[0].get('is_playoffs', False),
        }
        for m in matchups:
            for me, opp in ((m['a'], m['b']), (m['b'], m['a'])):
                ties = N_CATS - m['a']['cats'] - m['b']['cats']
                rows[w][me['team_id']] = {
                    'week': w,
                    'team_id': me['team_id'],
                    'name': me['name'],
                    'opponent': opp['team_id'],
                    'cats': me['cats'],
                    'opp_cats': opp['cats'],
                    'ties': ties,
                    'pts': me['cats'] + 0.5 * ties,
                    'result': ('W' if m['winner'] == me['team_id']
                               else 'T' if m['winner'] is None else 'L'),
                    'ip': me['ip'],
                    'ip_short': round(max(0.0, MIN_IP - me['ip']), 2),
                    'counts_ip': meta[w]['counts_ip'],
                    'forfeit': me['ip'] < MIN_IP and meta[w]['counts_ip'],
                    'stats': {c: to_float(c, me['stats'].get(c)) for c in CATS},
                }
    return rows, meta


def all_play(rows, weeks=None):
    """
    Replay each week against all eleven other teams on the twelve categories.

    Two different things come out of this, and the difference matters more in
    this league than in most:

      * `pct` - how often a team would BEAT a random opponent. A matchup rate.
      * `exp_cats` - how many of the twelve CATEGORIES a team would average
        against the field. This is the one that matters here, because the
        standings and the playoff seeds are kept in category points
        (`wins + 0.5 * ties`), not in matchup record. A team can be third in
        matchup terms and first on the table, and in 2026 one was.

    Ties inside a category split it, exactly as Yahoo scores them, so a 6-6
    week against an opponent is half a win rather than a loss.
    """
    weeks = sorted(rows) if weeks is None else [w for w in weeks if w in rows]
    tally = defaultdict(lambda: {'w': 0.0, 'l': 0.0, 'games': 0, 'weeks': 0,
                                 'exp_cats': 0.0})
    per_week = defaultdict(dict)

    for w in weeks:
        teams = rows[w]
        for tid, me in teams.items():
            wins = 0.0
            cats = 0.0
            for oid, other in teams.items():
                if oid == tid:
                    continue
                score = 0.0
                for c in CATS:
                    a, b = me['stats'][c], other['stats'][c]
                    if a == b:
                        score += 0.5
                    elif (a < b) if c in LOWER_IS_BETTER else (a > b):
                        score += 1.0
                cats += score
                # the head-to-head is decided on the twelve categories
                if score > N_CATS / 2:
                    wins += 1.0
                elif score == N_CATS / 2:
                    wins += 0.5
            n = len(teams) - 1
            tally[tid]['w'] += wins
            tally[tid]['l'] += n - wins
            tally[tid]['games'] += n
            tally[tid]['weeks'] += 1
            tally[tid]['exp_cats'] += cats / n if n else 0.0
            per_week[w][tid] = wins / n if n else 0.0

    for t in tally.values():
        t['pct'] = t['w'] / t['games'] if t['games'] else 0.0
    return dict(tally), dict(per_week)


# ==============================================================
# Transactions
# ==============================================================
def transaction_summary(data, meta):
    """
    Adds, drops and trades per team.

    Yahoo's own number_of_moves / number_of_trades in the standings are the
    authoritative totals and are used for the headline: the transaction feed
    misses trades that moved only draft picks, because those carry no player
    rows to iterate. The feed is still what supplies every breakdown - by week,
    by position, and the busiest single day.
    """
    by_team = defaultdict(lambda: {
        'adds': 0, 'drops': 0, 'trade_moves': 0, 'trade_ids': set(),
        'by_week': defaultdict(int), 'by_day': defaultdict(int),
        'bat_adds': 0, 'pit_adds': 0, 'players_used': set(),
    })

    # week lookup by date, so an add can be attributed to the scoring week it
    # was made in without assuming a fixed season start
    spans = sorted((date.fromisoformat(m['start']), date.fromisoformat(m['end']), w)
                   for w, m in meta.items() if m.get('start') and m.get('end'))

    def week_of(ts):
        d = date.fromtimestamp(ts)
        for s, e, w in spans:
            if s <= d <= e:
                return w
        return None

    for r in data['transactions']:
        day = date.fromtimestamp(r['timestamp']).isoformat() if r['timestamp'] else None
        wk = week_of(r['timestamp']) if r['timestamp'] else None
        if r['move'] == 'add' and r['to_team']:
            t = by_team[r['to_team']]
            t['adds'] += 1
            t['players_used'].add(r['player'])
            if wk:
                t['by_week'][wk] += 1
            if day:
                t['by_day'][day] += 1
            if r['position_type'] == 'P':
                t['pit_adds'] += 1
            else:
                t['bat_adds'] += 1
        elif r['move'] == 'drop' and r['from_team']:
            by_team[r['from_team']]['drops'] += 1
        elif r['move'] == 'trade':
            for tid in (r['from_team'], r['to_team']):
                if tid:
                    by_team[tid]['trade_moves'] += 1
                    by_team[tid]['trade_ids'].add(r['transaction_id'])

    out = {}
    for t in data['standings']:
        tid = t['team_id']
        b = by_team[tid]
        busiest = max(b['by_day'].items(), key=lambda kv: kv[1]) if b['by_day'] else (None, 0)
        out[tid] = {
            'adds': b['adds'],
            'drops': b['drops'],
            'moves': t['moves'],                    # Yahoo's number, the headline
            'trades': t['trades'],                  # Yahoo's number, picks included
            'trades_with_players': len(b['trade_ids']),
            'players_moved_in_trades': b['trade_moves'],
            'distinct_players_added': len(b['players_used']),
            'bat_adds': b['bat_adds'],
            'pit_adds': b['pit_adds'],
            'busiest_day': busiest[0],
            'busiest_day_moves': busiest[1],
            'adds_by_week': dict(b['by_week']),
        }
    return out


# ==============================================================
# The board
# ==============================================================
def linreg(xs, ys):
    """Least-squares slope and Pearson r; (0, 0) when x has no spread."""
    n = len(xs)
    if n < 3:
        return 0.0, 0.0
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    syy = sum((y - my) ** 2 for y in ys)
    if sxx == 0 or syy == 0:
        return 0.0, 0.0
    return sxy / sxx, sxy / (sxx * syy) ** 0.5


def compute(data, through=None):
    rows, meta = build_weeks(data, through)
    weeks = sorted(rows)
    standings = {t['team_id']: t for t in data['standings']}
    ap, ap_week = all_play(rows)
    tx = transaction_summary(data, meta)

    last6 = weeks[-6:]
    last2 = weeks[-2:]
    first_half = weeks[:len(weeks) // 2]
    second_half = weeks[len(weeks) // 2:]
    ap6, _ = all_play(rows, last6)
    ap2, _ = all_play(rows, last2)
    ap_h1, _ = all_play(rows, first_half)
    ap_h2, _ = all_play(rows, second_half)

    teams = {}
    for tid, st in standings.items():
        wk = [rows[w][tid] for w in weeks if tid in rows[w]]
        mw = sum(1 for r in wk if r['result'] == 'W')
        ml = sum(1 for r in wk if r['result'] == 'L')
        mt = sum(1 for r in wk if r['result'] == 'T')
        pts = [r['pts'] for r in wk]
        ip_weeks = [r for r in wk if r['counts_ip']]
        forfeits = [r for r in ip_weeks if r['forfeit']]

        # Luck, in the currency the standings are actually kept in: category
        # points won, against the category points the same team would have
        # averaged against the whole field. Matchup luck is kept alongside it
        # because it is interesting, but it is NOT the headline - this league
        # seeds on category points, so a manager can be handed a division title
        # by the schedule while their matchup record looks ordinary.
        exp_cats = ap[tid]['exp_cats']
        actual_cats = sum(r['pts'] for r in wk)
        exp_w = ap[tid]['pct'] * len(wk)
        actual_w = mw + 0.5 * mt

        teams[tid] = {
            'team_id': tid,
            'manager': MANAGERS.get(tid, 'Team %d' % tid),
            'name': st['name'],
            'rank': st['rank'],
            'cat_w': st['wins'], 'cat_l': st['losses'], 'cat_t': st['ties'],
            'cat_pts': st['wins'] + 0.5 * st['ties'],
            'cat_pct': st['pct'],
            'matchup_w': mw, 'matchup_l': ml, 'matchup_t': mt,
            'weeks': len(wk),
            'allplay_w': round(ap[tid]['w'], 1),
            'allplay_l': round(ap[tid]['l'], 1),
            'allplay_pct': ap[tid]['pct'],
            'allplay_pct_l6': ap6.get(tid, {}).get('pct', 0.0),
            'allplay_pct_l2': ap2.get(tid, {}).get('pct', 0.0),
            'allplay_pct_h1': ap_h1.get(tid, {}).get('pct', 0.0),
            'allplay_pct_h2': ap_h2.get(tid, {}).get('pct', 0.0),
            'avg_pts': statistics.fmean(pts) if pts else 0.0,
            'stdev_pts': statistics.pstdev(pts) if len(pts) > 1 else 0.0,
            'best_week': max(wk, key=lambda r: r['pts'])['week'] if wk else None,
            'best_week_pts': max(pts) if pts else 0.0,
            'worst_week': min(wk, key=lambda r: r['pts'])['week'] if wk else None,
            'worst_week_pts': min(pts) if pts else 0.0,
            'cat_luck': actual_cats - exp_cats,
            'expected_cats': exp_cats,
            'actual_cats': actual_cats,
            'luck': actual_w - exp_w,
            'expected_wins': exp_w,
            'ip_forfeits': len(forfeits),
            'ip_forfeit_weeks': [r['week'] for r in forfeits],
            'ip_short_total': round(sum(r['ip_short'] for r in ip_weeks), 1),
            'avg_ip': statistics.fmean([r['ip'] for r in ip_weeks]) if ip_weeks else 0.0,
            'min_ip_week': min(ip_weeks, key=lambda r: r['ip'])['ip'] if ip_weeks else 0.0,
            'weekly': [{'week': r['week'], 'pts': r['pts'], 'opp': r['opponent'],
                        'result': r['result'], 'ip': r['ip'], 'forfeit': r['forfeit'],
                        'allplay': round(ap_week[r['week']][tid], 3)} for r in wk],
        }
        teams[tid].update(tx.get(tid, {}))

    # --- upsets and opponent-strength response -------------------------------
    for tid, t in teams.items():
        wk = [rows[w][tid] for w in weeks if tid in rows[w]]
        upsets, upset_detail, beat_by = 0, [], []
        xs, ys = [], []
        strong, weak = [], []
        median = statistics.median([teams[o]['allplay_pct'] for o in teams])
        for r in wk:
            opp = teams[r['opponent']]
            gap = opp['allplay_pct'] - t['allplay_pct']
            xs.append(opp['allplay_pct'])
            ys.append(r['pts'])
            (strong if opp['allplay_pct'] >= median else weak).append(r['pts'])
            if r['result'] == 'W' and gap > 0:
                upsets += 1
                upset_detail.append({'week': r['week'], 'opp': opp['manager'],
                                     'score': '%g-%g' % (r['cats'], r['opp_cats']),
                                     'gap': round(gap, 3)})
            if r['result'] == 'L' and gap < 0:
                beat_by.append({'week': r['week'], 'opp': opp['manager'],
                                'score': '%g-%g' % (r['cats'], r['opp_cats']),
                                'gap': round(-gap, 3)})
        slope, r_val = linreg(xs, ys)
        upset_detail.sort(key=lambda d: -d['gap'])
        beat_by.sort(key=lambda d: -d['gap'])
        t.update({
            'upsets': upsets,
            'upset_detail': upset_detail[:5],
            'upset_quality': round(sum(d['gap'] for d in upset_detail), 3),
            'losses_to_worse': len(beat_by),
            'worst_losses': beat_by[:5],
            'slope': slope,
            'slope_r': r_val,
            'vs_strong': statistics.fmean(strong) if strong else 0.0,
            'vs_weak': statistics.fmean(weak) if weak else 0.0,
            'split': (statistics.fmean(strong) if strong else 0.0)
                     - (statistics.fmean(weak) if weak else 0.0),
        })

    # Every team's points fall as the opponent gets better - that is what a
    # matchup is. The interesting number is therefore not the slope itself but
    # the slope against the league's, which is what separates a manager who
    # holds up against good teams from one who only feasts on bad ones.
    league_slope = statistics.fmean(t['slope'] for t in teams.values())
    league_split = statistics.fmean(t['split'] for t in teams.values())
    for t in teams.values():
        t['slope_rel'] = t['slope'] - league_slope
        t['split_rel'] = t['split'] - league_split

    return {
        'weeks': weeks,
        'week_meta': meta,
        'teams': teams,
        'rows': rows,
        'allplay_week': ap_week,
        'league_slope': league_slope,
        'league_split': league_split,
        'records': league_records(rows, weeks, teams),
    }


def league_records(rows, weeks, teams):
    """Single-week extremes worth calling out on the board."""
    flat = [r for w in weeks for r in rows[w].values()]
    if not flat:
        return {}

    def who(r):
        return teams[r['team_id']]['manager']

    # Four genuinely different facts. 'Best week' and 'biggest blowout' are the
    # same row whenever a 12-0 exists - both are just max(pts) - and so are
    # 'worst week' and its mirror, so neither pairing earns two tiles.
    best = max(flat, key=lambda r: r['pts'])
    # Biggest jump between two consecutive weeks. 'Best week in a defeat' was
    # the obvious fourth tile and is a dud: a losing week is under 6 points by
    # definition, so the record is always about 5.5 and always the mirror of
    # some other tile's matchup.
    swing = {'gain': 0}
    for tid in teams:
        prev = None
        for w in weeks:
            r = rows[w].get(tid)
            if r and prev and r['pts'] - prev['pts'] > swing['gain']:
                swing = {'gain': r['pts'] - prev['pts'], 'tid': tid,
                         'from': prev, 'to': r}
            prev = r or prev
    # the win over the strongest opponent, by the gap in all-play rate
    ups = [r for r in flat if r['result'] == 'W'
           and teams[r['opponent']]['allplay_pct'] > teams[r['team_id']]['allplay_pct']]
    upset = max(ups, key=lambda r: teams[r['opponent']]['allplay_pct']
                - teams[r['team_id']]['allplay_pct']) if ups else None

    def pack(r, label, extra=''):
        if not r:
            return None
        return {'label': label, 'manager': who(r), 'week': r['week'],
                'opponent': teams[r['opponent']]['manager'],
                'score': '%g-%g' % (r['cats'], r['opp_cats']),
                'ties': r['ties'], 'pts': r['pts'], 'extra': extra}

    # Longest run of won weeks, which no other tile can duplicate because it is
    # the only one that spans more than a single week.
    streak = {'len': 0}
    for tid in teams:
        run = 0
        for w in weeks:
            r = rows[w].get(tid)
            if r and r['result'] == 'W':
                run += 1
                if run > streak['len']:
                    streak = {'len': run, 'tid': tid, 'end': w, 'start': w - run + 1}
            else:
                run = 0

    return {
        'best_week': pack(best, 'Best single week'),
        'swing': ({'label': 'Biggest week-to-week swing',
                   'manager': teams[swing['tid']]['manager'],
                   'score': '+%g' % swing['gain'],
                   'week': swing['to']['week'], 'ties': 0, 'pts': None,
                   'opponent': None,
                   'span': 'week %d (%g pts) to week %d (%g pts)' % (
                       swing['from']['week'], swing['from']['pts'],
                       swing['to']['week'], swing['to']['pts']),
                   'extra': ''} if swing['gain'] else None),
        'upset': pack(upset, 'Biggest upset',
                      'beat a team %d points of all-play better' % round(1000 * (
                          teams[upset['opponent']]['allplay_pct']
                          - teams[upset['team_id']]['allplay_pct'])) if upset else ''),
        'streak': ({'label': 'Longest winning streak',
                    'manager': teams[streak['tid']]['manager'],
                    'score': '%d wks' % streak['len'],
                    'week': streak['end'], 'ties': 0, 'pts': None,
                    'opponent': None,
                    'span': 'weeks %d-%d' % (streak['start'], streak['end']),
                    'extra': ''} if streak['len'] else None),
    }
