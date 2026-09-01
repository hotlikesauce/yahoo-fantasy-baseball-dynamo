"""
Playoff seeding, bracket and matchup prediction for the 2026 postseason.

The prediction model is a **week bootstrap**. To play a matchup, draw one whole
week each team actually had, and score its twelve categories against the other.
Drawing a whole week rather than each category on its own is the entire point:
a team's ERA, WHIP and K/9 all move together with how its starters went, and a
per-category normal would happily deal a team the best ERA it ever posted
alongside the fewest strikeouts, a week that has never happened and never will.

Three windows are reported because they answer different questions:

    season  - all completed weeks, the biggest sample and the least noisy
    last 8  - the shape of the roster after the trade deadline
    last 4  - who is hot, on the shortest window that is still a sample

and a blend that mixes the three, because none of them is right alone.

The short window is four weeks, not two, and that floor is deliberate. The
bootstrap draws one whole week per team, so a two-week window has a pool of two
and only 2x2 = 4 distinct outcomes: every probability it can emit is a multiple
of 0.25, and it routinely emitted 0.0 and 1.0 - certainties - off four samples,
carrying 20% of the blend while it did. Running SIMS draws against that pool
does not help; the resolution is set by the pool, not the draw count. Four weeks
gives 16 outcomes and a 0.0625 floor. Do not shorten this window again.

The 50-innings rule is simulated, not smoothed over: if a drawn week came in
under 50 IP, that team forfeits all six pitching categories in that draw,
exactly as Yahoo scores it. Managers who habitually run short carry that risk
into their odds instead of getting credit for the pretty rate stats they put up
on 35 innings.
"""

import random
from collections import defaultdict

import superlatives_core as sc

CATS = sc.CATS
LOWER_IS_BETTER = sc.LOWER_IS_BETTER
PITCHING = sc.PITCHING
N_CATS = sc.N_CATS
MIN_IP = sc.MIN_IP

# How the blended number weights the three windows. The season carries the most
# because it is the only window with enough weeks to mean anything; the short
# windows get a real but minority say so a genuinely hot team is not ignored.
BLEND = {'season': 0.50, 'last8': 0.30, 'last4': 0.20}

SIMS = 20000
SEED = 17


def windows(weeks):
    return {
        'season': list(weeks),
        'last8': list(weeks[-8:]),
        'last4': list(weeks[-4:]),
    }


def week_pool(rows, tid, weeks):
    """The weeks a team actually played, as draw-able samples."""
    return [rows[w][tid] for w in weeks if tid in rows[w]]


def score_pair(a, b):
    """
    Twelve categories between two drawn weeks. Returns team A's points, with a
    tied category worth half to each, and a sub-50-IP week forfeiting its six
    pitching categories outright.
    """
    a_forfeit = a['counts_ip'] and a['ip'] < MIN_IP
    b_forfeit = b['counts_ip'] and b['ip'] < MIN_IP
    pts = 0.0
    for c in CATS:
        if c in PITCHING and (a_forfeit or b_forfeit):
            # a double forfeit splits them; otherwise the side that made the
            # innings takes all six
            if a_forfeit and b_forfeit:
                pts += 0.5
            elif b_forfeit:
                pts += 1.0
            continue
        x, y = a['stats'][c], b['stats'][c]
        if x == y:
            pts += 0.5
        elif (x < y) if c in LOWER_IS_BETTER else (x > y):
            pts += 1.0
    return pts


BAT_COUNT = ['R', 'H', 'HR', 'RBI', 'SB']
PIT_COUNT = ['TB', 'QS', 'SVH']
RATE_PIT = ['ERA', 'WHIP', 'K9']


class LiveWeek:
    """
    A week that is underway: score the rest of it on top of what is banked.

    A part-played week is not a coin flip and it is not a decided result, and
    the difference between those two is the whole point of this class. Up 7-1 in
    home runs with two days left is very nearly a lock; up 1-0 on the Monday is
    still a toss-up. Both look identical to a model that only reads "leading".

    So every draw here scores the categories the way they will actually finish:
    banked totals plus a fraction of a bootstrapped week, and the fraction is
    how much of the week each team has left. Progress is measured in AB and IP
    against that team's own season averages rather than in calendar days,
    because a team that has already burned four starts has less left than the
    date implies.

    Counting categories add. Rates are re-weighted, not averaged: a 3.48 ERA
    over 10 innings and a 4.50 over the next 50 is a 4.33 week, and averaging
    those two numbers would say 3.99. The 50-IP minimum is applied to the
    PROJECTED innings, so a team on 8 IP on Monday is not treated as forfeiting
    - it has all week to get there.
    """

    def __init__(self, rows, weeks, live_rows):
        self.live = live_rows
        self.avg_ab, self.avg_ip = {}, {}
        for tid in {t for w in weeks for t in rows[w]}:
            abs_ = [rows[w][tid]['ab'] for w in weeks if tid in rows[w]]
            ips = [rows[w][tid]['ip'] for w in weeks
                   if tid in rows[w] and rows[w][tid]['counts_ip']]
            self.avg_ab[tid] = (sum(abs_) / len(abs_)) if abs_ else 0.0
            self.avg_ip[tid] = (sum(ips) / len(ips)) if ips else 0.0

    def fractions(self, tid):
        """How much of this team's normal week is still to come, per side."""
        cur = self.live[tid]
        f_bat = 1.0 - (cur['ab'] / self.avg_ab[tid] if self.avg_ab[tid] else 0.0)
        f_pit = 1.0 - (cur['ip'] / self.avg_ip[tid] if self.avg_ip[tid] else 0.0)
        return max(0.0, min(1.0, f_bat)), max(0.0, min(1.0, f_pit))

    def project(self, tid, draw):
        """Banked totals plus `fraction` of one bootstrapped week."""
        cur = self.live[tid]
        f_bat, f_pit = self.fractions(tid)
        # Counting stats are ROUNDED back to whole numbers. Nobody hits 5.16
        # home runs, and leaving them fractional quietly destroys ties - two
        # teams can then never finish level in a category, so a one-homer lead
        # on the Monday converts almost directly into a category win. With
        # rounding, a 1-0 lead is worth what it should be: a nudge, not a result.
        out = {}
        for c in BAT_COUNT:
            out[c] = round(cur['stats'][c] + f_bat * draw['stats'][c])
        for c in PIT_COUNT:
            out[c] = round(cur['stats'][c] + f_pit * draw['stats'][c])

        # Rates re-weighted by the innings/at-bats behind each piece.
        add_ip = f_pit * draw['ip']
        tot_ip = cur['ip'] + add_ip
        for c in RATE_PIT:
            out[c] = ((cur['stats'][c] * cur['ip'] + draw['stats'][c] * add_ip) / tot_ip
                      if tot_ip else draw['stats'][c])
        add_ab = f_bat * draw['ab']
        tot_ab = cur['ab'] + add_ab
        out['OPS'] = ((cur['stats']['OPS'] * cur['ab'] + draw['stats']['OPS'] * add_ab) / tot_ab
                      if tot_ab else draw['stats']['OPS'])
        return out, tot_ip

    def matchup(self, pred, a, b, sims=SIMS):
        """Same shape as Predictor.matchup, but finishing a part-played week."""
        wins = ties = 0
        total = 0.0
        cat_wins = defaultdict(float)
        for _ in range(sims):
            pa, ip_a = self.project(a, pred.draw(a))
            pb, ip_b = self.project(b, pred.draw(b))
            fa, fb = ip_a < MIN_IP, ip_b < MIN_IP
            pts = 0.0
            for c in CATS:
                if c in PITCHING and (fa or fb):
                    got = 0.5 if (fa and fb) else (1.0 if fb else 0.0)
                elif pa[c] == pb[c]:
                    got = 0.5
                elif (pa[c] < pb[c]) if c in LOWER_IS_BETTER else (pa[c] > pb[c]):
                    got = 1.0
                else:
                    got = 0.0
                pts += got
                cat_wins[c] += got
            total += pts
            if pts > N_CATS / 2:
                wins += 1
            elif pts == N_CATS / 2:
                ties += 1
        n = float(sims)
        return {
            'win': wins / n, 'tie': ties / n, 'loss': (n - wins - ties) / n,
            'exp_pts': total / n, 'cats': {c: cat_wins[c] / n for c in CATS},
        }

    def play(self, pred, a, b):
        """One simulated finish of this matchup. Returns team A's points."""
        pa, ip_a = self.project(a, pred.draw(a))
        pb, ip_b = self.project(b, pred.draw(b))
        fa, fb = ip_a < MIN_IP, ip_b < MIN_IP
        pts = 0.0
        for c in CATS:
            if c in PITCHING and (fa or fb):
                pts += 0.5 if (fa and fb) else (1.0 if fb else 0.0)
                continue
            x, y = pa[c], pb[c]
            if x == y:
                pts += 0.5
            elif (x < y) if c in LOWER_IS_BETTER else (x > y):
                pts += 1.0
        return pts


def live_bracket_week(data, week, field, rows):
    """
    The part-played bracket games for `week`, or None if there are none.

    Consolation games and bye filler are excluded by `field`, the same guard
    real_bracket_round uses - a consolation result must never move the bracket.
    """
    wk = data.get('weeks', {}).get(str(week)) or []
    live_rows, pairs = {}, []
    for m in wk:
        if not m.get('is_playoffs') or m.get('is_consolation'):
            continue
        if m['status'] != 'midevent':
            continue
        a, b = m['a']['team_id'], m['b']['team_id']
        if a not in field or b not in field:
            continue
        pairs.append((a, b))
        for side in (m['a'], m['b']):
            live_rows[side['team_id']] = {
                'ip': side['ip'],
                'ab': sc.at_bats(side['stats'].get('HAB')),
                'stats': {c: sc.to_float(c, side['stats'].get(c)) for c in CATS},
            }
    if not pairs:
        return None, None
    return pairs, live_rows


class Predictor:
    """Bootstrap sampler over one window, shared by matchup and bracket sims."""

    def __init__(self, rows, weeks, window, sims=SIMS, seed=SEED):
        self.sims = sims
        self.rng = random.Random(seed)
        self.window = window
        self.pool = {}
        for tid in {t for w in weeks for t in rows[w]}:
            p = week_pool(rows, tid, window) or week_pool(rows, tid, weeks)
            self.pool[tid] = p

    def draw(self, tid):
        return self.rng.choice(self.pool[tid])

    def matchup(self, a, b):
        """P(a wins), P(tie), expected category points, and per-category rates."""
        wins = ties = 0
        total = 0.0
        cat_wins = defaultdict(float)
        for _ in range(self.sims):
            wa, wb = self.draw(a), self.draw(b)
            pts = score_pair(wa, wb)
            total += pts
            if pts > N_CATS / 2:
                wins += 1
            elif pts == N_CATS / 2:
                ties += 1
            a_f = wa['counts_ip'] and wa['ip'] < MIN_IP
            b_f = wb['counts_ip'] and wb['ip'] < MIN_IP
            for c in CATS:
                if c in PITCHING and (a_f or b_f):
                    cat_wins[c] += 0.5 if (a_f and b_f) else (0.0 if a_f else 1.0)
                    continue
                x, y = wa['stats'][c], wb['stats'][c]
                if x == y:
                    cat_wins[c] += 0.5
                elif (x < y) if c in LOWER_IS_BETTER else (x > y):
                    cat_wins[c] += 1.0
        n = float(self.sims)
        return {
            'win': wins / n,
            'tie': ties / n,
            'loss': (n - wins - ties) / n,
            'exp_pts': total / n,
            'cats': {c: cat_wins[c] / n for c in CATS},
        }

    def play(self, a, b):
        """One drawn matchup, ties broken by the higher seed (the league rule)."""
        pts = score_pair(self.draw(a), self.draw(b))
        if pts > N_CATS / 2:
            return a
        if pts < N_CATS / 2:
            return b
        return None          # caller applies the seeding tiebreak


class BlendPredictor:
    """
    The blended number, as a mixture rather than an average of three answers.

    Averaging three win probabilities and sampling from a weighted mixture of
    the three week pools are not the same thing, and the mixture is the right
    one: it is a single coherent model of 'a week this team might have', not
    three models glued together after the fact.
    """

    def __init__(self, rows, weeks, sims=SIMS, seed=SEED):
        self.sims = sims
        self.rng = random.Random(seed)
        wins = windows(weeks)
        self.pool, self.weights = {}, {}
        for tid in {t for w in weeks for t in rows[w]}:
            samples, wts = [], []
            for name, weight in BLEND.items():
                pool = week_pool(rows, tid, wins[name])
                if not pool:
                    continue
                for r in pool:
                    samples.append(r)
                    wts.append(weight / len(pool))
            self.pool[tid] = samples
            self.weights[tid] = wts

    def draw(self, tid):
        return self.rng.choices(self.pool[tid], weights=self.weights[tid], k=1)[0]

    matchup = Predictor.matchup
    play = Predictor.play


# ==============================================================
# Seeding
# ==============================================================
def seed_teams(data, res, playoff_teams=6):
    """
    Final regular-season order, live week included.

    Yahoo's standings block only counts completed weeks, so while the last week
    is still running the seeds have to be projected: banked points plus what
    the live scoreboard already shows. Once that week goes postevent the live
    part is zero and this returns the real thing - the same code path either
    way, with `final` saying which it was.
    """
    teams = res['teams']
    counted = set(res['weeks'])
    live = defaultdict(float)
    live_week = None
    pending = False

    for wk, matchups in data['weeks'].items():
        w = int(wk)
        if w in counted or not matchups:
            continue
        if any(m.get('is_playoffs') for m in matchups):
            continue
        live_week = w if live_week is None else min(live_week, w)
        for m in matchups:
            if m['status'] != 'postevent':
                pending = True
            ties = N_CATS - m['a']['cats'] - m['b']['cats']
            for side in (m['a'], m['b']):
                live[side['team_id']] += side['cats'] + 0.5 * ties

    rows = []
    for tid, t in teams.items():
        rows.append({
            'team_id': tid,
            'manager': t['manager'],
            'name': t['name'],
            'banked': t['cat_pts'],
            'live': live.get(tid, 0.0),
            'pts': t['cat_pts'] + live.get(tid, 0.0),
            'allplay_pct': t['allplay_pct'],
            'allplay_pct_l8': t['allplay_pct_l8'],
            'allplay_pct_l4': t['allplay_pct_l4'],
        })
    # ties on points are broken head-to-head on categories; all-play is the
    # stand-in here and is flagged on the page rather than pretended away
    rows.sort(key=lambda r: (-r['pts'], -r['allplay_pct']))
    for i, r in enumerate(rows, 1):
        r['seed'] = i
        r['in'] = i <= playoff_teams
    return {
        'seeds': rows,
        'final': not pending,
        'live_week': live_week,
        'playoff_teams': playoff_teams,
    }


# ==============================================================
# Bracket
# ==============================================================
def default_bracket(seeds, playoff_weeks):
    """
    Six teams over three weeks, with reseeding.

    Round 1 the top two sit out and 3-6 / 4-5 play. Round 2 the highest
    surviving seed draws the lowest surviving seed, which is what reseeding
    means and why a 6 seed upsetting a 3 does not hand the 2 seed an easier
    week - it hands it a harder one.
    """
    inn = [s for s in seeds if s['in']]
    by_seed = {s['seed']: s for s in inn}
    r1, r2, r3 = playoff_weeks
    return {
        'weeks': {'quarter': r1, 'semi': r2, 'final': r3},
        'byes': [by_seed[1]['team_id'], by_seed[2]['team_id']],
        'quarters': [
            {'week': r1, 'a': by_seed[3]['team_id'], 'b': by_seed[6]['team_id']},
            {'week': r1, 'a': by_seed[4]['team_id'], 'b': by_seed[5]['team_id']},
        ],
        'reseeded': True,
    }


def head_to_head(rows, weeks):
    """
    Season head-to-head between every pair, counted in CATEGORIES.

    This is the league's tiebreak, and it is not the same as who won more of
    the meetings: two weeks of 7-5 and 4-8 look like a 1-1 split but are 11-13
    on categories, and the categories decide it. Ties inside a week are already
    folded into `pts` at half a point each.

    Returns {(low_id, high_id): {'a': cats, 'b': cats, 'n': meetings}} keyed
    with the lower team id first.
    """
    h = {}
    for w in weeks:
        for tid, r in rows[w].items():
            o = r['opponent']
            if o is None or tid > o:
                continue
            key = (tid, o)
            rec = h.setdefault(key, {'a': 0.0, 'b': 0.0, 'n': 0})
            rec['a'] += r['pts']
            rec['b'] += N_CATS - r['pts']       # the two sides always sum to 12
            rec['n'] += 1
    return h


def h2h_winner(h2h, a, b):
    """
    Which of `a`/`b` the season series favours, or None when it is level or
    they never met. Callers fall back to seed order on None.
    """
    key = (a, b) if a < b else (b, a)
    rec = h2h.get(key)
    if not rec or not rec['n']:
        return None
    ca, cb = (rec['a'], rec['b']) if a < b else (rec['b'], rec['a'])
    if ca > cb:
        return a
    if cb > ca:
        return b
    return None


def real_bracket_round(data, week, field=None):
    """
    The actual pairings Yahoo posted for a playoff week, if it has posted them.

    Once the postseason starts this is the truth and the projection is not, so
    every round checks here first.

    `field` is the set of team ids actually in the bracket, and passing it is
    close to mandatory. All twelve teams play every week of the postseason: the
    six in the bracket, and the six in the consolation round. Yahoo's
    `is_consolation` flag sorts most of that out, but a bye team still gets put
    on the schedule against somebody, and a matchup that pairs a bracket team
    with a non-bracket team is that filler - not a playoff game. Without the
    field check, filler is read as a real pairing and the bye silently
    disappears, which turns the 1 seed's three-week path into a two-week one and
    quietly wrecks every number on the page.
    """
    matchups = data['weeks'].get(str(week)) or []
    out = []
    for m in matchups:
        if not m.get('is_playoffs') or m.get('is_consolation'):
            continue
        a, b = m['a']['team_id'], m['b']['team_id']
        if field is not None and not (a in field and b in field):
            continue
        out.append({
            'week': week,
            'a': a, 'b': b,
            'a_cats': m['a']['cats'], 'b_cats': m['b']['cats'],
            'status': m['status'],
            'winner': m['winner'],
        })
    return out


def simulate_bracket(pred, seeds, bracket, data, h2h=None, sims=SIMS, seed=SEED,
                     rows=None, weeks=None):
    """
    Run the whole postseason. Returns each team's chance of reaching each round
    and of winning it.

    A drawn week goes to whoever won the SEASON SERIES ON CATEGORIES, which is
    the league's tiebreak - not to the better seed. Those two disagree on four
    of the fifteen pairings in this field, so using seed order as a shortcut
    quietly hands several ties to the wrong team. Seed order is only the last
    resort, for a pair that is level head-to-head or never met.
    """
    rng = random.Random(seed)
    inn = [s for s in seeds if s['in']]
    seed_of = {s['team_id']: s['seed'] for s in inn}
    r1, r2, r3 = bracket['weeks']['quarter'], bracket['weeks']['semi'], bracket['weeks']['final']

    # Anything Yahoo has already decided is fixed, not simulated. The field is
    # passed so a consolation or bye-filler game can never be mistaken for a
    # bracket result.
    field = {s['team_id'] for s in inn}
    known = {}
    for wk in (r1, r2, r3):
        for m in real_bracket_round(data, wk, field):
            if m['winner'] is not None and m['status'] == 'postevent':
                known[(wk, m['a'], m['b'])] = m['winner']

    # A bracket week that is underway is scored from where it actually stands,
    # not re-simulated from scratch. Without this a team leading 10-0 on the
    # Monday is still quoted at its season-strength number, which is simply
    # wrong by Wednesday and indefensible by Saturday.
    live_pairs, live_rows = (None, None)
    if rows is not None and weeks:
        live_pairs, live_rows = live_bracket_week(data, r1, field, rows)
    live = LiveWeek(rows, weeks, live_rows) if live_pairs else None
    live_set = {frozenset(p) for p in (live_pairs or [])}

    def break_tie(a, b):
        """Season series on categories first; seed order only if that is level."""
        if h2h:
            w = h2h_winner(h2h, a, b)
            if w is not None:
                return w
        return a if seed_of.get(a, 99) < seed_of.get(b, 99) else b

    def resolve(a, b, wk):
        for key in ((wk, a, b), (wk, b, a)):
            if key in known:
                return known[key]
        if live and wk == r1 and frozenset((a, b)) in live_set:
            pts = live.play(pred, a, b)
            return a if pts > N_CATS / 2 else b if pts < N_CATS / 2 else break_tie(a, b)
        w = pred.play(a, b)
        if w is None:
            w = break_tie(a, b)
        return w

    reach = {s['team_id']: {'semi': 0, 'final': 0, 'title': 0} for s in inn}
    for _ in range(sims):
        # round 1
        alive = list(bracket['byes'])
        for q in bracket['quarters']:
            alive.append(resolve(q['a'], q['b'], r1))
        for t in alive:
            reach[t]['semi'] += 1

        # round 2, reseeded: best remaining seed vs worst remaining seed
        alive.sort(key=lambda t: seed_of.get(t, 99))
        finalists = [resolve(alive[0], alive[-1], r2), resolve(alive[1], alive[2], r2)]
        for t in finalists:
            reach[t]['final'] += 1

        champ = resolve(finalists[0], finalists[1], r3)
        reach[champ]['title'] += 1

    n = float(sims)
    return {tid: {k: v / n for k, v in d.items()} for tid, d in reach.items()}
