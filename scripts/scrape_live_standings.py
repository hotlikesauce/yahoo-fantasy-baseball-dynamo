#!/usr/bin/env python3
"""
Scrape live standings + current-week matchups from the PUBLIC Yahoo league page
and generate docs/live_standings_2026.html.

The Yahoo Fantasy API has been dead app-wide since ~2026-07-26 (every endpoint
403s, refresh tokens are gone), so pull_live_standings / serve_live_standings
Lambdas return {"error": "Failed to get access token"}. The league is set to
"Make League Publicly Viewable: Yes", so the league home page renders standings
and the live scoreboard to anonymous requests - that is what this scrapes.

Only the league HOME page (/b1/<id>) renders anonymously; /standings, /matchup
and /scoreboard come back as empty shells. Manager names are not public either,
so they come from DynamoDB FantasyBaseball-TeamInfo-2026.

Usage:
    python scripts/scrape_live_standings.py            # scrape + write page
    python scripts/scrape_live_standings.py --dry-run  # scrape + print, no write
"""

import os, sys, io, re, json, html, math, random, argparse
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(REPO, 'docs')

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

CATS_PER_WEEK = 12          # 6 batting + 6 pitching categories
SIMS = 100000               # Monte Carlo runs (enough to make a tenth of a
                            # percent mean something; the seed is fixed so the
                            # digit only moves when the data does)
YEAR = 2026


# ==============================================================
# Fetch
# ==============================================================
def league_id():
    pairs = os.getenv('YAHOO_LEAGUE_IDS', '')
    for pair in pairs.split(','):
        if pair.startswith(f'{YEAR}:'):
            return pair.split(':')[1]
    sys.exit(f'ERROR: no {YEAR} league id in .env YAHOO_LEAGUE_IDS')


def get(url):
    r = requests.get(url, headers={'User-Agent': UA}, timeout=30)
    r.raise_for_status()
    return r.text


def clean(s):
    """Unescape entities and collapse whitespace in scraped text."""
    return re.sub(r'\s+', ' ', html.unescape(s)).strip()


# ==============================================================
# Parse: league home page
# ==============================================================
def parse_standings(page, lid):
    """Standings table: rank (* = Yahoo says clinched), team, W-L-T, pct, GB."""
    rows = []
    row_re = re.compile(
        r'<tr class="Linkable[^"]*"[^>]*data-target="/b1/%s/(\d+)">(.*?)</tr>' % lid,
        re.S)
    for m in row_re.finditer(page):
        tid, body = int(m.group(1)), m.group(2)
        cells = [clean(re.sub(r'<[^>]+>', ' ', c))
                 for c in re.findall(r'<td[^>]*>(.*?)</td>', body, re.S)]
        if len(cells) < 5:
            continue
        rank_txt = cells[0]
        wlt = re.match(r'(\d+)-(\d+)-(\d+)', cells[2])
        if not wlt:
            continue
        name_m = re.search(r'/b1/%s/%d">([^<]+)</a>' % (lid, tid), body)
        w, l, t = (int(x) for x in wlt.groups())
        rows.append({
            'team_id': tid,
            'name': clean(name_m.group(1)) if name_m else f'Team {tid}',
            'rank': int(re.sub(r'\D', '', rank_txt) or 0),
            'yahoo_clinched': '*' in rank_txt,
            'wins': w, 'losses': l, 'ties': t,
            'pct': cells[3],
            'gb': cells[4],
            'last_week': cells[5] if len(cells) > 5 else '',
        })
    rows.sort(key=lambda r: r['rank'])
    return rows


def parse_matchups(page, lid):
    """Current-week scoreboard: team ids, names, records and live category wins."""
    blocks = re.split(r"data-target='/b1/%s/matchup\?" % lid, page)[1:]
    week = None
    matchups = []
    for b in blocks:
        head = re.match(r'week=(\d+)&mid1=(\d+)&mid2=(\d+)', b)
        if not head:
            continue
        week = int(head.group(1))
        body = b[:6000]
        names = re.findall(r'/b1/%s/(\d+)">([^<]+)</a>' % lid, body)
        scores = re.findall(r"Fz-lg ?'>\s*(\d+)\s*</div>", body)
        if len(names) < 2 or len(scores) < 2:
            continue
        # records come from the standings table, not the scoreboard card
        sides = [{'team_id': int(names[i][0]),
                  'name': clean(names[i][1]),
                  'live': int(scores[i])} for i in (0, 1)]
        decided = sides[0]['live'] + sides[1]['live']
        matchups.append({
            'week': week,
            'a': sides[0], 'b': sides[1],
            'decided': decided,
            'remaining': max(0, CATS_PER_WEEK - decided),
        })
    return week, matchups


def parse_meta(page):
    status = 'unknown'
    m = re.search(r"""Ta-end Fz-xxs["']>\s*<span[^>]*>([^<]+)</span>""", page, re.S)
    if m:
        status = clean(m.group(1))
    updated = ''
    m = re.search(r'Last standings update:\s*([^<]+)<', page)
    if m:
        updated = clean(m.group(1))
    name = ''
    m = re.search(r'<title>([^<]*?)\s*\|', page)
    if m:
        name = clean(m.group(1))
    return {'status': status, 'yahoo_updated': updated, 'league_name': name}


def parse_playoff_settings(settings_page):
    """'Playoffs: 6 teams - Week 23, 24 and 25 (ends Sunday, Sep 20)'"""
    txt = re.sub(r'(?s)<script.*?</script>', '', settings_page)
    txt = clean(re.sub(r'<[^>]+>', ' ', txt))
    m = re.search(r'Playoffs:\s*(\d+)\s*teams?\s*-\s*Week\s*([\d,\s and]+)', txt)
    if not m:
        return {'spots': 6, 'first_playoff_week': None, 'raw': ''}
    weeks = [int(x) for x in re.findall(r'\d+', m.group(2))]
    return {
        'spots': int(m.group(1)),
        'first_playoff_week': min(weeks) if weeks else None,
        'playoff_weeks': weeks,
        'raw': clean(m.group(0)).replace('Playoffs:', '').strip(),
    }


# ==============================================================
# Managers (DynamoDB - Yahoo does not expose them publicly)
# ==============================================================
def load_managers():
    try:
        import boto3
        tbl = boto3.resource('dynamodb', region_name='us-west-2') \
                   .Table(f'FantasyBaseball-TeamInfo-{YEAR}')
        out = {}
        for item in tbl.scan().get('Items', []):
            mgr = (item.get('ManagerName') or '').strip()
            if mgr:
                out[int(item['TeamNumber'])] = mgr.split()[0].capitalize()
        return out
    except Exception as e:
        print(f'  (no manager names: {e})')
        return {}


# ==============================================================
# Playoff math
# ==============================================================
def week_progress(status, now=None):
    """
    How much of the scoring week is in the books, 0..1. Yahoo weeks run Monday
    through Sunday. Nothing is banked until the week closes: a team up 8-3 on
    Monday night can be down 3-8 by Sunday, so a live lead only counts for as
    much of the week as has actually been played.
    """
    if status.lower().startswith('final') or status.lower() == 'postevent':
        return 1.0
    if 'coming up' in status.lower() or 'pre' in status.lower():
        return 0.0
    now = now or datetime.now()
    elapsed = now.weekday() * 24 + now.hour + now.minute / 60.0
    return min(1.0, max(0.0, elapsed / (7 * 24)))


def build_teams(standings, matchups, weeks_left_after, spots, progress):
    """
    Merge the season record with this week's live score.

    `progress` is the share of the week already played. While the week is live
    every one of its 12 categories is still winnable, so the floor/ceiling range
    stays wide - only a finished week banks its categories.
    """
    opp, live, decided = {}, {}, {}
    for m in matchups:
        for side, other in ((m['a'], m['b']), (m['b'], m['a'])):
            opp[side['team_id']] = other['team_id']
            live[side['team_id']] = side['live']
            decided[side['team_id']] = m['decided']

    week_over = progress >= 1.0
    teams = {}
    for row in standings:
        tid = row['team_id']
        pts = row['wins'] + 0.5 * row['ties']
        games = row['wins'] + row['losses'] + row['ties']
        banked = live.get(tid, 0) if week_over else 0
        pool = 0 if week_over else CATS_PER_WEEK    # all 12 stay in play
        free = weeks_left_after * CATS_PER_WEEK     # later weeks (opponents unknown)
        teams[tid] = dict(row,
                          pts=pts,
                          games=games,
                          cat_pct=pts / games if games else 0.5,
                          live=live.get(tid, 0),
                          decided=decided.get(tid, 0),
                          banked=banked,
                          pool=pool,
                          free=free,
                          remaining=pool + free,
                          opponent=opp.get(tid),
                          floor=pts + banked,
                          ceiling=pts + banked + pool + free)
    return teams


def teams_above(teams, tid, final, mode):
    """
    How many teams can finish above `final` if everything else breaks the worst
    way ('max') or the best way ('min') for team `tid`.

    A matchup only moves its own two teams, so the extreme can be taken matchup
    by matchup instead of enumerating the whole league. Categories in weeks
    after this one ('free') have no known opponent, so they swing the full
    amount in max mode and none of it in min mode.
    """
    # clinch math counts a tie as a team above (the tiebreak could go either
    # way); elimination math gives the tie to the team being tested.
    beats = (lambda x: x >= final) if mode == 'max' else (lambda x: x > final)
    me = teams[tid]
    my_share = final - me['floor']          # cats I take out of my own matchup

    seen, total = set(), 0
    for a in teams.values():
        aid = a['team_id']
        if aid == tid or aid in seen:
            continue
        seen.add(aid)
        bonus = a['free'] if mode == 'max' else 0
        b = teams.get(a['opponent'])

        if b is None:                        # no matchup this week
            if beats(a['floor'] + (a['pool'] if mode == 'max' else 0) + bonus):
                total += 1
            continue

        if b['team_id'] == tid:              # a is my opponent - its share is forced
            if beats(a['floor'] + max(0, a['pool'] - my_share) + bonus):
                total += 1
            continue

        seen.add(b['team_id'])
        pool = a['pool']
        b_bonus = b['free'] if mode == 'max' else 0
        counts = [int(beats(a['floor'] + share + bonus)) +
                  int(beats(b['floor'] + pool - share + b_bonus))
                  for share in range(pool + 1)]
        total += max(counts) if mode == 'max' else min(counts)
    return total


def clinched(teams, tid, final, spots):
    return teams_above(teams, tid, final, 'max') <= spots - 1


def alive(teams, tid, final, spots):
    return teams_above(teams, tid, final, 'min') <= spots - 1


def playoff_status(teams, spots):
    """Clinched / eliminated / bubble, plus 'cats needed this week' numbers."""
    for t in teams.values():
        tid = t['team_id']
        t['clinched'] = clinched(teams, tid, t['floor'], spots)
        t['eliminated'] = not alive(teams, tid, t['ceiling'], spots)

        # smallest number of the still-undecided categories that locks a spot /
        # that keeps a path alive
        t['magic'] = t['need_alive'] = None
        for extra in range(t['remaining'] + 1):
            if t['magic'] is None and clinched(teams, tid, t['floor'] + extra, spots):
                t['magic'] = extra
            if t['need_alive'] is None and alive(teams, tid, t['floor'] + extra, spots):
                t['need_alive'] = extra
            if t['magic'] is not None and t['need_alive'] is not None:
                break
        if t['eliminated']:
            t['magic'] = t['need_alive'] = None
        if t['clinched']:
            t['need_alive'] = 0


def simulate(teams, spots, progress, sims=SIMS, seed=17):
    """
    Monte Carlo the season out.

    A category currently led is treated as locked in proportion to how much of
    the week has been played, and played out on form for the rest: on Monday
    night an 8-3 lead is barely worth more than a coin flip, by Sunday night it
    is worth almost exactly 8-3. Form is each team's season category win rate,
    compared Bradley-Terry style so a .600 team is not a coin flip against a
    .400 team.
    """
    rng = random.Random(seed)
    ids = list(teams)
    made = {tid: 0 for tid in ids}
    seed_counts = {tid: [0] * len(ids) for tid in ids}
    totals = {tid: 0.0 for tid in ids}

    pairs, seen = [], set()
    for t in teams.values():
        o = teams.get(t['opponent'])
        if o is None or t['team_id'] in seen:
            continue
        seen.update({t['team_id'], o['team_id']})
        sa, sb = t['cat_pct'], o['cat_pct']
        form = sa / (sa + sb) if (sa + sb) else 0.5
        held = progress + (1 - progress) * form        # cats this team leads now
        stolen = (1 - progress) * form                 # cats the opponent leads
        tied = t['pool'] - t['live'] - o['live']
        pairs.append((t['team_id'], o['team_id'], t['pool'],
                      t['live'] if t['pool'] else 0,
                      o['live'] if t['pool'] else 0,
                      max(0, tied) if t['pool'] else 0,
                      held, stolen, form))

    def draw(n, p):
        return sum(1 for _ in range(n) if rng.random() < p)

    for _ in range(sims):
        final = {tid: teams[tid]['floor'] for tid in ids}
        for a, b, pool, a_lead, b_lead, tied, held, stolen, form in pairs:
            if not pool:
                continue
            won = draw(a_lead, held) + draw(b_lead, stolen) + draw(tied, form)
            final[a] += won
            final[b] += pool - won
        for tid in ids:
            t = teams[tid]
            if t['free']:
                final[tid] += draw(t['free'], t['cat_pct'])
        # ties at the cut line go to a coin flip here (the real rule is
        # head-to-head categories, which we cannot see without the API)
        order = sorted(ids, key=lambda x: (-final[x], rng.random()))
        for i, tid in enumerate(order):
            seed_counts[tid][i] += 1
            if i < spots:
                made[tid] += 1
        for tid in ids:
            totals[tid] += final[tid]

    for tid in ids:
        teams[tid]['odds'] = 100.0 * made[tid] / sims
        teams[tid]['projected'] = totals[tid] / sims
        teams[tid]['avg_seed'] = sum((i + 1) * c for i, c in enumerate(seed_counts[tid])) / sims
        teams[tid]['seed_counts'] = seed_counts[tid]


# ==============================================================
# Snapshot history (feeds the race chart)
# ==============================================================
SLOT_HOURS = (0, 12, 15, 18, 21)      # midnight, noon, 3pm, 6pm, 9pm - local clock
HISTORY_PATH = os.path.join(DOCS, 'data', f'playoff_odds_history_{YEAR}.json')


def odds_value(t):
    """
    The single number the page shows and the chart plots.

    Only the clinch maths earns a round 100: the simulation coming back
    100,000-for-100,000 is not the same thing as being unable to miss, so
    everyone else is held at 99.9 no matter how the rounding falls.
    """
    if t['eliminated']:
        return 0.0
    if t['clinched']:
        return 100.0
    return min(t['odds'], 99.9)


def current_slot(now=None):
    """The most recent noon / 3 / 6 / 9 / midnight boundary that has passed."""
    now = now or datetime.now()
    hour = max(h for h in SLOT_HOURS if h <= now.hour)   # 0 is always eligible
    return now.replace(hour=hour, minute=0, second=0, microsecond=0)


def slot_label(dt):
    hour = dt.hour
    clock = '12a' if hour == 0 else '12p' if hour == 12 else             (f'{hour - 12}p' if hour > 12 else f'{hour}a')
    return f'{dt.month}/{dt.day} {clock}'


def pick_tracked(teams, spots):
    """
    The teams fighting for the last spots - everyone still alive who has not
    already locked one up. Worked out once and then frozen in the history file,
    so a team that clinches mid-race stays on the chart (pinned at 100) instead
    of disappearing off it, and one that gets eliminated flatlines at 0 in view.
    """
    race = [t for t in teams.values() if not t['clinched'] and not t['eliminated']]
    if not race:                                   # everything already decided
        order = sorted(teams.values(), key=lambda t: -odds_value(t))
        race = order[max(0, spots - 2):spots + 2]
    return [t['team_id'] for t in sorted(race, key=lambda t: -odds_value(t))]


def load_history():
    hist = {'year': YEAR, 'slot_hours': list(SLOT_HOURS), 'tracked': [], 'points': []}
    if os.path.exists(HISTORY_PATH):
        try:
            with io.open(HISTORY_PATH, encoding='utf-8') as f:
                hist.update(json.load(f))
        except (OSError, ValueError):
            pass
    return hist


def record_history(teams, spots, week, write=True):
    """
    Keep one snapshot per slot. The scrape runs every 10 minutes but only the
    first run after a boundary is kept, so the chart is five evenly spaced
    points a day instead of 144 jittery ones.

    Returns (new_point_written, history).
    """
    hist = load_history()
    if not hist['tracked']:
        hist['tracked'] = pick_tracked(teams, spots)

    slot = current_slot()
    key = slot.strftime('%Y-%m-%dT%H')
    if any(p['slot'] == key for p in hist['points']):
        return False, hist

    hist['points'].append({
        'slot': key,
        'label': slot_label(slot),
        'recorded_at': datetime.now().isoformat(timespec='seconds'),
        'week': week,
        'odds': {str(tid): round(odds_value(t), 2) for tid, t in teams.items()},
    })
    hist['points'].sort(key=lambda p: p['slot'])

    if write:
        os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
        with io.open(HISTORY_PATH, 'w', encoding='utf-8') as f:
            json.dump(hist, f, indent=2, ensure_ascii=False)
    return True, hist


# ==============================================================
# Render
# ==============================================================
def fmt_odds(t):
    """
    A clinched team reads a flat 100% - it cannot miss, so the number should not
    hedge. Nobody else gets there: a team the simulation likes at 99.96 is still
    held at 99.9 until the maths locks it in, and a team that is alive but never
    made the playoffs in any run reads <0.1 rather than a flat zero.
    """
    o = odds_value(t)
    if t['eliminated']:
        return '0.0%'
    if t['clinched']:
        return '100%'
    if o < 0.05:
        return '&lt;0.1%'          # escaped: this lands straight in the markup
    return f'{o:.1f}%'


def fmt_pts(v):
    return f'{v:.0f}' if float(v) % 1 == 0 else f'{v:.1f}'


def status_chip(t):
    if t['clinched']:
        return ('clinched', 'CLINCHED')
    if t['eliminated']:
        return ('out', 'ELIMINATED')
    if t['odds'] >= 50:
        return ('in', 'IN THE HUNT')
    if t['odds'] < 1:
        return ('longshot', 'ALIVE ON PAPER')
    return ('bubble', 'ON THE BUBBLE')


RACE_COLORS = ['#38bdf8', '#a78bfa', '#fbbf24', '#34d399', '#f87171', '#f472b6']

# The JS half of the race chart. Kept out of render()'s f-string so the braces
# can stay braces; the two placeholders are filled by json.dumps below.
RACE_JS = """
<script>
(function () {
  const points = __POINTS__;
  new Chart(document.getElementById('raceChart'), {
    type: 'line',
    data: { labels: __LABELS__, datasets: __SETS__ },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      // Room above 100 and below 0 for the dots to sit whole: the datasets
      // turn clipping off, so this padding is what they spill into.
      layout: { padding: { top: 14, bottom: 14, left: 4, right: 10 } },
      scales: {
        y: {
          min: 0, max: 100,
          ticks: { color: '#64748b', stepSize: 20, padding: 8, callback: v => v + '%' },
          grid: { color: '#1e293b' }
        },
        x: {
          ticks: { color: '#64748b', maxRotation: 60, autoSkip: true, maxTicksLimit: 16 },
          grid: { display: false }
        }
      },
      plugins: {
        legend: {
          labels: { color: '#cbd5e1', usePointStyle: true, pointStyle: 'line', boxWidth: 26 }
        },
        tooltip: {
          callbacks: {
            title: items => points[items[0].dataIndex] || '',
            label: c => c.dataset.label + ': ' +
                        (c.parsed.y == null ? 'n/a' : c.parsed.y.toFixed(1) + '%')
          }
        }
      }
    }
  });
})();
</script>
"""


def render_race_chart(teams, hist, spots):
    """
    The last-spots race, snapshotted at midnight / noon / 3 / 6 / 9 every day.

    The tracked teams are frozen in the history file, so the lines do not
    reshuffle underneath the chart - a team that clinches stays on it at a flat
    100, and one that gets eliminated stays on it at a flat 0.
    """
    tracked = [tid for tid in hist.get('tracked', []) if tid in teams]
    points = hist.get('points', [])
    if not tracked or not points:
        return ''

    datasets = []
    for i, tid in enumerate(sorted(tracked, key=lambda x: -odds_value(teams[x]))):
        t = teams[tid]
        name = t['name'] + (f' ({t["manager"]})' if t.get('manager') else '')
        datasets.append({
            'label': name,
            'data': [p['odds'].get(str(tid)) for p in points],
            'borderColor': RACE_COLORS[i % len(RACE_COLORS)],
            'backgroundColor': RACE_COLORS[i % len(RACE_COLORS)],
            'borderWidth': 2.5,
            # monotone rounds the corners without letting the curve overshoot -
            # plain tension would arc a 100-to-100 stretch up past 100, which
            # would be drawing a lie now that clipping is off
            'cubicInterpolationMode': 'monotone',
            'borderJoinStyle': 'round',
            'borderCapStyle': 'round',
            'pointRadius': 3,
            'pointHoverRadius': 6,
            'spanGaps': True,
            'clip': False,          # a dot at 0% or 100% draws whole
        })

    def js(obj):
        # </script> inside a team name would end the block early
        return json.dumps(obj, ensure_ascii=False).replace('</', '<\\/')

    script = (RACE_JS
              .replace('__LABELS__', js([p['label'] for p in points]))
              .replace('__POINTS__', js([f'{p["label"]} · week {p["week"]}' for p in points]))
              .replace('__SETS__', js(datasets)))

    names = ', '.join(html.escape(teams[tid]['name']) for tid in tracked)
    note = (f'Snapshotted at midnight, noon, 3pm, 6pm and 9pm every day · '
            f'{len(points)} snapshot{"" if len(points) == 1 else "s"} so far')
    if len(points) < 2:
        note += ' — the line fills in from here'

    return f'''
  <h2>Race for the Last Playoff Spots</h2>
  <div class="section-desc">{names} — chasing the {spots}-team field. {note}</div>
  <div class="chart-wrap"><canvas id="raceChart"></canvas></div>
{script}
'''


def render(data, teams, spots, week, meta, playoff_raw, matchups,
           progress, hist):
    week_pct = f'{progress * 100:.0f}%'
    race_chart = render_race_chart(teams, hist, spots)
    order = sorted(teams.values(), key=lambda t: (-t['pts'] - t['live'], -t['odds']))
    leader = order[0]['pts'] + order[0]['live']

    # --- playoff picture cards ---
    cards = []
    for i, t in enumerate(order):
        cls, label = status_chip(t)
        seed = i + 1
        cut = ' cutline' if seed == spots else ''
        mgr = f' <span class="mgr">{html.escape(t["manager"])}</span>' if t.get('manager') else ''
        if t['clinched']:
            note = 'Locked in — playing for seeding'
        elif t['eliminated']:
            note = 'Eliminated from the playoff race'
        elif t['magic'] is not None:
            note = (f'Clinches with {t["magic"]} of the {t["remaining"]} '
                    f'categor{"y" if t["remaining"] == 1 else "ies"} left this week')
        elif t['need_alive']:
            note = (f'Needs {t["need_alive"]} of the {t["remaining"]} left this week '
                    f'just to stay alive')
        elif t['need_alive'] == 0:
            note = 'Cannot be caught by the maths yet — but cannot lock it up either'
        else:
            note = 'Needs help elsewhere'
        cards.append(
            f'''<div class="pf-row {cls}{cut}">
  <div class="pf-seed">{seed}</div>
  <div class="pf-main">
    <div class="pf-name">{html.escape(t["name"])}{mgr}</div>
    <div class="pf-note">{html.escape(note)}</div>
  </div>
  <div class="pf-odds">
    <div class="pf-bar"><span style="width:{t["odds"]:.1f}%"></span></div>
    <div class="pf-pct">{fmt_odds(t)}</div>
  </div>
  <div class="pf-chip"><span class="chip {cls}">{label}</span></div>
</div>''')

    # --- standings table ---
    rows = []
    for i, t in enumerate(order):
        cls, label = status_chip(t)
        gb = leader - (t['pts'] + t['live'])
        mgr = f'<span class="mgr">{html.escape(t["manager"])}</span>' if t.get('manager') else ''
        live_txt = f'+{t["live"]}' if t['live'] else '—'
        rows.append(
            f'''<tr class="{cls}">
  <td class="rank">{i + 1}</td>
  <td class="team-name">{html.escape(t["name"])} {mgr}</td>
  <td>{t["wins"]}-{t["losses"]}-{t["ties"]}</td>
  <td class="live">{live_txt}</td>
  <td class="pts">{fmt_pts(t["pts"] + t["live"])}</td>
  <td class="gb">{"—" if gb == 0 else fmt_pts(gb)}</td>
  <td class="proj">{t["projected"]:.1f}</td>
  <td class="ceil">{fmt_pts(t["floor"])} – {fmt_pts(t["ceiling"])}</td>
  <td class="odds">{fmt_odds(t)}</td>
  <td><span class="chip {cls}">{label}</span></td>
</tr>''')

    # --- live matchup cards ---
    mus = []
    for m in matchups:
        a, b = teams[m['a']['team_id']], teams[m['b']['team_id']]
        stakes = []
        for t in (a, b):
            if t['clinched'] or t['eliminated']:
                continue
            if t['magic'] is not None:
                stakes.append(f'{t["name"]} clinches with {t["magic"]} of {t["remaining"]}')
            elif t['need_alive']:
                stakes.append(f'{t["name"]} needs {t["need_alive"]} to stay alive')
            else:
                stakes.append(f'{t["name"]} is fighting for the last spot')
        stake_txt = ' · '.join(stakes) if stakes else 'No bearing on the cut line'
        key = ' key' if stakes else ''
        acls = 'winner' if a['live'] > b['live'] else 'loser' if a['live'] < b['live'] else 'tied'
        bcls = 'winner' if b['live'] > a['live'] else 'loser' if b['live'] < a['live'] else 'tied'
        mus.append(
            f'''<div class="mu{key}">
  <div class="mu-stakes">{html.escape(stake_txt)}</div>
  <div class="mu-head">
    <div class="mu-side"><span class="mu-name">{html.escape(a["name"])}</span>
      <span class="mu-rec">{a["wins"]}-{a["losses"]}-{a["ties"]}</span></div>
    <div class="mu-score"><span class="{acls}">{a["live"]}</span>
      <span class="sep">–</span><span class="{bcls}">{b["live"]}</span></div>
    <div class="mu-side r"><span class="mu-name">{html.escape(b["name"])}</span>
      <span class="mu-rec">{b["wins"]}-{b["losses"]}-{b["ties"]}</span></div>
  </div>
  <div class="mu-foot">Leading {m["decided"]} of {CATS_PER_WEEK} categories between them ·
    {week_pct} of the week played · nothing banked until it ends</div>
</div>''')

    built = datetime.now().strftime('%a %b %-d, %-I:%M %p' if os.name != 'nt'
                                    else '%a %b %d, %I:%M %p')
    cutline_teams = ', '.join(html.escape(t['name']) for t in order[:spots])

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="600">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
<link rel="stylesheet" href="common.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<title>Live Standings - {YEAR} Season</title>
<style>
  .banner {{
    background: #1e293b; border: 1px solid #334155; border-left: 3px solid #38bdf8;
    border-radius: 10px; padding: 12px 16px; margin-bottom: 20px;
    font-size: 0.85em; color: #94a3b8; line-height: 1.5;
  }}
  .banner strong {{ color: #e2e8f0; }}

  .pf-row {{
    display: grid; grid-template-columns: 42px 1fr 160px 130px;
    align-items: center; gap: 12px;
    background: #1e293b; border: 1px solid #334155; border-radius: 10px;
    padding: 10px 14px; margin-bottom: 6px;
  }}
  .pf-row.cutline {{ margin-bottom: 18px; position: relative; }}
  .pf-row.cutline::after {{
    content: 'PLAYOFF CUT LINE'; position: absolute; left: 0; right: 0; bottom: -14px;
    text-align: center; font-size: 0.6em; font-weight: 800; letter-spacing: .14em;
    color: #ef4444;
  }}
  .pf-row.clinched {{ border-color: #22c55e55; }}
  .pf-row.out {{ opacity: .62; }}
  .pf-row.longshot {{ opacity: .82; }}
  .pf-seed {{ font-size: 1.3em; font-weight: 800; color: #64748b; text-align: center; }}
  .pf-name {{ font-weight: 600; }}
  .pf-name .mgr {{ color: #64748b; font-weight: 400; font-size: .85em; margin-left: 6px; }}
  .pf-note {{ font-size: .78em; color: #64748b; margin-top: 2px; }}
  .pf-bar {{ height: 8px; background: #0f172a; border-radius: 4px; overflow: hidden; }}
  .pf-bar span {{ display: block; height: 100%; background: linear-gradient(90deg,#3b82f6,#22c55e); }}
  .pf-pct {{ font-size: .78em; color: #94a3b8; margin-top: 3px; font-variant-numeric: tabular-nums; }}
  .pf-chip {{ text-align: right; }}

  .chip {{
    display: inline-block; font-size: .62em; font-weight: 800; letter-spacing: .06em;
    padding: 3px 8px; border-radius: 4px; text-transform: uppercase; white-space: nowrap;
  }}
  .chip.clinched {{ background: #22c55e22; color: #4ade80; border: 1px solid #22c55e55; }}
  .chip.in       {{ background: #38bdf822; color: #38bdf8; border: 1px solid #38bdf855; }}
  .chip.bubble   {{ background: #f59e0b22; color: #fbbf24; border: 1px solid #f59e0b55; }}
  .chip.longshot {{ background: #94a3b822; color: #94a3b8; border: 1px solid #94a3b855; }}
  .chip.out      {{ background: #ef444422; color: #f87171; border: 1px solid #ef444455; }}

  table.standings td {{ font-variant-numeric: tabular-nums; }}
  table.standings tr.out td {{ opacity: .62; }}
  table.standings tr.longshot td {{ opacity: .82; }}
  .team-name {{ text-align: left; font-weight: 600; }}
  .team-name .mgr {{ color: #64748b; font-weight: 400; font-size: .85em; margin-left: 6px; }}
  td.pts {{ font-weight: 700; color: #34d399; }}
  td.live {{ color: #fbbf24; font-weight: 600; }}
  td.ceil, td.gb {{ color: #94a3b8; font-size: .9em; }}
  td.proj {{ color: #c4b5fd; font-weight: 600; }}
  td.odds {{ font-weight: 700; }}

  .chart-wrap {{
    height: 380px; background: #1e293b; border: 1px solid #334155;
    border-radius: 12px; padding: 18px 18px 14px; margin-bottom: 8px;
  }}
  @media (max-width: 720px) {{ .chart-wrap {{ height: 320px; padding: 12px 10px 10px; }} }}

  .mu-grid {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(340px,1fr)); gap: 12px; }}
  .mu {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 14px 16px; }}
  .mu.key {{ border-color: #3b82f6; }}
  .mu-stakes {{ font-size: .72em; text-transform: uppercase; letter-spacing: .06em;
                color: #64748b; font-weight: 700; margin-bottom: 10px; }}
  .mu.key .mu-stakes {{ color: #60a5fa; }}
  .mu-head {{ display: flex; align-items: center; gap: 10px; }}
  .mu-side {{ display: flex; flex-direction: column; gap: 2px; flex: 1; min-width: 0; }}
  .mu-side.r {{ align-items: flex-end; text-align: right; }}
  .mu-name {{ font-size: .92em; font-weight: 600; overflow-wrap: anywhere; }}
  .mu-rec {{ font-size: .74em; color: #64748b; }}
  .mu-score {{ font-size: 1.5em; font-weight: 800; white-space: nowrap; font-variant-numeric: tabular-nums; }}
  .mu-score .winner {{ color: #22c55e; }}
  .mu-score .loser {{ color: #ef4444; }}
  .mu-score .tied {{ color: #fbbf24; }}
  .mu-score .sep {{ color: #64748b; margin: 0 5px; }}
  .mu-foot {{ margin-top: 10px; font-size: .74em; color: #64748b; }}

  @media (max-width: 720px) {{
    .pf-row {{ grid-template-columns: 32px 1fr 90px; }}
    .pf-chip {{ display: none; }}
  }}
</style>
</head>
<body>
<div class="container">
  <h1>Live Standings</h1>
  <div class="page-subtitle">Week {week} · {html.escape(meta["status"])} ·
    {spots}-team playoff field</div>

  <div class="banner">
    <strong>Where this comes from:</strong> the Yahoo Fantasy API has been dead since late July,
    so this page is scraped from the public league page instead. Standings are Yahoo's through
    week {week - 1} ({html.escape(meta["yahoo_updated"])}); the <strong>Live</strong> column is
    week {week}'s category leads <em>right now</em> — none of that is banked, a category
    can flip any day until the week closes, so every live category is still treated as
    winnable in the maths. Playoff odds run the rest of the season {SIMS:,} times, holding
    each current lead in proportion to the {week_pct} of the week already played and
    playing out the rest on each team's season category win rate.
    <br><strong>Playoff format:</strong> {html.escape(playoff_raw)}.
    Built {built} — this page rescrapes and republishes itself every 10 minutes.
  </div>

  <h2>Playoff Picture</h2>
  <div class="section-desc">In as of right now: {cutline_teams}</div>
  {''.join(cards)}
{race_chart}
  <h2>Standings</h2>
  <div style="overflow-x:auto;">
  <table class="standings">
    <thead><tr>
      <th>#</th><th style="text-align:left">Team</th><th>W-L-T</th>
      <th title="Categories won in week {week} so far">Live</th>
      <th title="Category points: wins + half a point per tie">Pts</th>
      <th>GB</th>
      <th title="Average finish across the simulations">Proj</th>
      <th title="Worst case - best case finish, with every live category still winnable">Range</th>
      <th title="Chance of finishing in the top {spots}">Playoff&nbsp;%</th>
      <th>Status</th>
    </tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  </div>

  <h2>Week {week} Matchups</h2>
  <div class="section-desc">Live category scores — {html.escape(meta["status"])}</div>
  <div class="mu-grid">{''.join(mus)}</div>
</div>
<script src="nav.js"></script>
</body>
</html>
'''


# ==============================================================
# Main
# ==============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='print, do not write')
    ap.add_argument('--skip-unchanged', action='store_true',
                    help='do not rewrite the page when the data is identical')
    args = ap.parse_args()

    lid = league_id()
    base = f'https://baseball.fantasysports.yahoo.com/b1/{lid}'
    print(f'Scraping public league page: {base}')

    home = get(base)
    meta = parse_meta(home)
    standings = parse_standings(home, lid)
    week, matchups = parse_matchups(home, lid)
    if not standings or not matchups:
        sys.exit('ERROR: could not parse standings/matchups — Yahoo markup may have changed')

    settings = parse_playoff_settings(get(f'{base}/settings'))
    spots = settings['spots']
    first_po = settings.get('first_playoff_week')
    last_reg_week = (first_po - 1) if first_po else week
    weeks_left_after = max(0, last_reg_week - week)

    print(f'  {meta["league_name"]} · week {week} ({meta["status"]}) · '
          f'{len(standings)} teams · {len(matchups)} matchups')
    print(f'  Playoffs: {settings["raw"]} → regular season ends week {last_reg_week}, '
          f'{weeks_left_after} week(s) after this one')

    progress = week_progress(meta['status'])
    print(f'  Week {week} is {progress * 100:.0f}% played — live category leads are weighted, not banked')
    teams = build_teams(standings, matchups, weeks_left_after, spots, progress)
    for tid, mgr in load_managers().items():
        if tid in teams:
            teams[tid]['manager'] = mgr

    playoff_status(teams, spots)
    simulate(teams, spots, progress)

    order = sorted(teams.values(), key=lambda t: -(t['pts'] + t['live']))
    print(f'\n{"#":>2} {"team":<28} {"rec":>12} {"live":>4} {"pts":>6} '
          f'{"range":>13} {"odds":>6}  status')
    for i, t in enumerate(order, 1):
        _, label = status_chip(t)
        print(f'{i:>2} {t["name"][:28]:<28} '
              f'{t["wins"]}-{t["losses"]}-{t["ties"]:<6} {t["live"]:>4} '
              f'{fmt_pts(t["pts"] + t["live"]):>6} '
              f'{fmt_pts(t["floor"]) + "-" + fmt_pts(t["ceiling"]):>13} '
              f'{t["odds"]:>5.1f}%  {label}'
              + (f'  (clinch w/ {t["magic"]})' if t.get('magic') else ''))

    payload = {
        'year': YEAR,
        'week': week,
        'status': meta['status'],
        'yahoo_updated': meta['yahoo_updated'],
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'source': base,
        'playoff_spots': spots,
        'playoff_format': settings['raw'],
        'last_regular_week': last_reg_week,
        'sims': SIMS,
        'week_progress': round(progress, 4),
        'teams': [{k: v for k, v in t.items() if k != 'seed_counts'}
                  for t in order],
        'matchups': matchups,
    }

    new_point, hist = record_history(teams, spots, week, write=not args.dry_run)
    tracked = ', '.join(teams[t]['name'] for t in hist['tracked'] if t in teams)
    print(f'\nRace chart tracks: {tracked}')
    print(f"  {len(hist['points'])} snapshot(s) on file"
          + (f" — logged {hist['points'][-1]['label']}"
             if new_point else " — none due yet"))

    if args.dry_run:
        print('\n(dry run — nothing written)')
        return

    os.makedirs(os.path.join(DOCS, 'data'), exist_ok=True)
    json_path = os.path.join(DOCS, 'data', f'live_standings_{YEAR}.json')

    # Nothing but the clock moved? Leave the files alone so the scheduled run
    # does not churn out a commit every 10 minutes.
    if args.skip_unchanged and not new_point and os.path.exists(json_path):
        try:
            with open(json_path, encoding='utf-8') as f:
                prev = json.load(f)
            drop = ('scraped_at',)
            if ({k: v for k, v in prev.items() if k not in drop} ==
                    {k: v for k, v in payload.items() if k not in drop}):
                print('No change since last scrape - nothing written')
                return
        except (OSError, ValueError):
            pass
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    html_path = os.path.join(DOCS, f'live_standings_{YEAR}.html')
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(render(payload, teams, spots, week, meta, settings['raw'],
                       matchups, progress, hist))

    print(f'\nWrote {json_path}')
    print(f'Wrote {html_path}')


if __name__ == '__main__':
    main()
