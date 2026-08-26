"""
The HTML half of the live standings page.

Split out of scripts/scrape_live_standings.py so the scheduled AWS function can
render byte-for-byte the same page the local generator does - the Lambda is the
publisher now, and a second copy of this markup would drift within a week.

Pairs with playoff_core, which does the scraping and the maths.
"""

import os
import json
import html
from datetime import datetime

from playoff_core import CATS_PER_WEEK, SIMS, YEAR, odds_value, now_local

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

def stake_line(t):
    """
    What this week's matchup is worth to one team.

    The requirement and the live score have to be quoted on the same basis or
    the card lies by juxtaposition: 'needs 6' printed beside a 9-2 lead reads
    as 'needs 6 more', when in fact 6 is the target and the 9 already clears
    it. Nothing is banked mid-week, so a team past its number is 'already
    there' rather than safe - the footer carries that caveat.
    """
    if t['clinched'] or t['eliminated']:
        return ''

    if t['magic'] is not None:
        need, verb = t['magic'], 'clinches with'
    elif t['need_alive']:
        need, verb = t['need_alive'], 'needs'
    else:
        return f'{t["name"]} is fighting for the last spot'

    if need >= t['remaining']:
        target = f'needs all {t["remaining"]} this week ' \
                 f'{"to clinch" if verb == "clinches with" else "to stay alive"}'
    else:
        target = f'{verb} {need} of this week\'s {t["remaining"]}'
    pace = 'already there' if t['live'] >= need else f'leads {t["live"]}'
    return f'{t["name"]} {target} — {pace}'


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
                    f'categor{"y" if t["remaining"] == 1 else "ies"} left this week — '
                    + ('leading enough right now' if t['live'] >= t['magic']
                       else f'leading {t["live"]}'))
        elif t['need_alive']:
            note = (f'Needs {t["need_alive"]} of the {t["remaining"]} left this week '
                    f'just to stay alive — '
                    + ('leading enough right now' if t['live'] >= t['need_alive']
                       else f'leading {t["live"]}'))
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
        stakes = [s for s in (stake_line(a), stake_line(b)) if s]
        stake_txt = ' · '.join(stakes) if stakes else 'No bearing on the cut line'
        # decided counts the categories that currently have a leader, so the
        # only thing it tells the reader is how many are still level
        tied = CATS_PER_WEEK - m['decided']
        tied_txt = (f'{tied} categor{"y" if tied == 1 else "ies"} still level'
                    if tied else f'All {CATS_PER_WEEK} categories have a leader')
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
  <div class="mu-foot">{tied_txt} · {week_pct} of the week played ·
    nothing banks until the week ends</div>
</div>''')

    built = now_local().strftime('%a %b %-d, %-I:%M %p' if os.name != 'nt'
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
    Built {built} — a scheduled AWS job rescrapes and republishes this page, and pins
    a new point on the race chart at midnight, noon, 3pm, 6pm and 9pm.
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
