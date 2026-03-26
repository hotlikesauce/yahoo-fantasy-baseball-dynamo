#!/usr/bin/env python3
"""
Generate live standings page for 2026 season.
Pulls data directly from Yahoo Fantasy API.
"""

import os, sys, io, json, requests
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_IDS_STR = os.getenv('YAHOO_LEAGUE_IDS')

league_ids = {}
for pair in YAHOO_LEAGUE_IDS_STR.split(','):
    year, lid = pair.split(':')
    league_ids[int(year)] = lid

LEAGUE_KEY = f"469.l.{league_ids[2026]}"
BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

# Stat ID -> display name mapping (from league settings)
STAT_MAP = {
    '7': 'R', '8': 'H', '12': 'HR', '13': 'RBI', '16': 'SB', '55': 'OPS',
    '49': 'TB', '26': 'ERA', '27': 'WHIP', '57': 'K/9', '83': 'QS', '89': 'SV+H'
}
# Which stats are "lower is better"
LOW_STATS = {'TB', 'ERA', 'WHIP'}
# Display order
STAT_ORDER = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K/9', 'QS', 'SV+H', 'ERA', 'WHIP', 'TB']
# Non-scoring stats to skip
SKIP_STATS = {'60', '50'}  # H/AB, IP

# ==============================
# Yahoo API helpers
# ==============================
def get_token():
    resp = requests.post("https://api.login.yahoo.com/oauth2/get_token", data={
        'client_id': YAHOO_CONSUMER_KEY,
        'client_secret': YAHOO_CONSUMER_SECRET,
        'refresh_token': YAHOO_REFRESH_TOKEN,
        'grant_type': 'refresh_token'
    })
    return resp.json()['access_token']

def api_get(token, endpoint):
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    r = requests.get(f"{BASE_URL}/{endpoint}?format=json", headers=headers)
    if r.status_code == 200:
        return r.json()
    print(f"  API error {r.status_code}: {r.text[:200]}")
    return None

# ==============================
# Pull data
# ==============================
token = get_token()
print(f"Got token, league key: {LEAGUE_KEY}")

# 1. Standings (W-L-T)
print("\nPulling standings...")
standings_resp = api_get(token, f"league/{LEAGUE_KEY}/standings")
league_arr = standings_resp['fantasy_content']['league']
league_meta = league_arr[0]
current_week = int(league_meta.get('current_week', 1))
print(f"  Current week: {current_week}")

standings_data = league_arr[1]['standings'][0]['teams']
standings = []
for tidx in standings_data:
    if tidx == 'count':
        continue
    team_list = standings_data[tidx]['team']
    basic = team_list[0]
    team_id = team_name = manager = None
    for item in basic:
        if not isinstance(item, dict):
            continue
        if 'team_id' in item:
            team_id = item['team_id']
        elif 'name' in item:
            team_name = item['name']
        elif 'managers' in item:
            mgrs = item['managers']
            if isinstance(mgrs, list):
                manager = mgrs[0].get('manager', {}).get('nickname', '?')

    ts = team_list[2].get('team_standings', {})
    ot = ts.get('outcome_totals', {})
    w = int(ot.get('wins', 0))
    l = int(ot.get('losses', 0))
    t = int(ot.get('ties', 0))
    rank = int(ts.get('rank') or 99)
    gp = w + l + t
    pts = w + 0.5 * t
    pct = pts / gp if gp > 0 else 0

    standings.append({
        'rank': rank,
        'name': team_name,
        'manager': manager,
        'wins': w, 'losses': l, 'ties': t,
        'pts': pts, 'pct': pct, 'gp': gp,
    })

standings.sort(key=lambda r: r['rank'])

# Calculate GB
leader_pts = standings[0]['pts'] if standings else 0
for row in standings:
    row['gb'] = leader_pts - row['pts']

for row in standings:
    print(f"  #{row['rank']:2d} {row['name']:35s} {row['wins']}-{row['losses']}-{row['ties']}  Pts={row['pts']}  GB={row['gb']}")

# 2. Scoreboard (current matchups with category stats)
print(f"\nPulling scoreboard (week {current_week})...")
sb_resp = api_get(token, f"league/{LEAGUE_KEY}/scoreboard")
scoreboard = sb_resp['fantasy_content']['league'][1]['scoreboard']
sb_week = scoreboard.get('week', current_week)
matchups_raw = scoreboard['0']['matchups']

def parse_team_stats(team_list):
    """Extract name, total score, and individual stat values."""
    name = '?'
    for item in team_list[0]:
        if isinstance(item, dict) and 'name' in item:
            name = item['name']

    pw = team_list[1]
    total = pw.get('team_points', {}).get('total', '0')
    raw_stats = pw.get('team_stats', {}).get('stats', [])

    stats = {}
    for s in raw_stats:
        stat = s['stat']
        sid = str(stat['stat_id'])
        if sid in SKIP_STATS:
            continue
        cat = STAT_MAP.get(sid)
        if not cat:
            continue
        val = stat.get('value')
        if val is None or val == 'None' or val == '':
            val = None
        else:
            try:
                val = float(val)
            except (ValueError, TypeError):
                val = None
        stats[cat] = val

    return {'name': name, 'total': total, 'stats': stats}

matchups = []
for midx in matchups_raw:
    if midx == 'count':
        continue
    matchup = matchups_raw[midx]['matchup']
    status = matchup.get('status', '?')
    teams = matchup['0']['teams']

    a = parse_team_stats(teams['0']['team'])
    b = parse_team_stats(teams['1']['team'])

    # Calculate per-category winners
    a_wins = b_wins = ties = 0
    cat_results = []
    for cat in STAT_ORDER:
        av = a['stats'].get(cat)
        bv = b['stats'].get(cat)
        if av is None and bv is None:
            cat_results.append({'cat': cat, 'a': av, 'b': bv, 'winner': 'none'})
            continue
        if av is None:
            av = float('inf') if cat not in LOW_STATS else float('inf')
        if bv is None:
            bv = float('inf') if cat not in LOW_STATS else float('inf')

        if cat in LOW_STATS:
            if av < bv: winner = 'a'; a_wins += 1
            elif bv < av: winner = 'b'; b_wins += 1
            else: winner = 'tie'; ties += 1
        else:
            if av > bv: winner = 'a'; a_wins += 1
            elif bv > av: winner = 'b'; b_wins += 1
            else: winner = 'tie'; ties += 1

        cat_results.append({'cat': cat, 'a': a['stats'].get(cat), 'b': b['stats'].get(cat), 'winner': winner})

    margin = abs(a_wins - b_wins)
    matchups.append({
        'teamA': a['name'], 'teamB': b['name'],
        'scoreA': int(a['total']), 'scoreB': int(b['total']),
        'cats': cat_results,
        'margin': margin,
        'status': status,
    })
    print(f"  {a['name']} {a['total']}-{b['total']} {b['name']} ({status})")

# Sort by biggest blowout first
matchups.sort(key=lambda m: m['margin'], reverse=True)

# ==============================
# Generate HTML
# ==============================
from datetime import datetime
build_time = datetime.now().strftime('%Y-%m-%d %I:%M %p')

standings_json = json.dumps(standings, ensure_ascii=False)
matchups_json = json.dumps(matchups, ensure_ascii=False)

html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Live Standings - 2026 Season</title>
<link rel="stylesheet" href="common.css">
<style>
  .matchup-card {{
    background: #1e293b;
    border-radius: 12px;
    padding: 20px;
    margin-bottom: 16px;
    border: 1px solid #334155;
  }}
  .matchup-header {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 14px;
    gap: 12px;
  }}
  .matchup-team {{
    font-weight: 700;
    font-size: 1.05em;
    flex: 1;
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}
  .matchup-team.left {{ text-align: left; }}
  .matchup-team.right {{ text-align: right; }}
  .matchup-score {{
    font-size: 1.6em;
    font-weight: 800;
    text-align: center;
    min-width: 120px;
    white-space: nowrap;
  }}
  .matchup-score .winner {{ color: #22c55e; }}
  .matchup-score .loser {{ color: #ef4444; }}
  .matchup-score .tied {{ color: #fbbf24; }}
  .matchup-score .sep {{ color: #64748b; margin: 0 6px; }}
  .cat-grid {{
    display: grid;
    grid-template-columns: repeat(12, 1fr);
    gap: 4px;
    font-size: 0.78em;
  }}
  .cat-cell {{
    text-align: center;
    padding: 6px 2px;
    border-radius: 6px;
    background: #0f172a;
  }}
  .cat-cell .cat-name {{
    color: #64748b;
    font-size: 0.82em;
    text-transform: uppercase;
    margin-bottom: 3px;
  }}
  .cat-cell .cat-val {{
    font-weight: 600;
    font-size: 0.95em;
  }}
  .cat-cell .cat-val.bottom {{
    opacity: 0.5;
    font-size: 0.85em;
  }}
  .cat-cell.win-a .cat-val:first-of-type {{ color: #22c55e; }}
  .cat-cell.win-a .cat-val.bottom {{ color: #ef4444; }}
  .cat-cell.win-b .cat-val:first-of-type {{ color: #ef4444; }}
  .cat-cell.win-b .cat-val.bottom {{ color: #22c55e; }}
  .cat-cell.tie .cat-val {{ color: #fbbf24; }}
  .cat-cell.none .cat-val {{ color: #475569; }}
  .blowout-tag {{
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.55em;
    font-weight: 700;
    vertical-align: middle;
    margin-left: 6px;
    letter-spacing: 0.03em;
  }}
  .blowout-tag.domination {{ background: #22c55e22; color: #22c55e; }}
  .blowout-tag.close {{ background: #fbbf2422; color: #fbbf24; }}
  .blowout-tag.sweep {{ background: #a855f722; color: #a855f7; }}
  .blowout-tag.shutout {{ background: #ef444422; color: #ef4444; }}
  .standings-table td.team-name {{ font-weight: 600; color: #e2e8f0; }}
  .standings-table .manager {{ color: #64748b; font-size: 0.85em; font-weight: 400; }}
  .standings-table td.gb {{ color: #64748b; }}
  .last-updated {{
    text-align: center;
    color: #475569;
    font-size: 0.82em;
    margin-top: 16px;
  }}
  .record {{ font-variant-numeric: tabular-nums; }}
  .status-badge {{
    display: inline-block;
    padding: 1px 8px;
    border-radius: 10px;
    font-size: 0.6em;
    font-weight: 600;
    vertical-align: middle;
    margin-left: 6px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }}
  .status-badge.live {{ background: #22c55e22; color: #22c55e; }}
  .status-badge.final {{ background: #3b82f622; color: #3b82f6; }}
  .status-badge.pre {{ background: #64748b22; color: #64748b; }}
  @media (max-width: 900px) {{
    .cat-grid {{ grid-template-columns: repeat(6, 1fr); }}
    .matchup-header {{ flex-wrap: wrap; }}
    .matchup-team {{ min-width: 100%; text-align: center !important; }}
    .matchup-score {{ min-width: 100%; }}
  }}
</style>
</head>
<body>
<div id="nav"></div>
<div class="container">
  <h1>Live Standings</h1>
  <p class="page-subtitle">2026 Season &mdash; Week {sb_week}</p>

  <h2>League Standings</h2>
  <p class="section-desc">Win = 1 pt, Tie = 0.5 pts. Sorted by Yahoo ranking.</p>
  <div style="overflow-x:auto;">
    <table class="standings-table" id="standings-table">
      <thead>
        <tr>
          <th class="sortable" data-col="rank">#</th>
          <th class="sortable" data-col="name">Team</th>
          <th class="sortable" data-col="record">Record</th>
          <th class="sortable" data-col="pts">Pts</th>
          <th class="sortable" data-col="pct">Win%</th>
          <th class="sortable" data-col="gb">GB</th>
        </tr>
      </thead>
      <tbody id="standings-body"></tbody>
    </table>
  </div>

  <h2>Current Matchups &mdash; Week {sb_week}</h2>
  <p class="section-desc">Live category-by-category breakdown. Top value = left team, bottom = right team. Sorted by blowout margin.</p>
  <div id="matchups-container"></div>

  <p class="last-updated">Last generated: {build_time}. Re-run <code>gen_live_standings.py</code> to refresh.</p>
</div>

<script src="nav.js"></script>
<script>
const standings = {standings_json};
const matchups = {matchups_json};

// Render standings
const tbody = document.getElementById('standings-body');
standings.forEach((row, i) => {{
  const tr = document.createElement('tr');
  const gb = row.gb === 0 ? '\\u2014' : row.gb.toFixed(1);
  const record = row.wins + '-' + row.losses + (row.ties > 0 ? '-' + row.ties : '');
  const pct = row.gp > 0 ? (row.pct * 100).toFixed(1) + '%' : '\\u2014';
  tr.innerHTML =
    '<td class="rank">' + row.rank + '</td>' +
    '<td class="team-name">' + row.name + ' <span class="manager">' + row.manager + '</span></td>' +
    '<td class="record">' + record + '</td>' +
    '<td style="font-weight:700;color:#34d399">' + row.pts.toFixed(1) + '</td>' +
    '<td>' + pct + '</td>' +
    '<td class="gb">' + gb + '</td>';
  tbody.appendChild(tr);
}});

// Render matchups
const container = document.getElementById('matchups-container');

matchups.forEach(m => {{
  const card = document.createElement('div');
  card.className = 'matchup-card';

  const margin = Math.abs(m.scoreA - m.scoreB);
  let tag = '';
  if (m.scoreA === 12 || m.scoreB === 12) {{
    tag = '<span class="blowout-tag sweep">PERFECT</span>';
  }} else if (m.scoreA === 0 || m.scoreB === 0) {{
    tag = '<span class="blowout-tag shutout">SHUTOUT</span>';
  }} else if (margin >= 8) {{
    tag = '<span class="blowout-tag domination">DOMINATION</span>';
  }} else if (margin <= 2) {{
    tag = '<span class="blowout-tag close">CLOSE</span>';
  }}

  let statusBadge = '';
  if (m.status === 'midevent') statusBadge = '<span class="status-badge live">LIVE</span>';
  else if (m.status === 'postevent') statusBadge = '<span class="status-badge final">FINAL</span>';
  else statusBadge = '<span class="status-badge pre">PRE</span>';

  const aClass = m.scoreA > m.scoreB ? 'winner' : m.scoreA < m.scoreB ? 'loser' : 'tied';
  const bClass = m.scoreB > m.scoreA ? 'winner' : m.scoreB < m.scoreA ? 'loser' : 'tied';

  let catCells = '';
  m.cats.forEach(c => {{
    const cls = c.winner === 'a' ? 'win-a' : c.winner === 'b' ? 'win-b' : c.winner === 'tie' ? 'tie' : 'none';
    const fmt = (v, cat) => {{
      if (v === null || v === undefined) return '\\u2014';
      if (['OPS','ERA','WHIP'].includes(cat)) return v.toFixed(3);
      if (['K/9'].includes(cat)) return v.toFixed(2);
      return Math.round(v).toString();
    }};
    catCells +=
      '<div class="cat-cell ' + cls + '">' +
        '<div class="cat-name">' + c.cat + '</div>' +
        '<div class="cat-val">' + fmt(c.a, c.cat) + '</div>' +
        '<div class="cat-val bottom">' + fmt(c.b, c.cat) + '</div>' +
      '</div>';
  }});

  card.innerHTML =
    '<div class="matchup-header">' +
      '<div class="matchup-team left">' + m.teamA + '</div>' +
      '<div class="matchup-score">' +
        '<span class="' + aClass + '">' + m.scoreA + '</span>' +
        '<span class="sep">-</span>' +
        '<span class="' + bClass + '">' + m.scoreB + '</span>' +
        tag + statusBadge +
      '</div>' +
      '<div class="matchup-team right">' + m.teamB + '</div>' +
    '</div>' +
    '<div class="cat-grid">' + catCells + '</div>';
  container.appendChild(card);
}});

// Sortable table
document.querySelectorAll('.sortable').forEach(th => {{
  th.addEventListener('click', () => {{
    const col = th.dataset.col;
    const asc = th.classList.contains('asc');
    document.querySelectorAll('.sortable').forEach(h => h.classList.remove('asc','desc'));
    th.classList.add(asc ? 'desc' : 'asc');
    const dir = asc ? -1 : 1;
    const rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort((a, b) => {{
      const ca = a.querySelectorAll('td');
      const cb = b.querySelectorAll('td');
      let av, bv;
      switch(col) {{
        case 'rank': av = parseInt(ca[0].textContent); bv = parseInt(cb[0].textContent); break;
        case 'name': av = ca[1].textContent.toLowerCase(); bv = cb[1].textContent.toLowerCase(); return av < bv ? -dir : av > bv ? dir : 0;
        case 'record': {{
          const p = s => {{ const x = s.split('-').map(Number); return x[0] + 0.5*(x[2]||0); }};
          av = p(ca[2].textContent); bv = p(cb[2].textContent); break;
        }}
        case 'pts': av = parseFloat(ca[3].textContent); bv = parseFloat(cb[3].textContent); break;
        case 'pct': av = parseFloat(ca[4].textContent)||0; bv = parseFloat(cb[4].textContent)||0; break;
        case 'gb': av = ca[5].textContent === '\\u2014' ? 0 : parseFloat(ca[5].textContent); bv = cb[5].textContent === '\\u2014' ? 0 : parseFloat(cb[5].textContent); break;
      }}
      return (av - bv) * dir;
    }});
    rows.forEach(r => tbody.appendChild(r));
  }});
}});
</script>
</body>
</html>
'''

output_path = 'docs/live_standings_2026.html'
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"\nWrote {output_path}")
