import boto3, json, sys, io, statistics
from boto3.dynamodb.conditions import Key
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

# Categories
HIGH_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB', 'K9', 'QS', 'SVH']
LOW_CATS = ['ERA', 'WHIP']
ALL_CATS = HIGH_CATS + LOW_CATS
BATTER_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB']
PITCHER_HIGH = ['K9', 'QS', 'SVH']
PITCHER_LOW = ['ERA', 'WHIP']

def h2h_result(a, b):
    wa = wb = 0
    for c in HIGH_CATS:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    for c in LOW_CATS:
        if c in a and c in b:
            if a[c] < b[c]: wa += 1
            elif b[c] < a[c]: wb += 1
    return wa, wb

def h2h_batter(a, b):
    wa = wb = 0
    for c in BATTER_CATS:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    return wa, wb

def h2h_pitcher(a, b):
    wa = wb = 0
    for c in PITCHER_HIGH:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    for c in PITCHER_LOW:
        if c in a and c in b:
            if a[c] < b[c]: wa += 1
            elif b[c] < a[c]: wb += 1
    return wa, wb

# ============================================================
# 1. Build stable name mapping from power_ranks_season_trend
# ============================================================
name_to_tn = {}
tn_latest_name = {}
power_data = {}

for week in range(1, 30):
    resp = table.query(IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'power_ranks_season_trend#{week}'))
    if resp['Count'] == 0:
        break
    for item in resp['Items']:
        tn = item['TeamNumber']
        name_to_tn[item['Team']] = tn
        tn_latest_name[tn] = item['Team']
        if tn not in power_data:
            power_data[tn] = {}
        w = int(item['Week'])
        power_data[tn][w] = {
            'score': float(item['Score_Sum']),
            'rank': float(item['Stats_Power_Rank']),
        }

print("Team mapping:")
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    print(f"  {tn}: {tn_latest_name[tn]}")

all_tns_set = set(str(i) for i in range(1, 13))

def auto_map_week(items_list, week_stats=None, week_results=None):
    mapped = {}
    unmapped = []
    for item in items_list:
        name = item.get('Team', '')
        if not name: continue
        if name_to_tn.get(name):
            mapped[name] = name_to_tn[name]
        else:
            unmapped.append(name)
    if not unmapped: return
    used = set(mapped.values())
    missing = sorted(all_tns_set - used, key=int)
    if len(unmapped) != len(missing): return
    if len(unmapped) == 1:
        name_to_tn[unmapped[0]] = missing[0]
        print(f"  Auto-mapped: '{unmapped[0]}' -> TN {missing[0]}")
    elif len(unmapped) >= 2 and week_stats and week_results:
        _resolve_multi(unmapped, missing, week_stats, week_results)

def _resolve_multi(unmapped, missing_tns, ws, wr):
    stats_by_name = {}
    for item in ws:
        s = {c: float(item[c]) for c in ALL_CATS if c in item}
        stats_by_name[item['Team']] = s
    tn_matchups = {}
    for item in wr:
        tn = name_to_tn.get(item['Team'])
        if tn and tn in missing_tns:
            opp_tn = name_to_tn.get(item['Opponent'])
            if opp_tn:
                tn_matchups[tn] = {'opp_tn': opp_tn, 'score': float(item['Score'])}
    if not tn_matchups: return
    tn_to_sn = {tn: n for n, tn in name_to_tn.items() if n in stats_by_name}
    best = {}
    for mt, m in tn_matchups.items():
        osn = tn_to_sn.get(m['opp_tn'])
        if not osn or osn not in stats_by_name: continue
        for un in unmapped:
            if un not in stats_by_name: continue
            wa, _ = h2h_result(stats_by_name[un], stats_by_name[osn])
            d = abs(wa - m['score'])
            if mt not in best or d < best[mt][1]:
                best[mt] = (un, d)
    used_n, used_t = set(), set()
    for mt in missing_tns:
        if mt in best and best[mt][0] not in used_n:
            name_to_tn[best[mt][0]] = mt
            used_n.add(best[mt][0]); used_t.add(mt)
            print(f"  Auto-mapped (H2H): '{best[mt][0]}' -> TN {mt}")
    for n, t in zip([x for x in unmapped if x not in used_n], [x for x in missing_tns if x not in used_t]):
        name_to_tn[n] = t
        print(f"  Auto-mapped (elim): '{n}' -> TN {t}")

# ============================================================
# 2. Pull weekly_stats + auto-mapping
# ============================================================
for week in range(1, 30):
    resp = table.query(IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
    if resp['Count'] == 0: break
    if resp['Count'] == 12: auto_map_week(resp['Items'])

for week in range(1, 30):
    resp = table.query(IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_results#{week}'))
    if resp['Count'] == 0: break
    auto_map_week(resp['Items'])

for week in range(1, 30):
    ws = table.query(IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
    if ws['Count'] == 0: break
    if ws['Count'] < 12: continue
    if any(name_to_tn.get(i['Team']) is None for i in ws['Items']):
        wr = table.query(IndexName='DataTypeWeekIndex',
            KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_results#{week}'))
        auto_map_week(ws['Items'], ws['Items'], wr['Items'])

weekly_stats = defaultdict(dict)
for week in range(1, 30):
    resp = table.query(IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
    if resp['Count'] == 0: break
    if resp['Count'] < 12: continue
    for item in resp['Items']:
        tn = name_to_tn.get(item['Team'])
        if not tn: continue
        weekly_stats[week][tn] = {c: float(item[c]) for c in ALL_CATS if c in item}

stat_weeks = sorted(weekly_stats.keys())
power_weeks = sorted(set(w for d in power_data.values() for w in d))
print(f"\nLoaded {len(stat_weeks)} stat weeks, {len(power_weeks)} power weeks")

# ============================================================
# 3. H2H Simulation with batter/pitcher splits
# ============================================================
weekly_xwins = defaultdict(dict)
season_bat = defaultdict(lambda: {'w': 0, 'total': 0})
season_pit = defaultdict(lambda: {'w': 0, 'total': 0})

for week in stat_weeks:
    tns = list(weekly_stats[week].keys())
    for i, ta in enumerate(tns):
        cat_w = 0
        for j, tb in enumerate(tns):
            if i == j: continue
            wa, wb = h2h_result(weekly_stats[week][ta], weekly_stats[week][tb])
            cat_w += wa
            ba, _ = h2h_batter(weekly_stats[week][ta], weekly_stats[week][tb])
            season_bat[ta]['w'] += ba
            season_bat[ta]['total'] += len(BATTER_CATS)
            pa, _ = h2h_pitcher(weekly_stats[week][ta], weekly_stats[week][tb])
            season_pit[ta]['w'] += pa
            season_pit[ta]['total'] += len(PITCHER_HIGH) + len(PITCHER_LOW)
        n_opps = len(tns) - 1
        weekly_xwins[week][ta] = cat_w / n_opps if n_opps > 0 else 0

print(f"H2H simulation complete")

# ============================================================
# 4. Build chart data
# ============================================================
colors = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]

# Sort by final power score
sorted_teams = sorted(power_data.items(),
    key=lambda x: x[1].get(max(power_weeks), {}).get('score', 0), reverse=True)
color_map = {tn: colors[i % len(colors)] for i, (tn, _) in enumerate(sorted_teams)}

def js_label(tn):
    return tn_latest_name[tn].replace('\\', '\\\\').replace("'", "\\'")

# Power Score/Rank datasets
def make_power_ds(field):
    datasets = []
    for i, (tn, data) in enumerate(sorted_teams):
        points = [round(data[w][field], 1) if w in data else None for w in power_weeks]
        datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: {json.dumps(points)},
      borderColor: '{colors[i % len(colors)]}', backgroundColor: '{colors[i % len(colors)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
    return ',\n      '.join(datasets)

score_ds = make_power_ds('score')
rank_ds = make_power_ds('rank')
power_labels = json.dumps([f'Week {w}' for w in power_weeks])

# xWins by week datasets
xwin_datasets = []
for i, (tn, _) in enumerate(sorted_teams):
    points = [round(weekly_xwins[w].get(tn, 0), 2) for w in stat_weeks]
    xwin_datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: {json.dumps(points)},
      borderColor: '{colors[i % len(colors)]}', backgroundColor: '{colors[i % len(colors)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
xwin_ds = ',\n      '.join(xwin_datasets)
stat_labels = json.dumps([f'Wk {w}' for w in stat_weeks])

# Cumulative xWins datasets
cum_datasets = []
for i, (tn, _) in enumerate(sorted_teams):
    cum = 0
    points = []
    for w in stat_weeks:
        cum += weekly_xwins[w].get(tn, 0)
        points.append(round(cum, 1))
    cum_datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: {json.dumps(points)},
      borderColor: '{colors[i % len(colors)]}', backgroundColor: '{colors[i % len(colors)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
cum_ds = ',\n      '.join(cum_datasets)

# Scatter data (batter vs pitcher win%)
scatter_datasets = []
for i, (tn, _) in enumerate(sorted_teams):
    bat = season_bat[tn]
    pit = season_pit[tn]
    bx = round(bat['w'] / bat['total'] * 100, 1) if bat['total'] > 0 else 50
    py_ = round(pit['w'] / pit['total'] * 100, 1) if pit['total'] > 0 else 50
    scatter_datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: [{{x: {bx}, y: {py_}}}],
      backgroundColor: '{colors[i % len(colors)]}', borderColor: '{colors[i % len(colors)]}',
      pointRadius: 8, pointHoverRadius: 11
    }}""")
scatter_ds = ',\n      '.join(scatter_datasets)

# ============================================================
# 5. Who's Hot / Who's Not
# ============================================================
last_2 = stat_weeks[-2:] if len(stat_weeks) >= 2 else stat_weeks
last_2_label = f"Weeks {last_2[0]}-{last_2[-1]}" if len(last_2) > 1 else f"Week {last_2[0]}"

hot_data = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    recent = [weekly_xwins[w].get(tn, 0) for w in last_2]
    recent_avg = sum(recent) / len(recent) if recent else 0
    season_avg = sum(weekly_xwins[w].get(tn, 0) for w in stat_weeks) / len(stat_weeks) if stat_weeks else 0
    hot_data.append({
        'tn': tn, 'name': tn_latest_name[tn],
        'recent': round(recent_avg, 2), 'season': round(season_avg, 2),
        'diff': round(recent_avg - season_avg, 2),
    })
hot_data.sort(key=lambda x: x['recent'], reverse=True)

hot_rows = ""
for i, d in enumerate(hot_data):
    color = color_map.get(d['tn'], '#94a3b8')
    if d['diff'] > 0.5:
        trend = '<span class="hot-tag">HOT</span>'
    elif d['diff'] < -0.5:
        trend = '<span class="cold-tag">COLD</span>'
    else:
        trend = '<span class="steady-tag">STEADY</span>'
    diff_class = "hot-val" if d['diff'] > 0 else "cold-val" if d['diff'] < 0 else ""
    diff_val = f"+{d['diff']}" if d['diff'] > 0 else str(d['diff'])
    hot_rows += f'<tr><td class="rank">{i+1}</td><td><span style="color:{color}">&#9679;</span> {d["name"]}</td><td class="score">{d["recent"]:.2f}</td><td>{d["season"]:.2f}</td><td class="{diff_class}">{diff_val}</td><td>{trend}</td></tr>'

# ============================================================
# 6. Season's Best
# ============================================================
season_best = {}
for cat in ALL_CATS:
    is_low = cat in LOW_CATS
    best = None
    for week in stat_weeks:
        for tn, stats in weekly_stats[week].items():
            if cat not in stats: continue
            val = stats[cat]
            if best is None or (is_low and val < best['val']) or (not is_low and val > best['val']):
                best = {'val': val, 'tn': tn, 'week': week}
    if best:
        if cat in ['ERA', 'WHIP', 'OPS']:
            fmt = f"{best['val']:.3f}"
        elif cat == 'K9':
            fmt = f"{best['val']:.1f}"
        else:
            fmt = f"{int(best['val'])}" if best['val'] == int(best['val']) else f"{best['val']:.1f}"
        season_best[cat] = {**best, 'formatted': fmt, 'team': tn_latest_name.get(best['tn'], best['tn'])}

cat_labels = {
    'R': 'Runs', 'H': 'Hits', 'HR': 'Home Runs', 'RBI': 'RBI', 'SB': 'Stolen Bases',
    'OPS': 'OPS', 'TB': 'Total Bases', 'K9': 'K/9', 'QS': 'Quality Starts',
    'SVH': 'Saves+Holds', 'ERA': 'ERA (Lowest)', 'WHIP': 'WHIP (Lowest)',
}
best_rows = ""
for cat in ALL_CATS:
    if cat not in season_best: continue
    b = season_best[cat]
    color = color_map.get(b['tn'], '#94a3b8')
    icon = '&#x26BE;' if cat in BATTER_CATS else '&#x1F3AF;'
    best_rows += f'<tr><td>{icon}</td><td class="year">{cat_labels.get(cat, cat)}</td><td class="score">{b["formatted"]}</td><td><span style="color:{color}">&#9679;</span> {b["team"]}</td><td class="year">Week {b["week"]}</td></tr>'

# ============================================================
# 7. Generate HTML
# ============================================================
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Summertime Sadness - 2025 Season Trends</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', system-ui, sans-serif; padding: 24px; }}
  .container {{ max-width: 1400px; margin: 0 auto; }}
  h1 {{ text-align: center; font-size: 2em; margin-bottom: 4px; background: linear-gradient(135deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
  h2 {{ text-align: center; color: #64748b; font-size: 1.1em; margin-bottom: 24px; }}
  h3 {{ color: #38bdf8; margin: 32px 0 12px; font-size: 1.3em; border-bottom: 1px solid #1e293b; padding-bottom: 8px; }}
  .section-desc {{ color: #94a3b8; font-size: 0.9em; margin-bottom: 16px; }}
  .chart-box {{ background: #1e293b; border-radius: 12px; padding: 24px; margin-bottom: 24px; position: relative; height: 550px; }}
  .toggle-container {{ text-align: center; margin-bottom: 16px; }}
  .toggle-btn {{ background: #334155; color: #94a3b8; border: 1px solid #475569; padding: 8px 20px; cursor: pointer; font-size: 0.95em; transition: all 0.2s; }}
  .toggle-btn:first-child {{ border-radius: 8px 0 0 8px; }}
  .toggle-btn:last-child {{ border-radius: 0 8px 8px 0; }}
  .toggle-btn.active {{ background: #3b82f6; color: white; border-color: #3b82f6; }}
  .toggle-btn:hover {{ background: #475569; color: #e2e8f0; }}
  .toggle-btn.active:hover {{ background: #2563eb; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
  th {{ text-align: left; padding: 10px 12px; border-bottom: 2px solid #334155; color: #94a3b8; font-size: 0.85em; text-transform: uppercase; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #1e293b; font-size: 0.95em; }}
  tr:hover td {{ background: #1e293b; }}
  .rank {{ color: #64748b; font-weight: 600; width: 40px; }}
  .score {{ font-weight: 700; color: #34d399; }}
  .year {{ color: #a78bfa; font-weight: 600; }}
  .hot-val {{ color: #22c55e; font-weight: 700; }}
  .cold-val {{ color: #ef4444; font-weight: 700; }}
  .hot-tag {{ background: #22c55e22; color: #22c55e; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; letter-spacing: 0.05em; }}
  .cold-tag {{ background: #ef444422; color: #ef4444; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; letter-spacing: 0.05em; }}
  .steady-tag {{ background: #64748b22; color: #64748b; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; letter-spacing: 0.05em; }}
  th.sortable {{ cursor: pointer; user-select: none; position: relative; padding-right: 18px; }}
  th.sortable:hover {{ color: #e2e8f0; }}
  th.sortable::after {{ content: '\\2195'; position: absolute; right: 4px; opacity: 0.3; font-size: 0.8em; }}
  th.sortable.asc::after {{ content: '\\2191'; opacity: 0.8; }}
  th.sortable.desc::after {{ content: '\\2193'; opacity: 0.8; }}
  .best-table td:first-child {{ font-size: 1.2em; width: 36px; text-align: center; }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>
<div class="container">
<h1>Summertime Sadness Fantasy Baseball</h1>
<h2>2025 Season Trends</h2>

<div class="toggle-container">
  <button class="toggle-btn active" onclick="showChart('score')">Power Score</button>
  <button class="toggle-btn" onclick="showChart('rank')">Power Rank</button>
</div>

<div class="chart-box" id="scoreBox">
<canvas id="scoreChart"></canvas>
</div>

<div class="chart-box" id="rankBox" style="display:none">
<canvas id="rankChart"></canvas>
</div>

<h3>Expected Category Wins by Week</h3>
<p class="section-desc">Each team's expected category wins per week based on all-play H2H simulation (every team vs every team, 12 categories). Higher = dominated the field that week. Range is 0-12.</p>
<div class="chart-box">
<canvas id="xwinsChart"></canvas>
</div>

<h3>Cumulative Expected Wins</h3>
<p class="section-desc">Running total of expected category wins over the season. Steeper slopes = hot streaks. Flatter = cold stretches. The gap between lines shows separation between teams.</p>
<div class="chart-box">
<canvas id="cumChart"></canvas>
</div>

<h3>Batter vs Pitcher Performance</h3>
<p class="section-desc">Each team's batting win rate (x-axis) vs pitching win rate (y-axis) from all-play H2H. Batting = R, H, HR, RBI, SB, OPS, TB. Pitching = K/9, QS, SVH, ERA, WHIP. Top-right = elite at both. Dashed lines at 50%.</p>
<div class="chart-box">
<canvas id="scatterChart"></canvas>
</div>

<h3>Who's Hot / Who's Not</h3>
<p class="section-desc">Average expected category wins over the last 2 weeks ({last_2_label}) vs season average. A team trending above their season average is heating up.</p>
<table class="sortable-table">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="num">Last 2 Wk Avg</th><th class="sortable" data-type="num">Season Avg</th><th class="sortable" data-type="num">Diff</th><th class="sortable" data-type="str">Trend</th></tr>
{hot_rows}
</table>

<h3>Season's Best Weekly Performances</h3>
<p class="section-desc">The single best (or lowest for ERA/WHIP) team stat line in any week this season across all 12 scoring categories.</p>
<table class="best-table">
<tr><th></th><th>Category</th><th>Value</th><th>Team</th><th>Week</th></tr>
{best_rows}
</table>

</div>
<script>
// ---- Chart options shared ----
const legendOpts = {{
  position: 'bottom',
  labels: {{ color: '#e2e8f0', padding: 16, font: {{ size: 12 }}, usePointStyle: true, pointStyle: 'circle' }}
}};
const tooltipOpts = {{
  backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8',
  borderColor: '#475569', borderWidth: 1, padding: 12
}};

// ---- Power Score Chart ----
new Chart(document.getElementById('scoreChart').getContext('2d'), {{
  type: 'line',
  data: {{ labels: {power_labels}, datasets: [{score_ds}] }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    layout: {{ padding: {{ top: 8, right: 16 }} }},
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{ legend: legendOpts, tooltip: {{ ...tooltipOpts, callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b33' }} }},
      y: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Power Score', color: '#64748b' }} }}
    }}
  }}
}});

// ---- Power Rank Chart ----
new Chart(document.getElementById('rankChart').getContext('2d'), {{
  type: 'line',
  data: {{ labels: {power_labels}, datasets: [{rank_ds}] }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    layout: {{ padding: {{ top: 8, right: 16 }} }},
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{ legend: legendOpts, tooltip: tooltipOpts }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b33' }} }},
      y: {{ reverse: true, min: 1, max: 12, ticks: {{ color: '#94a3b8', stepSize: 1 }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Power Rank', color: '#64748b' }} }}
    }}
  }}
}});

// ---- xWins by Week Chart ----
new Chart(document.getElementById('xwinsChart').getContext('2d'), {{
  type: 'line',
  data: {{ labels: {stat_labels}, datasets: [{xwin_ds}] }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    layout: {{ padding: {{ top: 8, right: 16 }} }},
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{ legend: legendOpts, tooltip: {{ ...tooltipOpts, callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b33' }} }},
      y: {{ min: 0, max: 12, ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Expected Category Wins', color: '#64748b' }} }}
    }}
  }}
}});

// ---- Cumulative xWins Chart ----
new Chart(document.getElementById('cumChart').getContext('2d'), {{
  type: 'line',
  data: {{ labels: {stat_labels}, datasets: [{cum_ds}] }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    layout: {{ padding: {{ top: 8, right: 16 }} }},
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{ legend: legendOpts, tooltip: {{ ...tooltipOpts, callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b33' }} }},
      y: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Cumulative Expected Wins', color: '#64748b' }} }}
    }}
  }}
}});

// ---- Batter vs Pitcher Scatter ----
const quadrantPlugin = {{
  id: 'quadrant',
  beforeDraw(chart) {{
    const {{ctx, scales: {{x, y}}}} = chart;
    const x50 = x.getPixelForValue(50);
    const y50 = y.getPixelForValue(50);
    ctx.save();
    ctx.strokeStyle = '#47556966';
    ctx.lineWidth = 1;
    ctx.setLineDash([5, 5]);
    ctx.beginPath(); ctx.moveTo(x50, y.top); ctx.lineTo(x50, y.bottom); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(x.left, y50); ctx.lineTo(x.right, y50); ctx.stroke();
    ctx.fillStyle = '#47556955';
    ctx.font = '11px Segoe UI';
    ctx.textAlign = 'center';
    const lx = x.getPixelForValue((x.min + 50) / 2);
    const rx = x.getPixelForValue((x.max + 50) / 2);
    const ty = y.getPixelForValue((y.max + 50) / 2);
    const by = y.getPixelForValue((y.min + 50) / 2);
    ctx.fillText('Pitching Carry', lx, ty);
    ctx.fillText('Elite', rx, ty);
    ctx.fillText('Struggling', lx, by);
    ctx.fillText('Batting Carry', rx, by);
    ctx.restore();
  }}
}};
const labelPlugin = {{
  id: 'scatterLabels',
  afterDatasetDraw(chart) {{
    const {{ctx}} = chart;
    chart.data.datasets.forEach((ds, i) => {{
      chart.getDatasetMeta(i).data.forEach(pt => {{
        ctx.save();
        ctx.font = '10px Segoe UI';
        ctx.fillStyle = '#94a3b8';
        ctx.textAlign = 'center';
        ctx.fillText(ds.label, pt.x, pt.y - 14);
        ctx.restore();
      }});
    }});
  }}
}};
new Chart(document.getElementById('scatterChart').getContext('2d'), {{
  type: 'scatter',
  data: {{ datasets: [{scatter_ds}] }},
  plugins: [quadrantPlugin, labelPlugin],
  options: {{
    responsive: true, maintainAspectRatio: false,
    layout: {{ padding: {{ top: 28, right: 40, bottom: 8, left: 8 }} }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        ...tooltipOpts,
        callbacks: {{
          label: ctx => ctx.dataset.label + ' - Bat: ' + ctx.parsed.x.toFixed(1) + '%, Pitch: ' + ctx.parsed.y.toFixed(1) + '%'
        }}
      }}
    }},
    scales: {{
      x: {{ title: {{ display: true, text: 'Batting Win %', color: '#64748b' }}, ticks: {{ color: '#94a3b8', callback: v => v + '%' }}, grid: {{ color: '#334155' }} }},
      y: {{ title: {{ display: true, text: 'Pitching Win %', color: '#64748b' }}, ticks: {{ color: '#94a3b8', callback: v => v + '%' }}, grid: {{ color: '#334155' }} }}
    }}
  }}
}});

// ---- Toggle Power Score / Rank ----
function showChart(type) {{
  document.querySelectorAll('.toggle-btn').forEach(b => b.classList.remove('active'));
  if (type === 'score') {{
    document.getElementById('scoreBox').style.display = 'block';
    document.getElementById('rankBox').style.display = 'none';
    document.querySelectorAll('.toggle-btn')[0].classList.add('active');
  }} else {{
    document.getElementById('scoreBox').style.display = 'none';
    document.getElementById('rankBox').style.display = 'block';
    document.querySelectorAll('.toggle-btn')[1].classList.add('active');
  }}
}}

// ---- Sortable tables ----
document.querySelectorAll('th.sortable').forEach(th => {{
  th.addEventListener('click', () => {{
    const tbl = th.closest('table');
    const idx = Array.from(th.parentElement.children).indexOf(th);
    const rows = Array.from(tbl.querySelectorAll('tr')).slice(1);
    const type = th.dataset.type || 'str';
    const isAsc = th.classList.contains('asc');
    th.parentElement.querySelectorAll('th').forEach(h => h.classList.remove('asc', 'desc'));
    th.classList.add(isAsc ? 'desc' : 'asc');
    const getVal = (row, i) => {{
      const text = row.children[i].textContent.trim();
      if (type === 'num') return parseFloat(text.replace(/[+#]/g, '')) || 0;
      return text.toLowerCase();
    }};
    rows.sort((a, b) => {{
      const va = getVal(a, idx), vb = getVal(b, idx);
      const cmp = typeof va === 'string' ? va.localeCompare(vb) : va - vb;
      return isAsc ? -cmp : cmp;
    }});
    const tbody = tbl.querySelector('tbody') || tbl;
    rows.forEach(r => tbody.appendChild(r));
  }});
}});
</script>
</body>
</html>"""

with open(r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\season_trends_2025.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\nGenerated docs/season_trends_2025.html')
print(f'{len(sorted_teams)} teams, power weeks {power_weeks[0]}-{power_weeks[-1]}, stat weeks {stat_weeks[0]}-{stat_weeks[-1]}')
for i, (tn, data) in enumerate(sorted_teams):
    bat = season_bat[tn]
    pit = season_pit[tn]
    bpct = round(bat['w'] / bat['total'] * 100, 1) if bat['total'] > 0 else 0
    ppct = round(pit['w'] / pit['total'] * 100, 1) if pit['total'] > 0 else 0
    final = data.get(max(power_weeks), {}).get('score', 0)
    print(f'  {colors[i % len(colors)]} {tn_latest_name[tn]} (Score:{final:.0f} Bat:{bpct}% Pit:{ppct}%)')
