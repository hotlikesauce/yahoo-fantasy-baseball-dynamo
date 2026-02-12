"""
Generate season trends pages for historical years (2022-2024) from HistoricalSeasons table.

Adapts to each year's available data:
- 2022: Power score/rank charts + standings only (no weekly_stats)
- 2023-2024: Full charts (power, xWins, scatter, hot/cold, season's best)

Usage:
  python scripts/gen_historical_trends.py 2024
  python scripts/gen_historical_trends.py 2023
  python scripts/gen_historical_trends.py 2022
  python scripts/gen_historical_trends.py all   # generate all years
"""

import boto3, json, sys, io, statistics
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from decimal import Decimal

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

# Categories per year
CATS_2022 = {
    'high': ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K9', 'QS', 'SVH', 'NW'],
    'low': ['ERA', 'WHIP'],
    'batter': ['R', 'H', 'HR', 'RBI', 'SB', 'OPS'],
    'pitcher_high': ['K9', 'QS', 'SVH', 'NW'],
    'pitcher_low': ['ERA', 'WHIP'],
}
CATS_DEFAULT = {
    'high': ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'HRA', 'K9', 'QS', 'SVH'],
    'low': ['ERA', 'WHIP'],
    'batter': ['R', 'H', 'HR', 'RBI', 'SB', 'OPS'],
    'pitcher_high': ['HRA', 'K9', 'QS', 'SVH'],
    'pitcher_low': ['ERA', 'WHIP'],
}

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]

CAT_LABELS = {
    'R': 'Runs', 'H': 'Hits', 'HR': 'Home Runs', 'RBI': 'RBI', 'SB': 'Stolen Bases',
    'OPS': 'OPS', 'TB': 'Total Bases', 'K9': 'K/9', 'QS': 'Quality Starts',
    'SVH': 'Saves+Holds', 'ERA': 'ERA (Lowest)', 'WHIP': 'WHIP (Lowest)',
    'HRA': 'HR Allowed (Lowest)', 'NW': 'Net Wins',
}


def dec(val):
    """Convert Decimal/string to float."""
    if val is None or val == '-' or val == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def query_data_type(year, data_type):
    """Query all items for a year + data_type from HistoricalSeasons."""
    items = []
    resp = table.query(
        IndexName='YearDataTypeIndex',
        KeyConditionExpression=Key('YearDataType').eq(f'{year}#{data_type}')
    )
    items.extend(resp['Items'])
    while 'LastEvaluatedKey' in resp:
        resp = table.query(
            IndexName='YearDataTypeIndex',
            KeyConditionExpression=Key('YearDataType').eq(f'{year}#{data_type}'),
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        items.extend(resp['Items'])
    return items


def h2h_result(a, b, cats):
    wa = wb = 0
    for c in cats['high']:
        if c in a and c in b and a[c] is not None and b[c] is not None:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    for c in cats['low']:
        if c in a and c in b and a[c] is not None and b[c] is not None:
            if a[c] < b[c]: wa += 1
            elif b[c] < a[c]: wb += 1
    return wa, wb


def h2h_split(a, b, split_cats, low_cats):
    wa = wb = 0
    for c in split_cats:
        if c in a and c in b and a[c] is not None and b[c] is not None:
            if c in low_cats:
                if a[c] < b[c]: wa += 1
                elif b[c] < a[c]: wb += 1
            else:
                if a[c] > b[c]: wa += 1
                elif b[c] > a[c]: wb += 1
    return wa, wb


def js_escape(s):
    return s.replace('\\', '\\\\').replace("'", "\\'")


def generate_year(year):
    print(f"\n{'='*60}")
    print(f"Generating season trends for {year}")
    print(f"{'='*60}")

    cats = CATS_2022 if year == 2022 else CATS_DEFAULT
    all_cats = cats['high'] + cats['low']

    # ================================================================
    # 1. Pull power_ranks_season_trend
    # ================================================================
    power_items = query_data_type(year, 'power_ranks_season_trend')
    print(f"  power_ranks_season_trend: {len(power_items)} items")

    # Build team mapping and power data
    tn_latest_name = {}
    power_data = {}

    for item in power_items:
        tn = str(item.get('TeamNumber', ''))
        team = item.get('Team', f'Team {tn}')
        week = int(dec(item.get('Week', 0)) or 0)
        if week == 0:
            continue

        tn_latest_name[tn] = team  # last write wins = latest week

        # Score: prefer Score_Sum, fallback to Stats_Power_Score
        score = dec(item.get('Score_Sum')) or dec(item.get('Stats_Power_Score'))
        rank = dec(item.get('Stats_Power_Rank'))

        if score is None or rank is None:
            continue

        if tn not in power_data:
            power_data[tn] = {}
        power_data[tn][week] = {'score': score, 'rank': rank}

    power_weeks = sorted(set(w for d in power_data.values() for w in d))
    print(f"  Power weeks: {power_weeks[0] if power_weeks else '?'}-{power_weeks[-1] if power_weeks else '?'}")
    print(f"  Teams: {len(tn_latest_name)}")
    for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x) if x.isdigit() else 999):
        print(f"    {tn}: {tn_latest_name[tn]}")

    # ================================================================
    # 2. Pull weekly_stats (if available)
    # ================================================================
    ws_items = query_data_type(year, 'weekly_stats')
    has_weekly_stats = len(ws_items) > 0
    print(f"  weekly_stats: {len(ws_items)} items {'(available)' if has_weekly_stats else '(NOT AVAILABLE)'}")

    weekly_stats = defaultdict(dict)
    if has_weekly_stats:
        # Build name->tn mapping for weekly_stats (may use different names)
        name_to_tn = {}
        for item in power_items:
            name_to_tn[item.get('Team', '')] = str(item.get('TeamNumber', ''))

        for item in ws_items:
            week = int(dec(item.get('Week', 0)) or 0)
            if week == 0:
                continue
            tn = str(item.get('TeamNumber', ''))
            if not tn or tn == 'None':
                # Try name lookup
                team = item.get('Team', '')
                tn = name_to_tn.get(team, '')
            if not tn:
                continue

            stats = {}
            for c in all_cats:
                v = dec(item.get(c))
                if v is not None:
                    stats[c] = v
            if stats:
                weekly_stats[week][tn] = stats

    stat_weeks = sorted(w for w in weekly_stats.keys() if w <= 20)  # exclude playoff weeks
    print(f"  Stat weeks: {stat_weeks[0] if stat_weeks else 'N/A'}-{stat_weeks[-1] if stat_weeks else 'N/A'}")

    # ================================================================
    # 3. H2H Simulation (if weekly_stats available)
    # ================================================================
    weekly_xwins = defaultdict(dict)
    season_bat = defaultdict(lambda: {'w': 0, 'total': 0})
    season_pit = defaultdict(lambda: {'w': 0, 'total': 0})

    if has_weekly_stats:
        for week in stat_weeks:
            tns = list(weekly_stats[week].keys())
            for i, ta in enumerate(tns):
                cat_w = 0
                for j, tb in enumerate(tns):
                    if i == j:
                        continue
                    wa, wb = h2h_result(weekly_stats[week][ta], weekly_stats[week][tb], cats)
                    cat_w += wa
                    ba, _ = h2h_split(weekly_stats[week][ta], weekly_stats[week][tb], cats['batter'], cats['low'])
                    season_bat[ta]['w'] += ba
                    season_bat[ta]['total'] += len(cats['batter'])
                    pa, _ = h2h_split(weekly_stats[week][ta], weekly_stats[week][tb],
                                      cats['pitcher_high'] + cats['pitcher_low'], cats['pitcher_low'])
                    season_pit[ta]['w'] += pa
                    season_pit[ta]['total'] += len(cats['pitcher_high']) + len(cats['pitcher_low'])
                n_opps = len(tns) - 1
                weekly_xwins[week][ta] = cat_w / n_opps if n_opps > 0 else 0
        print(f"  H2H simulation complete")

    # ================================================================
    # 4. Build chart data
    # ================================================================
    sorted_teams = sorted(power_data.items(),
        key=lambda x: x[1].get(max(power_weeks), {}).get('score', 0), reverse=True)
    color_map = {tn: COLORS[i % len(COLORS)] for i, (tn, _) in enumerate(sorted_teams)}

    def js_label(tn):
        return js_escape(tn_latest_name.get(tn, f'Team {tn}'))

    # Power Score/Rank datasets
    def make_power_ds(field):
        datasets = []
        for i, (tn, data) in enumerate(sorted_teams):
            points = [round(data[w][field], 1) if w in data else None for w in power_weeks]
            datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: {json.dumps(points)},
      borderColor: '{COLORS[i % len(COLORS)]}', backgroundColor: '{COLORS[i % len(COLORS)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
        return ',\n      '.join(datasets)

    score_ds = make_power_ds('score')
    rank_ds = make_power_ds('rank')
    power_labels = json.dumps([f'Week {w}' for w in power_weeks])

    # xWins datasets (if available)
    xwin_ds = ''
    stat_labels = '[]'
    cum_ds = ''
    scatter_ds = ''
    hot_rows = ''
    best_rows = ''
    last_2_label = ''

    if has_weekly_stats and stat_weeks:
        xwin_datasets = []
        for i, (tn, _) in enumerate(sorted_teams):
            points = [round(weekly_xwins[w].get(tn, 0), 2) for w in stat_weeks]
            xwin_datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: {json.dumps(points)},
      borderColor: '{COLORS[i % len(COLORS)]}', backgroundColor: '{COLORS[i % len(COLORS)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
        xwin_ds = ',\n      '.join(xwin_datasets)
        stat_labels = json.dumps([f'Wk {w}' for w in stat_weeks])

        # Cumulative xWins
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
      borderColor: '{COLORS[i % len(COLORS)]}', backgroundColor: '{COLORS[i % len(COLORS)]}22',
      borderWidth: 2.5, pointRadius: 3, pointHoverRadius: 6, tension: 0.3, fill: false
    }}""")
        cum_ds = ',\n      '.join(cum_datasets)

        # Scatter
        scatter_datasets = []
        for i, (tn, _) in enumerate(sorted_teams):
            bat = season_bat[tn]
            pit = season_pit[tn]
            bx = round(bat['w'] / bat['total'] * 100, 1) if bat['total'] > 0 else 50
            py_ = round(pit['w'] / pit['total'] * 100, 1) if pit['total'] > 0 else 50
            scatter_datasets.append(f"""{{
      label: '{js_label(tn)}',
      data: [{{x: {bx}, y: {py_}}}],
      backgroundColor: '{COLORS[i % len(COLORS)]}', borderColor: '{COLORS[i % len(COLORS)]}',
      pointRadius: 8, pointHoverRadius: 11
    }}""")
        scatter_ds = ',\n      '.join(scatter_datasets)

        # Hot/Cold
        last_2 = stat_weeks[-2:] if len(stat_weeks) >= 2 else stat_weeks
        last_2_label = f"Weeks {last_2[0]}-{last_2[-1]}" if len(last_2) > 1 else f"Week {last_2[0]}"
        hot_data = []
        for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x) if x.isdigit() else 999):
            recent = [weekly_xwins[w].get(tn, 0) for w in last_2]
            recent_avg = sum(recent) / len(recent) if recent else 0
            season_avg = sum(weekly_xwins[w].get(tn, 0) for w in stat_weeks) / len(stat_weeks) if stat_weeks else 0
            hot_data.append({'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'recent': round(recent_avg, 2), 'season': round(season_avg, 2),
                'diff': round(recent_avg - season_avg, 2)})
        hot_data.sort(key=lambda x: x['recent'], reverse=True)
        for i, d in enumerate(hot_data):
            color = color_map.get(d['tn'], '#94a3b8')
            if d['diff'] > 0.5: trend = '<span class="hot-tag">HOT</span>'
            elif d['diff'] < -0.5: trend = '<span class="cold-tag">COLD</span>'
            else: trend = '<span class="steady-tag">STEADY</span>'
            diff_class = "hot-val" if d['diff'] > 0 else "cold-val" if d['diff'] < 0 else ""
            diff_val = f"+{d['diff']}" if d['diff'] > 0 else str(d['diff'])
            hot_rows += f'<tr><td class="rank">{i+1}</td><td><span style="color:{color}">&#9679;</span> {d["name"]}</td><td class="score">{d["recent"]:.2f}</td><td>{d["season"]:.2f}</td><td class="{diff_class}">{diff_val}</td><td>{trend}</td></tr>'

        # Season's Best
        season_best = {}
        for cat in all_cats:
            is_low = cat in cats['low']
            best = None
            for week in stat_weeks:
                for tn, stats in weekly_stats[week].items():
                    if cat not in stats:
                        continue
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
        for cat in all_cats:
            if cat not in season_best:
                continue
            b = season_best[cat]
            color = color_map.get(b['tn'], '#94a3b8')
            icon = '&#x26BE;' if cat in cats['batter'] else '&#x1F3AF;'
            best_rows += f'<tr><td>{icon}</td><td class="year">{CAT_LABELS.get(cat, cat)}</td><td class="score">{b["formatted"]}</td><td><span style="color:{color}">&#9679;</span> {b["team"]}</td><td class="year">Week {b["week"]}</td></tr>'

    # ================================================================
    # 5. Build conditional HTML sections
    # ================================================================
    xwins_section = ""
    if has_weekly_stats:
        xwins_section = f"""
<h3>Expected Category Wins by Week</h3>
<p class="section-desc">Each team's expected category wins per week based on all-play H2H simulation. Higher = dominated the field that week.</p>
<div class="chart-box">
<canvas id="xwinsChart"></canvas>
</div>

<h3>Cumulative Expected Wins</h3>
<p class="section-desc">Running total of expected category wins. Steeper slopes = hot streaks.</p>
<div class="chart-box">
<canvas id="cumChart"></canvas>
</div>

<h3>Batter vs Pitcher Performance</h3>
<p class="section-desc">Batting win rate (x) vs pitching win rate (y) from all-play H2H. Top-right = elite at both.</p>
<div class="chart-box">
<canvas id="scatterChart"></canvas>
</div>

<h3>Who's Hot / Who's Not</h3>
<p class="section-desc">Last 2 weeks ({last_2_label}) vs season average.</p>
<table class="sortable-table">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="num">Last 2 Wk Avg</th><th class="sortable" data-type="num">Season Avg</th><th class="sortable" data-type="num">Diff</th><th class="sortable" data-type="str">Trend</th></tr>
{hot_rows}
</table>

<h3>Season's Best Weekly Performances</h3>
<p class="section-desc">Best (or lowest for ERA/WHIP) team stat in any week.</p>
<table class="best-table">
<tr><th></th><th>Category</th><th>Value</th><th>Team</th><th>Week</th></tr>
{best_rows}
</table>"""

    xwins_js = ""
    if has_weekly_stats:
        xwins_js = f"""
// ---- xWins by Week ----
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

// ---- Cumulative xWins ----
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

// ---- Scatter ----
const quadrantPlugin = {{
  id: 'quadrant',
  beforeDraw(chart) {{
    const {{ctx, scales: {{x, y}}}} = chart;
    const x50 = x.getPixelForValue(50), y50 = y.getPixelForValue(50);
    ctx.save();
    ctx.strokeStyle = '#47556966'; ctx.lineWidth = 1; ctx.setLineDash([5, 5]);
    ctx.beginPath(); ctx.moveTo(x50, y.top); ctx.lineTo(x50, y.bottom); ctx.stroke();
    ctx.beginPath(); ctx.moveTo(x.left, y50); ctx.lineTo(x.right, y50); ctx.stroke();
    ctx.fillStyle = '#47556955'; ctx.font = '11px Segoe UI'; ctx.textAlign = 'center';
    const lx = x.getPixelForValue((x.min+50)/2), rx = x.getPixelForValue((x.max+50)/2);
    const ty = y.getPixelForValue((y.max+50)/2), by = y.getPixelForValue((y.min+50)/2);
    ctx.fillText('Pitching Carry', lx, ty); ctx.fillText('Elite', rx, ty);
    ctx.fillText('Struggling', lx, by); ctx.fillText('Batting Carry', rx, by);
    ctx.restore();
  }}
}};
const labelPlugin = {{
  id: 'scatterLabels',
  afterDatasetDraw(chart) {{
    const {{ctx}} = chart;
    chart.data.datasets.forEach((ds, i) => {{
      chart.getDatasetMeta(i).data.forEach(pt => {{
        ctx.save(); ctx.font = '10px Segoe UI'; ctx.fillStyle = '#94a3b8'; ctx.textAlign = 'center';
        ctx.fillText(ds.label, pt.x, pt.y - 14); ctx.restore();
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
      tooltip: {{ ...tooltipOpts, callbacks: {{ label: ctx => ctx.dataset.label + ' - Bat: ' + ctx.parsed.x.toFixed(1) + '%, Pitch: ' + ctx.parsed.y.toFixed(1) + '%' }} }}
    }},
    scales: {{
      x: {{ title: {{ display: true, text: 'Batting Win %', color: '#64748b' }}, ticks: {{ color: '#94a3b8', callback: v => v + '%' }}, grid: {{ color: '#334155' }} }},
      y: {{ title: {{ display: true, text: 'Pitching Win %', color: '#64748b' }}, ticks: {{ color: '#94a3b8', callback: v => v + '%' }}, grid: {{ color: '#334155' }} }}
    }}
  }}
}});"""

    # ================================================================
    # 6. Generate HTML
    # ================================================================
    score_label = "Normalized Score" if year in [2022, 2023] else "Power Score"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Summertime Sadness - {year} Season Trends</title>
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
  .hot-tag {{ background: #22c55e22; color: #22c55e; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; }}
  .cold-tag {{ background: #ef444422; color: #ef4444; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; }}
  .steady-tag {{ background: #64748b22; color: #64748b; padding: 2px 12px; border-radius: 12px; font-size: 0.8em; font-weight: 700; }}
  th.sortable {{ cursor: pointer; user-select: none; position: relative; padding-right: 18px; }}
  th.sortable:hover {{ color: #e2e8f0; }}
  th.sortable::after {{ content: '\\2195'; position: absolute; right: 4px; opacity: 0.3; font-size: 0.8em; }}
  th.sortable.asc::after {{ content: '\\2191'; opacity: 0.8; }}
  th.sortable.desc::after {{ content: '\\2193'; opacity: 0.8; }}
  .best-table td:first-child {{ font-size: 1.2em; width: 36px; text-align: center; }}
  .archive-badge {{ display: inline-block; background: #334155; color: #94a3b8; padding: 4px 16px; border-radius: 20px; font-size: 0.85em; margin-bottom: 8px; }}
  @media (max-width: 640px) {{
    body {{ padding: 10px; }}
    h1 {{ font-size: 1.4em; }}
    h2 {{ font-size: 0.95em; }}
    h3 {{ font-size: 1.1em; }}
    .chart-box {{ height: 300px; padding: 12px; }}
    .toggle-btn {{ padding: 6px 12px; font-size: 0.82em; }}
    th {{ padding: 8px 6px; font-size: 0.75em; }}
    td {{ padding: 6px 6px; font-size: 0.82em; }}
  }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>
<div class="container">
<h1>Summertime Sadness Fantasy Baseball</h1>
<div style="text-align:center"><span class="archive-badge">Season Archive</span></div>
<h2>{year} Season Trends</h2>

<div class="toggle-container">
  <button class="toggle-btn active" onclick="showChart('score')">{score_label}</button>
  <button class="toggle-btn" onclick="showChart('rank')">Power Rank</button>
</div>

<div class="chart-box" id="scoreBox">
<canvas id="scoreChart"></canvas>
</div>

<div class="chart-box" id="rankBox" style="display:none">
<canvas id="rankChart"></canvas>
</div>

{xwins_section}

</div>
<script>
const legendOpts = {{
  position: 'bottom',
  labels: {{ color: '#e2e8f0', padding: 16, font: {{ size: 12 }}, usePointStyle: true, pointStyle: 'circle' }}
}};
const tooltipOpts = {{
  backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8',
  borderColor: '#475569', borderWidth: 1, padding: 12
}};

// ---- Power Score ----
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
      y: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: '{score_label}', color: '#64748b' }} }}
    }}
  }}
}});

// ---- Power Rank ----
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
{xwins_js}

// ---- Toggle ----
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

    outpath = f'c:\\Users\\taylor.ward\\Documents\\yahoo-fantasy-baseball-dynamo\\docs\\season_trends_{year}.html'
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n  Generated docs/season_trends_{year}.html")
    print(f"  {len(sorted_teams)} teams, power weeks {power_weeks[0]}-{power_weeks[-1]}")
    if stat_weeks:
        print(f"  Stat weeks {stat_weeks[0]}-{stat_weeks[-1]}, {len(all_cats)} categories")
    else:
        print(f"  No weekly stats (power charts only)")


# ============================================================
# Main
# ============================================================
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python gen_historical_trends.py <year|all>")
        sys.exit(1)

    if sys.argv[1].lower() == 'all':
        years = [2022, 2023, 2024]
    else:
        years = [int(sys.argv[1])]

    for year in years:
        generate_year(year)

    print(f"\nDone! Generated {len(years)} page(s).")
