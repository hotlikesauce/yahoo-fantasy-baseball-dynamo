"""
Generate all-time manager vs manager H2H records page.

Queries weekly_results from HistoricalSeasons (2023-2025) and builds:
- 12x12 H2H matrix with year filter
- Most dominant performances (biggest blowouts)
- Most even matchups (closest games)
- Most lopsided rivalries
- Most even rivalries

Team numbers shuffle across years when the Yahoo league resets,
so we hardcode the (year, tn) -> manager mapping.

Usage:
  python scripts/gen_h2h_records.py
"""

import boto3, json, sys, io
from boto3.dynamodb.conditions import Key
from collections import defaultdict
from decimal import Decimal

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

from team_config import MANAGERS, YEAR_TN_TO_MANAGER

YEARS = [2023, 2024, 2025]

# Build manager list from all managers who appear in the H2H years
# (includes historical managers like David who aren't in current MANAGERS)
ALL_H2H_MANAGERS = sorted(set(
    mgr for (y, tn), mgr in YEAR_TN_TO_MANAGER.items() if y in YEARS
))

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
    '#64748b',
]

MGR_IDX = {name: i for i, name in enumerate(ALL_H2H_MANAGERS)}
NUM_MGRS = len(ALL_H2H_MANAGERS)


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


def js_safe(s):
    return s.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n')


# ============================================================
# 1. Build per-year name -> manager mapping
# ============================================================
print("Building name -> manager mappings...")

year_name_to_mgr = {y: {} for y in YEARS}  # year -> {team_name: manager}
year_mgr_latest = {y: {} for y in YEARS}    # year -> {manager: latest_team_name}

for year in YEARS:
    # First pass: power_ranks_season_trend for stable name mapping
    items = query_data_type(year, 'power_ranks_season_trend')
    tn_max_week = {}
    for item in items:
        tn = str(item.get('TeamNumber', ''))
        name = item.get('Team', '')
        week = int(item.get('Week', 0))
        if not tn or not name:
            continue
        mgr = YEAR_TN_TO_MANAGER.get((year, tn))
        if not mgr:
            continue
        year_name_to_mgr[year][name] = mgr
        if week > tn_max_week.get(mgr, 0):
            tn_max_week[mgr] = week
            year_mgr_latest[year][mgr] = name

    # Second pass: weekly_results Team names (catches mid-season name changes)
    wr_items = query_data_type(year, 'weekly_results')
    for item in wr_items:
        tn = str(item.get('TeamNumber', ''))
        name = item.get('Team', '')
        if tn and name and name not in year_name_to_mgr[year]:
            mgr = YEAR_TN_TO_MANAGER.get((year, tn))
            if mgr:
                year_name_to_mgr[year][name] = mgr

    # Third pass: cross-reference opponent names within each week
    # If opponent name X is unresolved but in this week Team Y has Team name X,
    # then X maps to Y's manager
    week_items = defaultdict(list)
    for item in wr_items:
        week_items[int(item.get('Week', 0))].append(item)
    for week, items_in_week in week_items.items():
        # Build week-specific TN -> Team name
        week_tn_name = {}
        for item in items_in_week:
            tn = str(item.get('TeamNumber', ''))
            name = item.get('Team', '')
            if tn and name:
                week_tn_name[tn] = name
        # Map any opponent names that match a Team name in this week
        for item in items_in_week:
            opp = item.get('Opponent', '')
            if opp and opp not in year_name_to_mgr[year]:
                # Find which TN has this opponent as their Team name this week
                for other_tn, other_name in week_tn_name.items():
                    if other_name == opp:
                        mgr = YEAR_TN_TO_MANAGER.get((year, other_tn))
                        if mgr:
                            year_name_to_mgr[year][opp] = mgr
                            break

    print(f"  {year}: {len(year_name_to_mgr[year])} names mapped")
    for mgr in ALL_H2H_MANAGERS:
        latest = year_mgr_latest[year].get(mgr, '???')
        print(f"    {mgr:>8}: {latest}")

# ============================================================
# 2. Query weekly_results and aggregate by manager
# ============================================================
print("\nQuerying weekly_results...")

def new_rec(): return {'w': 0, 'l': 0, 't': 0, 'sf': 0.0, 'sa': 0.0, 'g': 0}
h2h_by_year = {y: defaultdict(new_rec) for y in YEARS}
h2h_alltime = defaultdict(new_rec)

all_matchups = []
skipped = 0

for year in YEARS:
    items = query_data_type(year, 'weekly_results')
    print(f"  {year}: {len(items)} items")

    seen = set()

    for item in items:
        tn = str(item.get('TeamNumber', ''))
        team_name = item.get('Team', '')
        opp_name = item.get('Opponent', '')
        score = float(item.get('Score', 0))
        opp_score = float(item.get('Opponent_Score', 0))
        week = int(item.get('Week', 0))

        # Normalize: 2023-2024 stored raw W-L (ties not distributed),
        # 2025 stores tie-adjusted (0.5 per tie). Ensure all sum to 12.
        total = score + opp_score
        if total < 12:
            ties = 12 - total
            score += ties * 0.5
            opp_score += ties * 0.5

        # All years had 21 regular-season weeks; exclude playoff weeks (22+)
        if week > 21:
            continue

        mgr_a = YEAR_TN_TO_MANAGER.get((year, tn))
        mgr_b = year_name_to_mgr[year].get(opp_name)

        if not mgr_a or not mgr_b:
            skipped += 1
            continue

        idx_a = MGR_IDX[mgr_a]
        idx_b = MGR_IDX[mgr_b]

        # Record from A's perspective
        if score > opp_score:
            h2h_by_year[year][(idx_a, idx_b)]['w'] += 1
            h2h_alltime[(idx_a, idx_b)]['w'] += 1
        elif score < opp_score:
            h2h_by_year[year][(idx_a, idx_b)]['l'] += 1
            h2h_alltime[(idx_a, idx_b)]['l'] += 1
        else:
            h2h_by_year[year][(idx_a, idx_b)]['t'] += 1
            h2h_alltime[(idx_a, idx_b)]['t'] += 1

        # Track cumulative category scores
        h2h_by_year[year][(idx_a, idx_b)]['sf'] += score
        h2h_by_year[year][(idx_a, idx_b)]['sa'] += opp_score
        h2h_by_year[year][(idx_a, idx_b)]['g'] += 1
        h2h_alltime[(idx_a, idx_b)]['sf'] += score
        h2h_alltime[(idx_a, idx_b)]['sa'] += opp_score
        h2h_alltime[(idx_a, idx_b)]['g'] += 1

        # Collect individual matchup once per game
        key = (year, week, min(idx_a, idx_b), max(idx_a, idx_b))
        if key not in seen:
            seen.add(key)
            if score >= opp_score:
                w_name, l_name, w_sc, l_sc = team_name, opp_name, score, opp_score
            else:
                w_name, l_name, w_sc, l_sc = opp_name, team_name, opp_score, score
            all_matchups.append({
                'year': year, 'week': week,
                'winner': w_name, 'loser': l_name,
                'w_score': w_sc, 'l_score': l_sc,
                'margin': abs(score - opp_score),
                'is_tie': score == opp_score,
                'team_a': team_name, 'team_b': opp_name,
                'score_a': score, 'score_b': opp_score,
            })

if skipped:
    print(f"  (skipped {skipped} unresolved items)")

# Quick sanity: check symmetry
print("\nSymmetry check (A's wins vs B should equal B's losses vs A):")
errors = 0
for i in range(NUM_MGRS):
    for j in range(NUM_MGRS):
        if i == j:
            continue
        a_wins = h2h_alltime.get((i, j), {'w': 0})['w']
        b_losses = h2h_alltime.get((j, i), {'l': 0})['l']
        if a_wins != b_losses:
            print(f"  MISMATCH: {ALL_H2H_MANAGERS[i]} wins vs {ALL_H2H_MANAGERS[j]} = {a_wins}, but {ALL_H2H_MANAGERS[j]} losses vs {ALL_H2H_MANAGERS[i]} = {b_losses}")
            errors += 1
if errors == 0:
    print("  All records symmetric!")

# ============================================================
# 3. Compute rivalry stats
# ============================================================
rivalries = []
for i in range(NUM_MGRS):
    for j in range(i + 1, NUM_MGRS):
        rec_ij = h2h_alltime.get((i, j), new_rec())
        rec_ji = h2h_alltime.get((j, i), new_rec())
        total = rec_ij['w'] + rec_ij['l'] + rec_ij['t']
        if total == 0:
            continue
        win_pct = (rec_ij['w'] + rec_ij['t'] * 0.5) / total
        # Cumulative category scores from A's perspective
        sf_a = rec_ij['sf']  # categories A won against B
        sa_a = rec_ij['sa']  # categories B won against A
        rivalries.append({
            'i': i, 'j': j,
            'name_a': ALL_H2H_MANAGERS[i], 'name_b': ALL_H2H_MANAGERS[j],
            'w': rec_ij['w'], 'l': rec_ij['l'], 't': rec_ij['t'],
            'total': total, 'win_pct': win_pct,
            'imbalance': abs(win_pct - 0.5),
            'sf': round(sf_a, 1), 'sa': round(sa_a, 1),
            'g': rec_ij['g'],
        })

print(f"\nTotal unique matchups: {len(all_matchups)}")
print(f"Rivalries: {len(rivalries)}")

# ============================================================
# 4. Build matrix JS data
# ============================================================

def build_matrix(h2h_data):
    rows = {}
    for i in range(NUM_MGRS):
        cells = {}
        for j in range(NUM_MGRS):
            if i == j:
                cells[j] = {'w': '-', 'l': '-', 't': '-', 'sf': 0, 'sa': 0, 'g': 0, 'cls': 'self'}
            else:
                rec = h2h_data.get((i, j), new_rec())
                total = rec['w'] + rec['l'] + rec['t']
                cls = 'none' if total == 0 else ('winning' if rec['w'] > rec['l'] else ('losing' if rec['w'] < rec['l'] else 'even'))
                cells[j] = {'w': rec['w'], 'l': rec['l'], 't': rec['t'],
                             'sf': rec['sf'], 'sa': rec['sa'], 'g': rec['g'], 'cls': cls}
        rows[i] = cells
    return rows


def matrix_to_js(matrix):
    lines = []
    for i in range(NUM_MGRS):
        for j in range(NUM_MGRS):
            c = matrix[i][j]
            sf = json.dumps(round(c['sf'], 1) if isinstance(c['sf'], float) else c['sf'])
            sa = json.dumps(round(c['sa'], 1) if isinstance(c['sa'], float) else c['sa'])
            lines.append(f"m[{i}][{j}]={{w:{json.dumps(c['w'])},l:{json.dumps(c['l'])},t:{json.dumps(c['t'])},sf:{sf},sa:{sa},g:{c['g']},c:'{c['cls']}'}};")
    return '\n'.join(lines)


def full_matrix_js():
    parts = []
    parts.append("const D={};")
    parts.append(f"['alltime',{','.join(repr(str(y)) for y in YEARS)}].forEach(k=>D[k]={{}});")
    for i in range(NUM_MGRS):
        parts.append(f"for(let k in D) D[k][{i}]={{}};")

    parts.append("let m=D['alltime'];")
    parts.append(matrix_to_js(build_matrix(h2h_alltime)))
    for y in YEARS:
        parts.append(f"m=D['{y}'];")
        parts.append(matrix_to_js(build_matrix(h2h_by_year[y])))
    return '\n'.join(parts)


# ============================================================
# 5. Generate HTML
# ============================================================
print("\nGenerating HTML...")

# Team info for JS (manager name + latest team name for tooltip)
team_info = []
for i, mgr in enumerate(ALL_H2H_MANAGERS):
    latest = year_mgr_latest[YEARS[-1]].get(mgr, mgr)
    team_info.append({'id': i, 'mgr': mgr, 'team': js_safe(latest), 'color': COLORS[i % len(COLORS)]})
team_info_js = json.dumps(team_info)

# Per-year team names
year_names = {}
for y in YEARS:
    names = {}
    for i, mgr in enumerate(ALL_H2H_MANAGERS):
        names[i] = js_safe(year_mgr_latest[y].get(mgr, mgr))
    year_names[str(y)] = names
year_names_js = json.dumps(year_names)

# Rivalry data as JS
rivalries_js = json.dumps([{
    'a': r['name_a'], 'b': r['name_b'],
    'w': r['w'], 'l': r['l'], 't': r['t'],
    'total': r['total'], 'pct': round(r['win_pct'], 4),
    'imb': round(r['imbalance'], 4),
    'sf': r['sf'], 'sa': r['sa'], 'g': r['g'],
} for r in rivalries])

html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
  <title>Summertime Sadness - H2H Records</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', system-ui, sans-serif; }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 32px 24px; }}
    h1 {{ text-align: center; font-size: 2em; margin-bottom: 4px; background: linear-gradient(135deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
    h2 {{ font-size: 1.7em; margin-bottom: 6px; }}
    h3 {{ color: #22d3ee; font-size: 1.15em; margin: 40px 0 10px; }}
    .section-desc {{ color: #94a3b8; font-size: 0.92em; margin-bottom: 18px; line-height: 1.5; }}
    .subtitle {{ color: #94a3b8; font-size: 0.95em; margin-bottom: 24px; }}

    .filter-bar {{ display: flex; align-items: center; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }}
    .filter-bar label {{ color: #94a3b8; font-size: 0.9em; }}
    .filter-btn {{
      background: #1e293b; color: #94a3b8; border: 1px solid #334155;
      padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 0.88em;
      transition: all 0.15s;
    }}
    .filter-btn:hover {{ color: #e2e8f0; border-color: #475569; }}
    .filter-btn.active {{ background: #3b82f6; color: #fff; border-color: #3b82f6; }}

    .view-btn {{
      background: #1e293b; color: #94a3b8; border: 1px solid #334155;
      padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 0.88em;
      transition: all 0.15s;
    }}
    .view-btn:hover {{ color: #e2e8f0; border-color: #475569; }}
    .view-btn.active {{ background: #8b5cf6; color: #fff; border-color: #8b5cf6; }}

    .rivalry-view-btn {{
      background: #1e293b; color: #94a3b8; border: 1px solid #334155;
      padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 0.88em;
      transition: all 0.15s;
    }}
    .rivalry-view-btn:hover {{ color: #e2e8f0; border-color: #475569; }}
    .rivalry-view-btn.active {{ background: #8b5cf6; color: #fff; border-color: #8b5cf6; }}

    .mgr-link {{ color: inherit; text-decoration: none; }}
    .mgr-link:hover {{ text-decoration: underline; color: #38bdf8; }}

    .matrix-wrap {{ overflow-x: auto; margin-bottom: 40px; }}
    .matrix {{ border-collapse: collapse; font-size: 0.82em; white-space: nowrap; }}
    .matrix th, .matrix td {{ padding: 7px 10px; text-align: center; border: 1px solid #1e293b; }}
    .matrix th {{ background: #0f172a; color: #94a3b8; font-weight: 600; position: sticky; top: 0; }}
    .matrix th.team-col {{ text-align: left; min-width: 100px; }}
    .matrix td.team-cell {{ text-align: left; font-weight: 600; color: #e2e8f0; background: #0f172a; position: sticky; left: 0; z-index: 1; }}
    .matrix td.self {{ background: #1e293b; color: #334155; }}
    .matrix td.winning {{ background: #22c55e15; color: #4ade80; }}
    .matrix td.losing {{ background: #ef444415; color: #f87171; }}
    .matrix td.even {{ background: #eab30815; color: #fbbf24; }}
    .matrix td.none {{ color: #475569; }}
    .matrix .record-line {{ font-size: 0.95em; }}

    table {{ width: 100%; border-collapse: collapse; margin-bottom: 12px; }}
    th {{ background: #1e293b; color: #94a3b8; font-weight: 600; text-align: left;
         padding: 10px 14px; font-size: 0.82em; text-transform: uppercase; letter-spacing: 0.03em; }}
    td {{ padding: 10px 14px; border-bottom: 1px solid #1e293b; font-size: 0.92em; }}
    tr:hover {{ background: #1e293b55; }}
    .rank {{ color: #64748b; width: 40px; }}
    .score {{ color: #34d399; font-weight: 600; }}
    .hot-val {{ color: #4ade80; font-weight: 600; }}
    .close-margin {{ color: #fbbf24; font-weight: 600; }}

    .sortable {{ cursor: pointer; user-select: none; }}
    .sortable:hover {{ color: #e2e8f0; }}
    .sortable::after {{ content: ' \\2195'; font-size: 0.8em; }}
    .sortable.asc::after {{ content: ' \\2191'; }}
    .sortable.desc::after {{ content: ' \\2193'; }}

    .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 32px; margin-bottom: 32px; }}
    @media (max-width: 900px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

    .card {{ background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }}
    .card h3 {{ margin-top: 0; }}
  </style>
</head>
<body>
  <div id="nav"></div>
  <script src="nav.js"></script>

  <div class="container">
    <h1>Summertime Sadness Fantasy Baseball</h1>
    <h2>&#x1F91C;&#x1F91B; Manager vs Manager H2H Records</h2>
    <p class="subtitle">Head-to-head matchup records across all seasons (2023&ndash;2025) &bull; Identified by manager, not team name</p>

    <h3>All-Time H2H Matrix</h3>
    <p class="section-desc">Win-Loss-Tie record for each matchup. Rows = your record against the column opponent. Green = winning record, red = losing, yellow = even. Hover manager name for current team name.</p>

    <div class="filter-bar">
      <label>Filter by season:</label>
      <button class="filter-btn active" data-year="alltime">All-Time</button>
      <button class="filter-btn" data-year="2025">2025</button>
      <button class="filter-btn" data-year="2024">2024</button>
      <button class="filter-btn" data-year="2023">2023</button>
      <span style="color:#475569;font-size:0.82em;margin-left:16px">Click Manager or Total header to sort</span>
    </div>
    <div class="filter-bar">
      <label>View:</label>
      <button class="view-btn active" data-view="record">Record (W-L)</button>
      <button class="view-btn" data-view="cumulative">Cumulative Scores</button>
      <button class="view-btn" data-view="average">Average Scores</button>
    </div>

    <div class="matrix-wrap">
      <table class="matrix" id="h2hMatrix">
        <thead id="matrixHead"></thead>
        <tbody id="matrixBody"></tbody>
      </table>
    </div>

    <div class="filter-bar">
      <label>Rivalry view:</label>
      <button class="rivalry-view-btn active" data-rview="cumulative">Cumulative Scores</button>
      <button class="rivalry-view-btn" data-rview="record">Record (W-L)</button>
      <button class="rivalry-view-btn" data-rview="average">Average Scores</button>
    </div>

    <div class="two-col">
      <div class="card">
        <h3>&#x1F451; Most Lopsided Rivalries</h3>
        <p class="section-desc">Manager pairs with the most one-sided all-time records</p>
        <table>
          <thead id="lopsidedHead"></thead>
          <tbody id="lopsidedBody"></tbody>
        </table>
      </div>
      <div class="card">
        <h3>&#x2696;&#xFE0F; Most Even Rivalries</h3>
        <p class="section-desc">Closest all-time records (min 3 matchups)</p>
        <table>
          <thead id="evenHead"></thead>
          <tbody id="evenBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    const teams = {team_info_js};
    const yearNames = {year_names_js};

    {full_matrix_js()}

    let currentYear = 'alltime';
    let currentView = 'record';
    let matrixSort = 'alpha';  // 'alpha' | 'total_desc' | 'total_asc' | 'vs_ID_desc' | 'vs_ID_asc'

    const rivalries = {rivalries_js};
    let rivalryView = 'cumulative';

    function mgrLink(name) {{
      return `<a href="manager_profiles.html#${{encodeURIComponent(name)}}" class="mgr-link">${{name}}</a>`;
    }}

    function renderRivalries() {{
      const view = rivalryView;

      // Sort helpers
      function cumDiff(r) {{ return Math.abs(r.sf - r.sa); }}
      function avgDiff(r) {{ return r.g > 0 ? Math.abs(r.sf - r.sa) / r.g : 0; }}

      // Lopsided: biggest gap
      let lopsided;
      if (view === 'cumulative') {{
        lopsided = [...rivalries].sort((a, b) => cumDiff(b) - cumDiff(a) || b.total - a.total);
      }} else if (view === 'average') {{
        lopsided = [...rivalries].sort((a, b) => avgDiff(b) - avgDiff(a) || b.total - a.total);
      }} else {{
        lopsided = [...rivalries].sort((a, b) => b.imb - a.imb || b.total - a.total);
      }}

      // Even: smallest gap (min 3 matchups)
      const eligible = rivalries.filter(r => r.total >= 3);
      let even;
      if (view === 'cumulative') {{
        even = [...eligible].sort((a, b) => cumDiff(a) - cumDiff(b) || b.total - a.total);
      }} else if (view === 'average') {{
        even = [...eligible].sort((a, b) => avgDiff(a) - avgDiff(b) || b.total - a.total);
      }} else {{
        even = [...eligible].sort((a, b) => a.imb - b.imb || b.total - a.total);
      }}

      // Column headers
      let valHeader, metricHeader;
      if (view === 'cumulative') {{
        valHeader = 'Score'; metricHeader = 'Diff';
      }} else if (view === 'average') {{
        valHeader = 'Avg Score'; metricHeader = 'Avg Diff';
      }} else {{
        valHeader = 'Record'; metricHeader = 'Win%';
      }}

      const headHtml = `<tr><th>#</th><th>Dominant</th><th>Opponent</th><th>${{valHeader}}</th><th>${{metricHeader}}</th><th>Games</th></tr>`;
      document.getElementById('lopsidedHead').innerHTML = headHtml;
      document.getElementById('evenHead').innerHTML = headHtml.replace('Dominant', 'Manager A').replace('Opponent', 'Manager B');

      function renderRow(r, idx, isLopsided) {{
        let dom, opp, valStr, metricStr, metricCls;
        // Determine dominant side
        const aWins = r.sf > r.sa || (r.sf === r.sa && r.pct >= 0.5);
        if (aWins) {{
          dom = r.a; opp = r.b;
        }} else {{
          dom = r.b; opp = r.a;
        }}
        const sf = aWins ? r.sf : r.sa;
        const sa = aWins ? r.sa : r.sf;
        const w = aWins ? r.w : r.l;
        const l = aWins ? r.l : r.w;
        const pct = aWins ? r.pct : 1 - r.pct;

        if (view === 'cumulative') {{
          const sfStr = sf % 1 === 0 ? sf.toFixed(0) : sf.toFixed(1);
          const saStr = sa % 1 === 0 ? sa.toFixed(0) : sa.toFixed(1);
          valStr = `${{sfStr}}-${{saStr}}`;
          const diff = sf - sa;
          metricStr = (diff >= 0 ? '+' : '') + (diff % 1 === 0 ? diff.toFixed(0) : diff.toFixed(1));
          metricCls = isLopsided ? 'hot-val' : 'close-margin';
        }} else if (view === 'average') {{
          const avgF = r.g > 0 ? sf / r.g : 0;
          const avgA = r.g > 0 ? sa / r.g : 0;
          valStr = `${{avgF.toFixed(1)}}-${{avgA.toFixed(1)}}`;
          const avgDf = avgF - avgA;
          metricStr = (avgDf >= 0 ? '+' : '') + avgDf.toFixed(1);
          metricCls = isLopsided ? 'hot-val' : 'close-margin';
        }} else {{
          valStr = `${{w}}-${{l}}${{r.t > 0 ? '-' + r.t : ''}}`;
          metricStr = (pct * 100).toFixed(0) + '%';
          metricCls = isLopsided ? 'hot-val' : 'close-margin';
        }}

        if (!isLopsided) {{
          dom = r.a; opp = r.b;
        }}

        return `<tr><td class="rank">${{idx+1}}</td><td>${{mgrLink(dom)}}</td><td>${{mgrLink(opp)}}</td><td class="score">${{valStr}}</td><td class="${{metricCls}}">${{metricStr}}</td><td>${{r.total}}</td></tr>`;
      }}

      document.getElementById('lopsidedBody').innerHTML = lopsided.slice(0, 15).map((r, i) => renderRow(r, i, true)).join('');
      document.getElementById('evenBody').innerHTML = even.slice(0, 15).map((r, i) => renderRow(r, i, false)).join('');
    }}

    renderRivalries();

    document.querySelectorAll('.rivalry-view-btn').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.rivalry-view-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        rivalryView = btn.dataset.rview;
        renderRivalries();
      }});
    }});

    function renderMatrix(yearKey) {{
      currentYear = yearKey;
      const m = D[yearKey];
      const view = currentView;
      const head = document.getElementById('matrixHead');
      const body = document.getElementById('matrixBody');
      const names = yearKey !== 'alltime' && yearNames[yearKey] ? yearNames[yearKey] : null;

      // Pre-compute totals
      const teamData = teams.map(t => {{
        let tw = 0, tl = 0, tt = 0, tsf = 0, tsa = 0, tg = 0;
        for (const tB of teams) {{
          if (t.id === tB.id) continue;
          const cell = m[t.id][tB.id];
          if (cell.w !== '-' && !(cell.w === 0 && cell.l === 0 && cell.t === 0)) {{
            tw += cell.w; tl += cell.l; tt += cell.t;
            tsf += cell.sf; tsa += cell.sa; tg += cell.g;
          }}
        }}
        const total = tw + tl + tt;
        const pct = total > 0 ? (tw + tt * 0.5) / total : 0;
        return {{ ...t, tw, tl, tt, total, pct, tsf, tsa, tg }};
      }});

      // Filter out managers with 0 games for year-specific views
      const active = yearKey !== 'alltime' ? teamData.filter(t => t.total > 0) : teamData;

      // Columns always stay in alphabetical order
      const cols = [...active];

      // Sort by record vs specific opponent (rows only)
      function vsSort(tid, dir) {{
        return (a, b) => {{
          const ca = m[a.id][tid], cb = m[b.id][tid];
          const sa = a.id === tid ? -999 : (ca.w !== '-' ? ca.sf - ca.sa : -998);
          const sb = b.id === tid ? -999 : (cb.w !== '-' ? cb.sf - cb.sa : -998);
          return dir === 'asc' ? sa - sb : sb - sa;
        }};
      }}

      let rows_sorted;
      const vsMatch = matrixSort.match(/^vs_(\\d+)_(asc|desc)$/);
      if (vsMatch) {{
        rows_sorted = [...active].sort(vsSort(parseInt(vsMatch[1]), vsMatch[2]));
      }} else if (matrixSort === 'total_desc' || matrixSort === 'total_asc') {{
        const dir = matrixSort === 'total_asc' ? 1 : -1;
        if (view === 'cumulative') {{
          rows_sorted = [...active].sort((a, b) => dir * ((a.tsf - a.tsa) - (b.tsf - b.tsa)) || dir * (a.tsf - b.tsf));
        }} else if (view === 'average') {{
          const avgDiff = t => t.tg > 0 ? (t.tsf - t.tsa) / t.tg : 0;
          rows_sorted = [...active].sort((a, b) => dir * (avgDiff(a) - avgDiff(b)));
        }} else {{
          rows_sorted = [...active].sort((a, b) => dir * (a.pct - b.pct) || dir * (a.tw - b.tw));
        }}
      }} else {{
        rows_sorted = [...active];
      }}

      // Sort indicators
      function arrow(key) {{
        if (matrixSort === key || matrixSort === key + '_desc') return ' \\u2193';
        if (matrixSort === key + '_asc') return ' \\u2191';
        return '';
      }}

      let hdr = `<tr><th class="team-col" data-msort="alpha" style="cursor:pointer">Manager${{matrixSort === 'alpha' ? ' \\u2193' : ''}}</th>`;
      for (const t of cols) {{
        const teamName = names ? names[t.id] : t.team;
        const vsKey = 'vs_' + t.id;
        const a = arrow(vsKey);
        hdr += `<th title="${{teamName}}" data-msort="${{vsKey}}" style="cursor:pointer">${{t.mgr}}${{a}}</th>`;
      }}
      hdr += `<th data-msort="total" style="border-left:2px solid #3b82f6;cursor:pointer">Total${{arrow('total')}}</th></tr>`;
      head.innerHTML = hdr;

      let rows = '';
      for (const tA of rows_sorted) {{
        const teamName = names ? names[tA.id] : tA.team;
        rows += `<tr><td class="team-cell" title="${{teamName}}"><span style="color:${{tA.color}}">&#9679;</span> ${{tA.mgr}}</td>`;
        for (const tB of cols) {{
          const cell = m[tA.id][tB.id];
          if (tA.id === tB.id) {{
            rows += '<td class="self">&mdash;</td>';
          }} else if (cell.g === 0 && cell.w !== '-') {{
            rows += '<td class="none">&mdash;</td>';
          }} else {{
            let display;
            if (view === 'cumulative') {{
              const sfStr = cell.sf % 1 === 0 ? cell.sf.toFixed(0) : cell.sf.toFixed(1);
              const saStr = cell.sa % 1 === 0 ? cell.sa.toFixed(0) : cell.sa.toFixed(1);
              display = `${{sfStr}}-${{saStr}}`;
            }} else if (view === 'average') {{
              const avgF = cell.g > 0 ? (cell.sf / cell.g) : 0;
              const avgA = cell.g > 0 ? (cell.sa / cell.g) : 0;
              display = `${{avgF.toFixed(1)}}-${{avgA.toFixed(1)}}`;
            }} else {{
              display = `${{cell.w}}-${{cell.l}}`;
              if (cell.t > 0) display += `-${{cell.t}}`;
            }}
            rows += `<td class="${{cell.c}}"><span class="record-line">${{display}}</span></td>`;
          }}
        }}
        // Total column
        if (view === 'cumulative') {{
          const tsfStr = tA.tsf % 1 === 0 ? tA.tsf.toFixed(0) : tA.tsf.toFixed(1);
          const tsaStr = tA.tsa % 1 === 0 ? tA.tsa.toFixed(0) : tA.tsa.toFixed(1);
          rows += `<td style="border-left:2px solid #3b82f6;font-weight:600;color:#e2e8f0">${{tsfStr}}-${{tsaStr}}</td>`;
        }} else if (view === 'average') {{
          const avgF = tA.tg > 0 ? (tA.tsf / tA.tg) : 0;
          const avgA = tA.tg > 0 ? (tA.tsa / tA.tg) : 0;
          rows += `<td style="border-left:2px solid #3b82f6;font-weight:600;color:#e2e8f0">${{avgF.toFixed(1)}}-${{avgA.toFixed(1)}}</td>`;
        }} else {{
          rows += `<td style="border-left:2px solid #3b82f6;font-weight:600;color:#e2e8f0">${{tA.tw}}-${{tA.tl}}${{tA.tt > 0 ? '-' + tA.tt : ''}} <span style="color:#64748b;font-weight:400">(${{(tA.pct * 100).toFixed(0)}}%)</span></td>`;
        }}
        rows += '</tr>';
      }}
      body.innerHTML = rows;
    }}

    renderMatrix('alltime');

    document.querySelectorAll('.filter-btn').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        renderMatrix(btn.dataset.year);
      }});
    }});

    document.querySelectorAll('.view-btn').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.view-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentView = btn.dataset.view;
        renderMatrix(currentYear);
      }});
    }});

    document.getElementById('h2hMatrix').addEventListener('click', (e) => {{
      const th = e.target.closest('th[data-msort]');
      if (!th) return;
      const key = th.dataset.msort;
      if (key === 'alpha') {{
        matrixSort = 'alpha';
      }} else if (key === 'total') {{
        matrixSort = matrixSort === 'total_desc' ? 'total_asc' : 'total_desc';
      }} else {{
        // vs_ID - toggle desc/asc
        matrixSort = matrixSort === key + '_desc' ? key + '_asc' : key + '_desc';
      }}
      renderMatrix(currentYear);
    }});

    document.querySelectorAll('.sortable').forEach(th => {{
      th.addEventListener('click', () => {{
        const table = th.closest('table');
        const idx = Array.from(th.parentNode.children).indexOf(th);
        const tbody = table.querySelector('tbody') || table;
        const headerRow = th.parentNode;
        const rows = Array.from(tbody.querySelectorAll('tr')).filter(r => r !== headerRow);
        const type = th.dataset.type || 'str';
        const asc = th.classList.contains('asc');
        rows.sort((a, b) => {{
          let va = a.children[idx]?.textContent.trim() || '';
          let vb = b.children[idx]?.textContent.trim() || '';
          if (type === 'num') {{
            va = parseFloat(va.replace(/[^\\d.\\-]/g, '')) || 0;
            vb = parseFloat(vb.replace(/[^\\d.\\-]/g, '')) || 0;
          }}
          if (va < vb) return asc ? 1 : -1;
          if (va > vb) return asc ? -1 : 1;
          return 0;
        }});
        th.parentNode.querySelectorAll('.sortable').forEach(s => s.classList.remove('asc', 'desc'));
        th.classList.add(asc ? 'desc' : 'asc');
        rows.forEach(r => tbody.appendChild(r));
      }});
    }});
  </script>
</body>
</html>'''

out_path = r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\h2h_records.html'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f'\nGenerated docs/h2h_records.html')
