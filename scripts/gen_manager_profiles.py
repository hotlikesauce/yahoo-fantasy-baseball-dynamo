"""
Generate manager profiles page.

Queries AllTimeRankings (2007-2025) and HistoricalSeasons (2023-2025 H2H)
to build per-manager career profiles with season history, H2H records,
and championship trophies.

Usage:
  python scripts/gen_manager_profiles.py
"""

import boto3, json, sys, io
from collections import defaultdict
from decimal import Decimal
from boto3.dynamodb.conditions import Key

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from team_config import MANAGERS, YEAR_TN_TO_MANAGER

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
rankings_table = dynamodb.Table('FantasyBaseball-AllTimeRankings')
historical_table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

H2H_YEARS = [2023, 2024, 2025]
ALIASES = {'Jamie': 'James'}


def normalize(name):
    return ALIASES.get(name, name)


def query_data_type(year, data_type):
    """Query all items for a year + data_type from HistoricalSeasons."""
    items = []
    resp = historical_table.query(
        IndexName='YearDataTypeIndex',
        KeyConditionExpression=Key('YearDataType').eq(f'{year}#{data_type}')
    )
    items.extend(resp['Items'])
    while 'LastEvaluatedKey' in resp:
        resp = historical_table.query(
            IndexName='YearDataTypeIndex',
            KeyConditionExpression=Key('YearDataType').eq(f'{year}#{data_type}'),
            ExclusiveStartKey=resp['LastEvaluatedKey']
        )
        items.extend(resp['Items'])
    return items


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# ============================================================
# 1. Scan AllTimeRankings
# ============================================================
print("Scanning AllTimeRankings...")
all_items = []
resp = rankings_table.scan()
all_items.extend(resp['Items'])
while 'LastEvaluatedKey' in resp:
    resp = rankings_table.scan(ExclusiveStartKey=resp['LastEvaluatedKey'])
    all_items.extend(resp['Items'])
print(f"  {len(all_items)} items")

# Group by year, compute ranks
by_year = defaultdict(list)
for item in all_items:
    year = str(item['Year'])
    team = str(item.get('Team', '?')).replace('\U0001f3c6', '').strip()
    manager = normalize(str(item.get('Manager', '?')))
    score = float(item['Score_Sum']) if 'Score_Sum' in item else None
    is_champ = 'Champion' in item
    by_year[year].append({
        'year': year,
        'team': team,
        'manager': manager,
        'score': score,
        'champ': is_champ,
    })

# Compute finish ranks per year (by Score_Sum descending)
for year, entries in by_year.items():
    scored = sorted([e for e in entries if e['score'] is not None],
                    key=lambda x: x['score'], reverse=True)
    for rank, entry in enumerate(scored, 1):
        entry['rank'] = rank
        entry['of'] = len(scored)
    for entry in entries:
        if 'rank' not in entry:
            entry['rank'] = None
            entry['of'] = len(entries)

# Build per-manager season lists
all_mgr_names = set()
manager_seasons = defaultdict(list)
manager_titles = defaultdict(list)

for year in sorted(by_year.keys()):
    for entry in by_year[year]:
        mgr = entry['manager']
        all_mgr_names.add(mgr)
        manager_seasons[mgr].append(entry)
        if entry['champ']:
            manager_titles[mgr].append({'year': entry['year'], 'team': entry['team']})

current_set = set(MANAGERS)
historical_set = all_mgr_names - current_set - {'?'}
historical_managers = sorted(historical_set)

print(f"  Current managers: {len(current_set)}")
print(f"  Historical managers: {len(historical_set)} - {historical_managers}")

# ============================================================
# 2. H2H Records from HistoricalSeasons (2023-2025)
# ============================================================
print("\nBuilding H2H records...")

year_name_to_mgr = {y: {} for y in H2H_YEARS}
cached_wr = {}  # year -> weekly_results items (avoid double-query)

for year in H2H_YEARS:
    # power_ranks for stable name mapping
    items = query_data_type(year, 'power_ranks_season_trend')
    for item in items:
        tn = str(item.get('TeamNumber', ''))
        name = item.get('Team', '')
        if not tn or not name:
            continue
        mgr = YEAR_TN_TO_MANAGER.get((year, tn))
        if mgr:
            year_name_to_mgr[year][name] = mgr

    # weekly_results (cache for reuse in H2H aggregation)
    wr_items = query_data_type(year, 'weekly_results')
    cached_wr[year] = wr_items

    for item in wr_items:
        tn = str(item.get('TeamNumber', ''))
        name = item.get('Team', '')
        if tn and name and name not in year_name_to_mgr[year]:
            mgr = YEAR_TN_TO_MANAGER.get((year, tn))
            if mgr:
                year_name_to_mgr[year][name] = mgr

    # Cross-reference opponent names
    week_items = defaultdict(list)
    for item in wr_items:
        week_items[int(item.get('Week', 0))].append(item)
    for week, items_in_week in week_items.items():
        week_tn_name = {}
        for item in items_in_week:
            tn = str(item.get('TeamNumber', ''))
            name = item.get('Team', '')
            if tn and name:
                week_tn_name[tn] = name
        for item in items_in_week:
            opp = item.get('Opponent', '')
            if opp and opp not in year_name_to_mgr[year]:
                for other_tn, other_name in week_tn_name.items():
                    if other_name == opp:
                        mgr = YEAR_TN_TO_MANAGER.get((year, other_tn))
                        if mgr:
                            year_name_to_mgr[year][opp] = mgr
                            break

    print(f"  {year}: {len(year_name_to_mgr[year])} names mapped")

# Aggregate H2H records
h2h_alltime = defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 't': 0}))
h2h_by_year = {y: defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 't': 0})) for y in H2H_YEARS}

for year in H2H_YEARS:
    for item in cached_wr[year]:
        tn = str(item.get('TeamNumber', ''))
        opp_name = item.get('Opponent', '')
        score = float(item.get('Score', 0))
        opp_score = float(item.get('Opponent_Score', 0))

        mgr_a = YEAR_TN_TO_MANAGER.get((year, tn))
        mgr_b = year_name_to_mgr[year].get(opp_name)

        if not mgr_a or not mgr_b or mgr_a == mgr_b:
            continue

        if score > opp_score:
            h2h_alltime[mgr_a][mgr_b]['w'] += 1
            h2h_by_year[year][mgr_a][mgr_b]['w'] += 1
        elif score < opp_score:
            h2h_alltime[mgr_a][mgr_b]['l'] += 1
            h2h_by_year[year][mgr_a][mgr_b]['l'] += 1
        else:
            h2h_alltime[mgr_a][mgr_b]['t'] += 1
            h2h_by_year[year][mgr_a][mgr_b]['t'] += 1

# ============================================================
# 3. Build profiles JSON
# ============================================================
print("\nBuilding profiles...")

profiles = {}
for mgr in sorted(all_mgr_names - {'?'}):
    seasons = sorted(manager_seasons[mgr], key=lambda s: s['year'], reverse=True)
    titles = manager_titles[mgr]

    clean_seasons = []
    for s in seasons:
        clean_seasons.append({
            'year': s['year'],
            'team': s['team'],
            'score': round(s['score'], 1) if s['score'] is not None else None,
            'rank': s['rank'],
            'of': s['of'],
            'champ': s['champ'],
        })

    # H2H records (only for managers who have H2H data)
    h2h = {}
    if mgr in h2h_alltime:
        h2h['alltime'] = {}
        for opp in h2h_alltime[mgr]:
            rec = h2h_alltime[mgr][opp]
            if rec['w'] + rec['l'] + rec['t'] > 0:
                h2h['alltime'][opp] = dict(rec)

        for year in H2H_YEARS:
            yr_key = str(year)
            if mgr in h2h_by_year[year]:
                yr_data = {}
                for opp in h2h_by_year[year][mgr]:
                    rec = h2h_by_year[year][mgr][opp]
                    if rec['w'] + rec['l'] + rec['t'] > 0:
                        yr_data[opp] = dict(rec)
                if yr_data:
                    h2h[yr_key] = yr_data

    profiles[mgr] = {
        'seasons': clean_seasons,
        'titles': titles,
        'isCurrent': mgr in current_set,
        'h2h': h2h,
    }

data = {
    'currentManagers': MANAGERS,
    'historicalManagers': historical_managers,
    'profiles': profiles,
}

data_json = json.dumps(data, cls=DecimalEncoder, ensure_ascii=False)
print(f"  {len(profiles)} manager profiles")

# ============================================================
# 4. Generate HTML
# ============================================================
print("Generating HTML...")

html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Summertime Sadness - Manager Profiles</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', system-ui, sans-serif; }}
    .container {{ max-width: 1400px; margin: 0 auto; padding: 32px 24px; }}
    h2 {{ font-size: 1.7em; margin-bottom: 6px; }}
    .subtitle {{ color: #94a3b8; font-size: 0.95em; margin-bottom: 24px; }}
    h3 {{ color: #22d3ee; font-size: 1.15em; margin: 32px 0 10px; }}

    .mgr-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(155px, 1fr));
      gap: 12px;
      margin-bottom: 20px;
    }}
    .mgr-card {{
      background: #1e293b;
      border: 2px solid #334155;
      border-radius: 12px;
      padding: 16px 14px;
      cursor: pointer;
      text-align: center;
      transition: all 0.2s;
    }}
    .mgr-card:hover {{ border-color: #475569; transform: translateY(-2px); }}
    .mgr-card.active {{ border-color: #3b82f6; box-shadow: 0 0 20px rgba(59,130,246,0.15); }}
    .mgr-card .mgr-name {{ font-size: 1.1em; font-weight: 700; margin-bottom: 4px; }}
    .mgr-card .mgr-titles {{ color: #fbbf24; font-size: 0.9em; min-height: 20px; }}
    .mgr-card .mgr-sub {{ color: #64748b; font-size: 0.78em; margin-top: 4px; }}

    .hist-toggle {{
      color: #64748b; font-size: 0.85em; cursor: pointer;
      margin-bottom: 16px; display: inline-block;
    }}
    .hist-toggle:hover {{ color: #94a3b8; }}

    #profile {{ display: none; }}
    #profile.visible {{ display: block; }}

    .profile-header {{
      display: flex; align-items: center; gap: 20px;
      margin-bottom: 24px; flex-wrap: wrap;
    }}
    .profile-name {{
      font-size: 2.2em; font-weight: 800;
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }}
    .trophy-row {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .trophy {{
      display: inline-flex; flex-direction: column; align-items: center;
      background: linear-gradient(135deg, #fbbf24, #f59e0b);
      color: #0f172a; font-weight: 800; font-size: 0.75em;
      padding: 6px 12px; border-radius: 8px;
      box-shadow: 0 0 12px rgba(251,191,36,0.3);
    }}

    .stat-cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 14px; margin-bottom: 28px;
    }}
    .stat-card {{
      background: #1e293b; border-radius: 10px; padding: 18px;
      border: 1px solid #334155; text-align: center;
    }}
    .stat-card .label {{ color: #64748b; font-size: 0.75em; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }}
    .stat-card .value {{ font-size: 1.6em; font-weight: 700; }}
    .stat-card .sub {{ color: #94a3b8; font-size: 0.82em; margin-top: 3px; }}
    .gold {{ color: #fbbf24; }}
    .blue {{ color: #3b82f6; }}
    .green {{ color: #22c55e; }}
    .purple {{ color: #a78bfa; }}
    .cyan {{ color: #22d3ee; }}
    .red {{ color: #f87171; }}

    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; margin-bottom: 12px; }}
    th {{ background: #1e293b; color: #94a3b8; font-weight: 600; text-align: left;
         padding: 10px 14px; font-size: 0.82em; text-transform: uppercase; letter-spacing: 0.03em; }}
    td {{ padding: 10px 14px; border-bottom: 1px solid #1e293b; font-size: 0.92em; }}
    tr:hover {{ background: #1e293b55; }}
    .champ-marker {{ color: #fbbf24; }}

    .sortable {{ cursor: pointer; user-select: none; }}
    .sortable:hover {{ color: #e2e8f0; }}
    .sortable::after {{ content: ' \\2195'; font-size: 0.8em; }}
    .sortable.asc::after {{ content: ' \\2191'; }}
    .sortable.desc::after {{ content: ' \\2193'; }}

    .winning {{ color: #4ade80; }}
    .losing {{ color: #f87171; }}
    .even-rec {{ color: #fbbf24; }}

    .section {{
      background: #1e293b; border-radius: 12px; padding: 20px 24px;
      border: 1px solid #334155; margin-bottom: 24px;
    }}
    .section h3 {{ margin-top: 0; }}

    .h2h-year-filter {{ display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }}
    .h2h-yr-btn {{
      background: #0f172a; color: #94a3b8; border: 1px solid #334155;
      padding: 4px 14px; border-radius: 6px; cursor: pointer; font-size: 0.82em;
    }}
    .h2h-yr-btn:hover {{ color: #e2e8f0; border-color: #475569; }}
    .h2h-yr-btn.active {{ background: #3b82f6; color: #fff; border-color: #3b82f6; }}

    @media (max-width: 768px) {{
      .profile-header {{ flex-direction: column; align-items: flex-start; }}
      .stat-cards {{ grid-template-columns: repeat(3, 1fr); }}
    }}
    @media (max-width: 640px) {{
      .container {{ padding: 16px 10px; }}
      h2 {{ font-size: 1.3em; }}
      .mgr-grid {{ grid-template-columns: repeat(3, 1fr); gap: 8px; }}
      .mgr-card {{ padding: 10px 8px; }}
      .mgr-card .mgr-name {{ font-size: 0.9em; }}
      .profile-name {{ font-size: 1.6em; }}
      .stat-cards {{ grid-template-columns: repeat(2, 1fr); gap: 8px; }}
      .stat-card {{ padding: 12px; }}
      .stat-card .value {{ font-size: 1.3em; }}
      .section {{ padding: 14px 10px; }}
      th {{ padding: 8px 8px; font-size: 0.75em; }}
      td {{ padding: 8px 8px; font-size: 0.82em; }}
    }}
  </style>
</head>
<body>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <div id="nav"></div>
  <script src="nav.js"></script>

  <div class="container">
    <h2>Manager Profiles</h2>
    <p class="subtitle">Career stats, season history, and head-to-head records for every manager in league history</p>

    <h3 style="margin-top:16px">Select a Manager</h3>
    <div class="mgr-grid" id="mgrGrid"></div>
    <div class="hist-toggle" id="histToggle" style="display:none"></div>
    <div class="mgr-grid" id="histGrid" style="display:none"></div>

    <div id="profile">
      <div class="profile-header">
        <span class="profile-name" id="profileName"></span>
        <div class="trophy-row" id="trophyRow"></div>
      </div>

      <div class="stat-cards" id="statCards"></div>

      <div class="section">
        <h3>Power Score Over Time</h3>
        <p style="color:#94a3b8;font-size:0.88em;margin-bottom:14px">End-of-season power score by year (higher is better, max 1200)</p>
        <div style="height:300px;position:relative">
          <canvas id="scoreChart"></canvas>
        </div>
      </div>

      <div class="section">
        <h3>Season History</h3>
        <div class="table-wrap">
          <table id="seasonTable">
            <thead>
              <tr>
                <th class="sortable" data-col="year" data-type="str">Year</th>
                <th>Team Name</th>
                <th class="sortable" data-col="score" data-type="num">Power Score</th>
                <th class="sortable" data-col="rank" data-type="num">Finish</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="seasonBody"></tbody>
          </table>
        </div>
      </div>

      <div class="section" id="h2hSection">
        <h3>Head-to-Head Records</h3>
        <p style="color:#94a3b8;font-size:0.88em;margin-bottom:14px">Record against each opponent (2023&ndash;2025 H2H data available)</p>
        <div class="h2h-year-filter" id="h2hFilter"></div>
        <div class="table-wrap">
          <table id="h2hTable">
            <thead>
              <tr>
                <th>Opponent</th>
                <th class="sortable" data-col="record" data-type="str">Record</th>
                <th class="sortable" data-col="winpct" data-type="num">Win %</th>
                <th class="sortable" data-col="wins" data-type="num">Wins</th>
                <th class="sortable" data-col="losses" data-type="num">Losses</th>
                <th class="sortable" data-col="games" data-type="num">Games</th>
              </tr>
            </thead>
            <tbody id="h2hBody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    const DATA = {data_json};

    let selectedManager = null;
    let h2hYear = 'alltime';

    function buildGrid() {{
      const grid = document.getElementById('mgrGrid');
      grid.innerHTML = '';
      for (const mgr of DATA.currentManagers) {{
        const p = DATA.profiles[mgr];
        const titles = p.titles.length;
        const seasons = p.seasons.length;
        const card = document.createElement('div');
        card.className = 'mgr-card';
        card.dataset.mgr = mgr;
        card.innerHTML = `
          <div class="mgr-name">${{mgr}}</div>
          <div class="mgr-titles">${{titles > 0 ? '\U0001f3c6'.repeat(titles) : '&mdash;'}}</div>
          <div class="mgr-sub">${{seasons}} season${{seasons !== 1 ? 's' : ''}}</div>
        `;
        card.addEventListener('click', () => selectManager(mgr));
        grid.appendChild(card);
      }}

      if (DATA.historicalManagers.length > 0) {{
        const toggle = document.getElementById('histToggle');
        toggle.style.display = 'inline-block';
        toggle.textContent = '\\u25B6 Show ' + DATA.historicalManagers.length + ' historical managers';
        let open = false;
        toggle.addEventListener('click', () => {{
          open = !open;
          const hg = document.getElementById('histGrid');
          hg.style.display = open ? 'grid' : 'none';
          toggle.textContent = open
            ? '\\u25BC Hide historical managers'
            : '\\u25B6 Show ' + DATA.historicalManagers.length + ' historical managers';
        }});

        const histGrid = document.getElementById('histGrid');
        for (const mgr of DATA.historicalManagers) {{
          const p = DATA.profiles[mgr];
          if (!p) continue;
          const titles = p.titles.length;
          const seasons = p.seasons.length;
          const card = document.createElement('div');
          card.className = 'mgr-card';
          card.dataset.mgr = mgr;
          card.innerHTML = `
            <div class="mgr-name">${{mgr}}</div>
            <div class="mgr-titles">${{titles > 0 ? '\U0001f3c6'.repeat(titles) : '&mdash;'}}</div>
            <div class="mgr-sub">${{seasons}} season${{seasons !== 1 ? 's' : ''}}</div>
          `;
          card.addEventListener('click', () => selectManager(mgr));
          histGrid.appendChild(card);
        }}
      }}
    }}

    function selectManager(mgr) {{
      selectedManager = mgr;
      h2hYear = 'alltime';

      document.querySelectorAll('.mgr-card').forEach(c => c.classList.remove('active'));
      document.querySelectorAll('.mgr-card[data-mgr="' + mgr + '"]').forEach(c => c.classList.add('active'));

      renderProfile();
      document.getElementById('profile').classList.add('visible');
      document.getElementById('profile').scrollIntoView({{ behavior: 'smooth', block: 'start' }});
    }}

    function renderProfile() {{
      const mgr = selectedManager;
      const p = DATA.profiles[mgr];
      if (!p) return;

      document.getElementById('profileName').textContent = mgr;

      // Trophies
      const tr = document.getElementById('trophyRow');
      tr.innerHTML = p.titles.map(t => '<div class="trophy">\U0001f3c6 ' + t.year + '</div>').join('');

      // Compute stats
      const scored = p.seasons.filter(s => s.score !== null);
      const avgScore = scored.length > 0 ? scored.reduce((a, s) => a + s.score, 0) / scored.length : null;
      const avgRank = scored.length > 0 ? scored.reduce((a, s) => a + s.rank, 0) / scored.length : null;
      const bestSeason = scored.length > 0 ? scored.reduce((a, s) => s.score > a.score ? s : a) : null;

      // H2H overall
      let totalW = 0, totalL = 0, totalT = 0;
      const h2hAll = p.h2h.alltime || {{}};
      for (const opp in h2hAll) {{
        totalW += h2hAll[opp].w;
        totalL += h2hAll[opp].l;
        totalT += h2hAll[opp].t;
      }}
      const totalGames = totalW + totalL + totalT;
      const h2hPct = totalGames > 0 ? ((totalW + totalT * 0.5) / totalGames * 100).toFixed(0) : null;

      const cards = document.getElementById('statCards');
      cards.innerHTML =
        '<div class="stat-card"><div class="label">Seasons</div><div class="value blue">' + p.seasons.length + '</div><div class="sub">' + scored.length + ' scored</div></div>' +
        '<div class="stat-card"><div class="label">Titles</div><div class="value gold">' + p.titles.length + '</div><div class="sub">' + (p.titles.map(t => t.year).join(', ') || 'None yet') + '</div></div>' +
        '<div class="stat-card"><div class="label">Avg Power Score</div><div class="value green">' + (avgScore !== null ? avgScore.toFixed(0) : 'N/A') + '</div><div class="sub">' + scored.length + ' seasons</div></div>' +
        '<div class="stat-card"><div class="label">Avg Finish</div><div class="value purple">' + (avgRank !== null ? avgRank.toFixed(1) : 'N/A') + '</div><div class="sub">Lower is better</div></div>' +
        '<div class="stat-card"><div class="label">Best Season</div><div class="value cyan">' + (bestSeason ? bestSeason.score.toFixed(0) : 'N/A') + '</div><div class="sub">' + (bestSeason ? bestSeason.year + ' (#' + bestSeason.rank + ')' : '') + '</div></div>' +
        '<div class="stat-card"><div class="label">H2H Win %</div><div class="value ' + (h2hPct !== null && h2hPct >= 50 ? 'green' : 'red') + '">' + (h2hPct !== null ? h2hPct + '%' : 'N/A') + '</div><div class="sub">' + totalW + '-' + totalL + (totalT > 0 ? '-' + totalT : '') + ' (' + totalGames + ' games)</div></div>';

      renderScoreChart();
      renderSeasonTable();

      const h2hSection = document.getElementById('h2hSection');
      if (Object.keys(p.h2h).length === 0) {{
        h2hSection.style.display = 'none';
      }} else {{
        h2hSection.style.display = 'block';
        renderH2HFilter();
        renderH2HTable();
      }}
    }}

    let scoreChartInstance = null;
    function renderScoreChart() {{
      const p = DATA.profiles[selectedManager];
      const scored = p.seasons.filter(s => s.score !== null).slice().sort((a, b) => a.year.localeCompare(b.year));
      if (scored.length < 2) {{
        document.getElementById('scoreChart').parentElement.parentElement.style.display = 'none';
        return;
      }}
      document.getElementById('scoreChart').parentElement.parentElement.style.display = 'block';

      const labels = scored.map(s => s.year);
      const values = scored.map(s => s.score);
      const champYears = new Set(p.titles.map(t => t.year));
      const pointColors = scored.map(s => champYears.has(s.year) ? '#fbbf24' : '#3b82f6');
      const pointRadii = scored.map(s => champYears.has(s.year) ? 7 : 4);

      if (scoreChartInstance) scoreChartInstance.destroy();

      const ctx = document.getElementById('scoreChart').getContext('2d');
      scoreChartInstance = new Chart(ctx, {{
        type: 'line',
        data: {{
          labels: labels,
          datasets: [{{
            label: 'Power Score',
            data: values,
            borderColor: '#3b82f6',
            backgroundColor: '#3b82f622',
            borderWidth: 2.5,
            pointBackgroundColor: pointColors,
            pointBorderColor: pointColors,
            pointRadius: pointRadii,
            pointHoverRadius: 8,
            tension: 0.3,
            fill: true,
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{
              callbacks: {{
                label: function(ctx) {{
                  const s = scored[ctx.dataIndex];
                  let label = 'Score: ' + s.score.toFixed(0) + ' (#' + s.rank + '/' + s.of + ')';
                  if (champYears.has(s.year)) label += ' \U0001f3c6';
                  return label;
                }}
              }}
            }}
          }},
          scales: {{
            x: {{
              ticks: {{ color: '#94a3b8', font: {{ size: 11 }} }},
              grid: {{ color: '#1e293b' }}
            }},
            y: {{
              min: 0,
              max: 1200,
              ticks: {{ color: '#94a3b8', font: {{ size: 11 }}, stepSize: 200 }},
              grid: {{ color: '#1e293b' }}
            }}
          }}
        }}
      }});
    }}

    function renderSeasonTable() {{
      const p = DATA.profiles[selectedManager];
      const body = document.getElementById('seasonBody');
      body.innerHTML = p.seasons.map(s =>
        '<tr>' +
        '<td>' + s.year + '</td>' +
        '<td>' + s.team + '</td>' +
        '<td>' + (s.score !== null ? s.score.toFixed(1) : '<span style="color:#475569">N/A</span>') + '</td>' +
        '<td>' + (s.rank !== null ? s.rank + ' / ' + s.of : '<span style="color:#475569">N/A</span>') + '</td>' +
        '<td>' + (s.champ ? '<span class="champ-marker">\U0001f3c6 Champion</span>' : '') + '</td>' +
        '</tr>'
      ).join('');
    }}

    function renderH2HFilter() {{
      const p = DATA.profiles[selectedManager];
      const filter = document.getElementById('h2hFilter');
      const years = ['alltime'].concat(Object.keys(p.h2h).filter(k => k !== 'alltime').sort().reverse());
      filter.innerHTML = years.map(y => {{
        const label = y === 'alltime' ? 'All-Time' : y;
        const cls = y === h2hYear ? 'h2h-yr-btn active' : 'h2h-yr-btn';
        return '<button class="' + cls + '" data-yr="' + y + '">' + label + '</button>';
      }}).join('');

      filter.querySelectorAll('.h2h-yr-btn').forEach(btn => {{
        btn.addEventListener('click', () => {{
          h2hYear = btn.dataset.yr;
          filter.querySelectorAll('.h2h-yr-btn').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          renderH2HTable();
        }});
      }});
    }}

    function renderH2HTable() {{
      const p = DATA.profiles[selectedManager];
      const records = p.h2h[h2hYear] || {{}};
      const body = document.getElementById('h2hBody');

      const rows = [];
      for (const opp of DATA.currentManagers) {{
        if (opp === selectedManager) continue;
        const rec = records[opp] || {{ w: 0, l: 0, t: 0 }};
        const total = rec.w + rec.l + rec.t;
        const pct = total > 0 ? (rec.w + rec.t * 0.5) / total : 0;
        const cls = total === 0 ? '' : (rec.w > rec.l ? 'winning' : (rec.w < rec.l ? 'losing' : 'even-rec'));
        rows.push({{ opp: opp, w: rec.w, l: rec.l, t: rec.t, total: total, pct: pct, cls: cls }});
      }}

      body.innerHTML = rows.map(r => {{
        let record = r.w + '-' + r.l;
        if (r.t > 0) record += '-' + r.t;
        return '<tr>' +
          '<td>' + r.opp + '</td>' +
          '<td class="' + r.cls + '" style="font-weight:600">' + (r.total > 0 ? record : '<span style="color:#475569">0-0</span>') + '</td>' +
          '<td>' + (r.total > 0 ? (r.pct * 100).toFixed(0) + '%' : '<span style="color:#475569">&mdash;</span>') + '</td>' +
          '<td>' + r.w + '</td>' +
          '<td>' + r.l + '</td>' +
          '<td>' + r.total + '</td>' +
          '</tr>';
      }}).join('');
    }}

    // Sortable table headers
    document.querySelectorAll('.sortable').forEach(th => {{
      th.addEventListener('click', () => {{
        const table = th.closest('table');
        const idx = Array.from(th.parentNode.children).indexOf(th);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const type = th.dataset.type || 'str';
        const asc = th.classList.contains('asc');
        rows.sort((a, b) => {{
          let va = a.children[idx] ? a.children[idx].textContent.trim() : '';
          let vb = b.children[idx] ? b.children[idx].textContent.trim() : '';
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

    buildGrid();

    // Auto-select manager from URL hash (e.g. #Greg)
    if (window.location.hash) {{
      const hashMgr = decodeURIComponent(window.location.hash.slice(1));
      if (DATA.profiles[hashMgr]) {{
        selectManager(hashMgr);
        document.getElementById('profileSection').scrollIntoView({{ behavior: 'smooth' }});
      }}
    }}
  </script>
</body>
</html>'''

out_path = r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\manager_profiles.html'
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f'\nGenerated {out_path}')
