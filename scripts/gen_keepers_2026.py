import json, sys, io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 2026 Keeper data pulled from Yahoo Fantasy API (preseason ranks as of Feb 2026)
KEEPERS = [
    {'team': '¯\\_(ツ)_/¯',              'keeper1': {'name': 'Shohei Ohtani (Batter)', 'rank': 2,  'pos': 'Util', 'mlb': 'LAD'},
                                           'keeper2': {'name': 'Juan Soto',              'rank': 4,  'pos': 'OF',   'mlb': 'NYM'}},
    {'team': 'SQUEEZE AGS',              'keeper1': {'name': 'Tarik Skubal',            'rank': 5,  'pos': 'SP',   'mlb': 'DET'},
                                           'keeper2': {'name': 'Garrett Crochet',        'rank': 11, 'pos': 'SP',   'mlb': 'BOS'}},
    {'team': 'Ian Cumsler',              'keeper1': {'name': 'Kyle Tucker',             'rank': 9,  'pos': 'OF',   'mlb': 'LAD'},
                                           'keeper2': {'name': 'Ronald Acuña Jr.',       'rank': 12, 'pos': 'OF',   'mlb': 'ATL'}},
    {'team': 'Serafini Hit Squad',       'keeper1': {'name': 'José Ramírez',            'rank': 7,  'pos': '3B',   'mlb': 'CLE'},
                                           'keeper2': {'name': 'Fernando Tatis Jr.',     'rank': 15, 'pos': 'OF',   'mlb': 'SD'}},
    {'team': 'WEMBY SZN',                'keeper1': {'name': 'Bobby Witt Jr.',          'rank': 3,  'pos': 'SS',   'mlb': 'KC'},
                                           'keeper2': {'name': 'Corbin Carroll',         'rank': 20, 'pos': 'OF',   'mlb': 'AZ'}},
    {'team': 'The Rosterbation Station', 'keeper1': {'name': 'Julio Rodríguez',         'rank': 10, 'pos': 'OF',   'mlb': 'SEA'},
                                           'keeper2': {'name': 'Junior Caminero',        'rank': 14, 'pos': '3B',   'mlb': 'TB'}},
    {'team': 'Hatfield Hurlers',         'keeper1': {'name': 'Aaron Judge',             'rank': 1,  'pos': 'OF',   'mlb': 'NYY'},
                                           'keeper2': {'name': 'Trea Turner',            'rank': 24, 'pos': 'SS',   'mlb': 'PHI'}},
    {'team': 'Floppy Salami Time',       'keeper1': {'name': 'Elly De La Cruz',         'rank': 6,  'pos': 'SS',   'mlb': 'CIN'},
                                           'keeper2': {'name': 'Nick Kurtz',             'rank': 21, 'pos': '1B',   'mlb': 'ATH'}},
    {'team': 'OG9\ufe0f\u20e3',          'keeper1': {'name': 'Kyle Schwarber',          'rank': 19, 'pos': 'OF',   'mlb': 'PHI'},
                                           'keeper2': {'name': 'Jazz Chisholm Jr.',      'rank': 23, 'pos': '2B/3B','mlb': 'NYY'}},
    {'team': 'Moniebol \U0001f433',      'keeper1': {'name': 'Jackson Chourio',         'rank': 16, 'pos': 'OF',   'mlb': 'MIL'},
                                           'keeper2': {'name': 'Ketel Marte',            'rank': 28, 'pos': '2B',   'mlb': 'AZ'}},
    {'team': 'Getting Plowed Again.',    'keeper1': {'name': 'Francisco Lindor',        'rank': 26, 'pos': 'SS',   'mlb': 'NYM'},
                                           'keeper2': {'name': 'James Wood',             'rank': 38, 'pos': 'OF',   'mlb': 'WSH'}},
    {'team': 'Rickie Flower',            'keeper1': {'name': 'Yordan Alvarez',          'rank': 37, 'pos': 'OF',   'mlb': 'HOU'},
                                           'keeper2': {'name': 'Shohei Ohtani (Pitcher)','rank': 117,'pos': 'SP',   'mlb': 'LAD'}},
]

# Sort by combined rank (already sorted above, verify)
for k in KEEPERS:
    k['combined'] = k['keeper1']['rank'] + k['keeper2']['rank']
KEEPERS.sort(key=lambda x: x['combined'])

# Colors
COLORS = [
    '#ef4444','#f97316','#eab308','#22c55e','#14b8a6','#3b82f6',
    '#8b5cf6','#ec4899','#06b6d4','#f59e0b','#10b981','#a855f7',
]

def pos_badge(pos):
    pos_colors = {
        'SP': '#3b82f6', 'RP': '#06b6d4', 'C': '#f59e0b',
        '1B': '#22c55e', '2B': '#22c55e', '3B': '#22c55e', 'SS': '#22c55e',
        'OF': '#8b5cf6', 'Util': '#ec4899', '2B/3B': '#22c55e',
    }
    color = pos_colors.get(pos, '#64748b')
    return f'<span class="pos-badge" style="background:{color}22;color:{color};border:1px solid {color}44">{pos}</span>'

# Team cards sorted by combined rank
cards_html = ''
for i, team in enumerate(KEEPERS):
    color = COLORS[i % len(COLORS)]
    rank_label = f'#{i+1}'
    k1 = team['keeper1']
    k2 = team['keeper2']
    medal = ['🥇','🥈','🥉','','','','','','','','',''][i]
    cards_html += f'''
    <div class="keeper-card">
      <div class="card-rank" style="color:{color}">{rank_label}</div>
      <div class="card-team" style="border-left:3px solid {color}">{team["team"]}</div>
      <div class="card-players">
        <div class="player-row">
          <span class="player-rank">#{k1["rank"]}</span>
          <span class="player-name">{k1["name"]}</span>
          <span class="player-meta">{pos_badge(k1["pos"])} <span class="player-mlb">{k1["mlb"]}</span></span>
        </div>
        <div class="player-row">
          <span class="player-rank">#{k2["rank"]}</span>
          <span class="player-name">{k2["name"]}</span>
          <span class="player-meta">{pos_badge(k2["pos"])} <span class="player-mlb">{k2["mlb"]}</span></span>
        </div>
      </div>
      <div class="card-combined">Combined rank: <strong style="color:{color}">#{team["combined"]}</strong></div>
    </div>'''

# Table rows
table_rows = ''
for i, team in enumerate(KEEPERS):
    color = COLORS[i % len(COLORS)]
    k1 = team['keeper1']
    k2 = team['keeper2']
    table_rows += f'''<tr>
      <td class="rank-cell">{i+1}</td>
      <td class="team-cell"><span style="color:{color}">&#9679;</span> {team["team"]}</td>
      <td>
        <span class="player-rank-sm">#{k1["rank"]}</span>
        {k1["name"]} {pos_badge(k1["pos"])}
        <span class="mlb-tag">{k1["mlb"]}</span>
      </td>
      <td>
        <span class="player-rank-sm">#{k2["rank"]}</span>
        {k2["name"]} {pos_badge(k2["pos"])}
        <span class="mlb-tag">{k2["mlb"]}</span>
      </td>
      <td class="combined-cell">#{team["combined"]}</td>
    </tr>'''

# Bar chart data (invert: lower combined rank = stronger)
max_combined = max(k['combined'] for k in KEEPERS)
chart_labels = json.dumps([k['team'] for k in KEEPERS])
chart_values = json.dumps([k['combined'] for k in KEEPERS])
chart_colors = json.dumps([COLORS[i % len(COLORS)] for i in range(len(KEEPERS))])

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
<link rel="stylesheet" href="common.css">
<title>Summertime Sadness - 2026 Keepers</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  .chart-box {{ height: 420px; }}

  /* Cards grid */
  .keeper-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
    gap: 16px;
    margin-bottom: 32px;
  }}
  .keeper-card {{
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 10px;
    padding: 16px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }}
  .card-rank {{
    font-size: 0.75em;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    opacity: 0.8;
  }}
  .card-team {{
    font-size: 1em;
    font-weight: 700;
    color: #e2e8f0;
    padding-left: 10px;
  }}
  .card-players {{
    display: flex;
    flex-direction: column;
    gap: 8px;
  }}
  .player-row {{
    display: flex;
    align-items: center;
    gap: 8px;
    background: #0f172a;
    border-radius: 6px;
    padding: 8px 10px;
  }}
  .player-rank {{
    font-size: 1.1em;
    font-weight: 800;
    color: #f59e0b;
    min-width: 36px;
  }}
  .player-name {{
    flex: 1;
    font-weight: 600;
    color: #e2e8f0;
    font-size: 0.95em;
  }}
  .player-meta {{
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .player-mlb {{
    font-size: 0.8em;
    color: #64748b;
    font-weight: 500;
  }}
  .card-combined {{
    font-size: 0.82em;
    color: #64748b;
    text-align: right;
  }}

  /* Position badge */
  .pos-badge {{
    font-size: 0.72em;
    font-weight: 700;
    padding: 1px 5px;
    border-radius: 4px;
    white-space: nowrap;
  }}

  /* Table */
  .rank-cell {{ font-weight: 700; color: #64748b; text-align: center; }}
  .team-cell {{ font-weight: 600; white-space: nowrap; }}
  .combined-cell {{ font-weight: 800; color: #f59e0b; text-align: center; }}
  .player-rank-sm {{
    font-size: 0.85em;
    font-weight: 800;
    color: #f59e0b;
    margin-right: 4px;
  }}
  .mlb-tag {{
    font-size: 0.78em;
    color: #64748b;
    margin-left: 4px;
  }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>
<div class="container">
<h1>Summertime Sadness Fantasy Baseball</h1>
<p class="page-subtitle">2026 Keepers</p>

<p class="section-desc">Each team keeps 2 players in rounds 1 and 2. Rankings are Yahoo's 2026 preseason rankings for this league. <strong>Lower combined rank = stronger keeper pair.</strong></p>

<h3>Keeper Strength Rankings</h3>
<p class="section-desc">Combined Yahoo preseason rank — lower is better. Chart shows combined rank (shorter bar = stronger keepers).</p>
<div class="chart-box">
<canvas id="keeperChart"></canvas>
</div>

<h3>Team Keepers</h3>
<div class="keeper-grid">
{cards_html}
</div>

<h3>All Teams — Keeper Summary</h3>
<table>
<tr>
  <th>#</th>
  <th>Team</th>
  <th>Keeper 1</th>
  <th>Keeper 2</th>
  <th>Combined Rank</th>
</tr>
{table_rows}
</table>

</div>
<script>
new Chart(document.getElementById('keeperChart').getContext('2d'), {{
  type: 'bar',
  data: {{
    labels: {chart_labels},
    datasets: [{{
      label: 'Combined Yahoo Rank',
      data: {chart_values},
      backgroundColor: {chart_colors},
      borderRadius: 4
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    indexAxis: 'y',
    layout: {{ padding: {{ top: 10, right: 30 }} }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8',
        borderColor: '#475569', borderWidth: 1,
        callbacks: {{
          label: ctx => 'Combined rank: #' + ctx.parsed.x + ' (lower = stronger)'
        }}
      }}
    }},
    scales: {{
      x: {{
        ticks: {{ color: '#94a3b8' }},
        grid: {{ color: '#334155' }},
        title: {{ display: true, text: 'Combined Yahoo Preseason Rank (lower = stronger)', color: '#64748b' }}
      }},
      y: {{
        ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }},
        grid: {{ display: false }}
      }}
    }}
  }}
}});
</script>
</body>
</html>"""

with open(r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\keepers_2026.html', 'w', encoding='utf-8') as f:
    f.write(html)

print('Generated docs/keepers_2026.html')
for i, k in enumerate(KEEPERS):
    print(f"  {i+1}. {k['team']:30s} #{k['keeper1']['rank']} {k['keeper1']['name']} + #{k['keeper2']['rank']} {k['keeper2']['name']} = #{k['combined']}")
