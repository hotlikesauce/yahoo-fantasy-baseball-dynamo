import json, sys, io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 2027 Draft Pick Counts (from Yahoo showtradedpicks - updated Apr 2026)
# Index 0 = R1 (keeper), 1 = R2 (keeper), 2 = R3 (first tradeable pick), ...
TOTAL_ROUNDS  = 22
NUM_TEAMS     = 12
KEEPER_ROUNDS = 2

pick_counts = {
    'Grab Em by the Tatis':      [1,1,1,0,0,1,0,1,1,1,1,1,1,1,1,1,0,2,2,1,2,2],
    'Bob Witt Sex Machine':      [1,1,1,2,1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,1,1,1],
    'Los Basureros':             [1,1,1,2,2,1,2,1,1,1,2,1,1,1,1,0,3,0,0,0,0,0],
    'Rickie Flower':             [1,1,1,0,1,1,0,1,1,0,1,1,1,1,3,1,1,1,1,1,1,2],
    'Getting Plowed Again.':     [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
    'Hatfield Hurlers':          [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
    'Ian Cumsler':               [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
    'Moniebol \U0001f433':       [1,1,1,1,1,1,1,1,1,1,0,1,1,1,1,2,1,1,1,1,1,1],
    'OG9\ufe0f\u20e3':           [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
    'Aces 4 Days':               [1,1,1,1,1,1,2,1,1,2,1,1,1,1,0,1,1,1,1,1,1,0],
    'The Rosterbation Station':  [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],
    '\u00af\\_(\u30c4)_/\u00af': [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,1,1,2,1,1],
}

# Scoring: exponential decay by overall draft position
# Each pick worth 2% less than the previous. Scaled so avg team capital = 1000.
# Since 2027 pick ORDER within rounds is unknown, each pick in a round gets
# the average value for that round (distribute equally).
DRAFT_ROUNDS = TOTAL_ROUNDS - KEEPER_ROUNDS   # 20
TOTAL_PICKS  = DRAFT_ROUNDS * NUM_TEAMS        # 240
DECAY        = 0.98
STANDARD_CAPITAL = 1000

total_raw = sum(DECAY ** i for i in range(TOTAL_PICKS))
SCALE = STANDARD_CAPITAL * NUM_TEAMS / total_raw

def overall_pick(rnd, pos):
    """Round (3-22) + pick position (1-12) → overall pick number (1-240)."""
    return (rnd - KEEPER_ROUNDS - 1) * NUM_TEAMS + pos

def pick_value(overall):
    raw = SCALE * DECAY ** (overall - 1)
    return round(raw) if raw >= 0.5 else 0

def avg_round_value(rnd):
    """Average pick value across all 12 slots in a round."""
    base = overall_pick(rnd, 1)
    return sum(pick_value(base + i) for i in range(NUM_TEAMS)) / NUM_TEAMS

# Sample values for display
p1_val   = pick_value(1)
p12_val  = pick_value(12)
p49_val  = pick_value(overall_pick(7, 1))
p240_val = pick_value(240)

# ── Build per-team stats ───────────────────────────────────────────────────────

team_data = []
for team, rounds in pick_counts.items():
    capital = sum(
        rounds[rnd - 1] * avg_round_value(rnd)
        for rnd in range(KEEPER_ROUNDS + 1, TOTAL_ROUNDS + 1)
    )
    capital = round(capital)

    total_picks  = sum(rounds)
    extra_high   = sum(max(0, rounds[i] - 1) for i in range(10))
    traded_away  = sum(1 for i in range(TOTAL_ROUNDS) if rounds[i] == 0)

    team_data.append({
        'name':          team,
        'picks':         rounds,
        'total_picks':   total_picks,
        'capital':       capital,
        'capital_vs_std': capital - STANDARD_CAPITAL,
        'extra_high':    extra_high,
        'traded_away':   traded_away,
    })

team_data.sort(key=lambda x: x['capital'], reverse=True)

# Print summary
print("=== 2027 DRAFT CAPITAL ===")
print(f"Standard capital (avg): {STANDARD_CAPITAL}")
for i, t in enumerate(team_data):
    diff = f"+{t['capital_vs_std']}" if t['capital_vs_std'] > 0 else str(t['capital_vs_std'])
    print(f"  {i+1}. {t['name']:30s} Capital: {t['capital']:4d} ({diff:>5s})  Picks: {t['total_picks']:2d}  Traded Away: {t['traded_away']}")

# ── Colors ─────────────────────────────────────────────────────────────────────


colors = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]
color_map = {t['name']: colors[i % len(colors)] for i, t in enumerate(team_data)}

# ── Output JSON data file ───────────────────────────────────────────────────────
from datetime import datetime
json_out = {
    'generated': datetime.utcnow().isoformat() + 'Z',
    'totalRounds': TOTAL_ROUNDS,
    'keeperRounds': KEEPER_ROUNDS,
    'standardCapital': STANDARD_CAPITAL,
    'pickValues': {
        'p1': p1_val, 'p12': p12_val, 'p49': p49_val, 'p240': p240_val
    },
    'teams': [
        {
            'name': t['name'],
            'color': color_map[t['name']],
            'picks': t['picks'],
            'totalPicks': t['total_picks'],
            'capital': t['capital'],
            'capitalVsStd': t['capital_vs_std'],
            'extraHigh': t['extra_high'],
            'tradedAway': t['traded_away'],
        }
        for t in team_data
    ]
}
json_path = r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\data\draft_capital_2026.json'
import os
os.makedirs(os.path.dirname(json_path), exist_ok=True)
with open(json_path, 'w', encoding='utf-8') as f:
    json.dump(json_out, f, indent=2, ensure_ascii=False)
print(f'Generated {json_path}')

# ── Chart data (kept for reference; HTML now loads from JSON) ──────────────────

bar_labels = json.dumps([t['name'] for t in team_data])
bar_values = json.dumps([t['capital'] for t in team_data])
bar_colors = json.dumps([color_map[t['name']] for t in team_data])

# ── Grid rows ──────────────────────────────────────────────────────────────────

grid_rows = ""
for t in team_data:
    color    = color_map[t['name']]
    diff     = t['capital_vs_std']
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    diff_cls = "pos" if diff > 0 else "neg" if diff < 0 else "even"

    cells = ""
    for i, count in enumerate(t['picks']):
        if   count == 0: cls = "cell-zero"
        elif count == 1: cls = "cell-one"
        elif count == 2: cls = "cell-two"
        else:            cls = "cell-three"
        cells += f'<td class="{cls}">{count}</td>'

    grid_rows += (
        f'<tr><td class="team-cell"><span style="color:{color}">&#9679;</span> {t["name"]}</td>'
        f'{cells}'
        f'<td class="total-cell">{t["total_picks"]}</td>'
        f'<td class="capital-cell">{t["capital"]}</td>'
        f'<td class="{diff_cls}">{diff_str}</td></tr>'
    )

# ── Summary rows ───────────────────────────────────────────────────────────────

summary_rows = ""
for i, t in enumerate(team_data):
    color    = color_map[t['name']]
    diff     = t['capital_vs_std']
    diff_str = f"+{diff}" if diff > 0 else str(diff)
    diff_cls = "pos" if diff > 0 else "neg" if diff < 0 else "even"

    notable, missing = [], []
    for r_i, count in enumerate(t['picks']):
        if count >= 3:
            notable.append(f"{count}x R{r_i+1}")
        elif count == 2 and r_i < 10:
            notable.append(f"2x R{r_i+1}")
        if count == 0 and r_i < 15:
            missing.append(f"R{r_i+1}")

    note_html = ""
    if notable: note_html += '<span class="note-extra">'  + ', '.join(notable) + '</span>'
    if missing: note_html += ' <span class="note-missing">No ' + ', '.join(missing) + '</span>'

    summary_rows += (
        f'<tr><td class="rank">{i+1}</td>'
        f'<td><span style="color:{color}">&#9679;</span> {t["name"]}</td>'
        f'<td class="capital-cell">{t["capital"]}</td>'
        f'<td class="{diff_cls}">{diff_str}</td>'
        f'<td>{t["total_picks"]}</td>'
        f'<td>{t["extra_high"]}</td>'
        f'<td>{t["traded_away"]}</td>'
        f'<td class="notes">{note_html}</td></tr>'
    )

# ── HTML ───────────────────────────────────────────────────────────────────────

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
<link rel="stylesheet" href="common.css">
<title>Summertime Sadness - 2027 Draft Capital</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  .chart-box {{ height: 450px; }}

  .grid-table {{ overflow-x: auto; margin-bottom: 24px; }}
  .grid-table table {{ min-width: 900px; }}
  .grid-table th {{ font-size: 0.72em; padding: 8px 4px; text-align: center; min-width: 32px; }}
  .grid-table th:first-child {{ text-align: left; min-width: 180px; }}
  .grid-table th.keeper {{ color: #475569; font-style: italic; }}
  .grid-table td {{ text-align: center; padding: 6px 4px; font-size: 0.85em; font-weight: 600; }}
  .team-cell {{ text-align: left !important; white-space: nowrap; font-weight: 500 !important; }}
  .cell-zero  {{ background: #ef444422; color: #ef4444; }}
  .cell-one   {{ color: #64748b; }}
  .cell-two   {{ background: #4ade8018; color: #4ade80; }}
  .cell-three {{ background: #22c55e44; color: #4ade80; font-weight: 800; text-shadow: 0 0 6px #22c55e66; }}
  .total-cell   {{ font-weight: 700; color: #e2e8f0; }}
  .capital-cell {{ font-weight: 700; color: #34d399; }}
  .pos  {{ color: #22c55e; font-weight: 700; }}
  .neg  {{ color: #ef4444; font-weight: 700; }}
  .even {{ color: #64748b; }}
  .notes {{ font-size: 0.82em; }}
  .note-extra   {{ color: #22c55e; }}
  .note-missing {{ color: #ef4444; opacity: 0.7; }}

  th.sortable {{ cursor: pointer; user-select: none; position: relative; padding-right: 14px; }}
  th.sortable:hover {{ color: #e2e8f0; }}
  th.sortable::after {{ content: '\\2195'; position: absolute; right: 2px; opacity: 0.3; font-size: 0.8em; }}
  th.sortable.asc::after  {{ content: '\\2191'; opacity: 0.8; }}
  th.sortable.desc::after {{ content: '\\2193'; opacity: 0.8; }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>
<div class="container">
<h1>Summertime Sadness Fantasy Baseball</h1>
<p class="page-subtitle">2027 Draft Capital</p>
<p class="section-desc" style="color:#64748b;margin-bottom:24px;">Current pick ownership as of April 2026. Pick value uses exponential decay &mdash; picks within a round are distributed equally since draft order is not yet set.</p>

<h3>Draft Capital Rankings</h3>
<p class="section-desc">Each pick is weighted by exponential decay (2% per slot). Pick 1 (R3P1) = {p1_val} pts, Pick 12 (R3P12) = {p12_val} pts, Pick 49 (R7P1) = {p49_val} pts, Pick 240 (R22P12) = {p240_val} pts. Standard (avg) = {STANDARD_CAPITAL} pts.</p>
<div class="chart-box">
<canvas id="capitalChart"></canvas>
</div>

<h3>Draft Capital Summary</h3>
<table>
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="num">Capital</th><th class="sortable" data-type="num">vs Avg</th><th class="sortable" data-type="num">Total Picks</th><th class="sortable" data-type="num">Bonus R1-10</th><th class="sortable" data-type="num">Rounds Empty</th><th>Notable</th></tr>
{summary_rows}
</table>

<h3>Pick Grid</h3>
<p class="section-desc">Number of picks each team holds per round. R1-R2 are keeper rounds (not tradeable). <span class="cell-zero" style="padding:2px 6px;border-radius:4px;">0 = traded away</span> &nbsp; <span style="color:#64748b">1 = standard</span> &nbsp; <span class="cell-two" style="padding:2px 6px;border-radius:4px;">2 = extra</span> &nbsp; <span class="cell-three" style="padding:2px 6px;border-radius:4px;">3+ = stockpiled</span></p>
<div class="grid-table">
<table>
<tr><th>Team</th>{''.join(f'<th class="keeper">R{i+1}</th>' if i < 2 else f'<th>R{i+1}</th>' for i in range(TOTAL_ROUNDS))}<th>Total</th><th>Capital</th><th>+/-</th></tr>
{grid_rows}
</table>
</div>

</div>
<script>
new Chart(document.getElementById('capitalChart').getContext('2d'), {{
  type: 'bar',
  data: {{
    labels: {bar_labels},
    datasets: [{{
      label: 'Draft Capital',
      data: {bar_values},
      backgroundColor: {bar_colors},
      borderRadius: 4
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    indexAxis: 'y',
    layout: {{ padding: {{ top: 20, right: 20 }} }},
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{
        backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8',
        borderColor: '#475569', borderWidth: 1,
        callbacks: {{
          label: ctx => {{
            const val = ctx.parsed.x;
            const diff = val - {STANDARD_CAPITAL};
            const sign = diff > 0 ? '+' : '';
            return 'Capital: ' + val + ' (' + sign + diff + ' vs avg)';
          }}
        }}
      }}
    }},
    scales: {{
      x: {{
        ticks: {{ color: '#94a3b8' }},
        grid: {{ color: '#334155' }},
        title: {{ display: true, text: 'Draft Capital Points', color: '#64748b' }}
      }},
      y: {{
        ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }},
        grid: {{ display: false }}
      }}
    }}
  }}
}});

const capitalChart = Chart.getChart('capitalChart');
const origDraw = capitalChart.draw.bind(capitalChart);
capitalChart.draw = function() {{
  origDraw();
  const ctx = this.ctx;
  const xScale = this.scales.x;
  const yScale = this.scales.y;
  const x = xScale.getPixelForValue({STANDARD_CAPITAL});
  ctx.save();
  ctx.strokeStyle = '#94a3b866';
  ctx.lineWidth = 2;
  ctx.setLineDash([6, 4]);
  ctx.beginPath();
  ctx.moveTo(x, yScale.top);
  ctx.lineTo(x, yScale.bottom);
  ctx.stroke();
  ctx.fillStyle = '#94a3b8';
  ctx.font = '10px Segoe UI';
  ctx.textAlign = 'center';
  ctx.fillText('Avg ({STANDARD_CAPITAL})', x, yScale.top - 6);
  ctx.restore();
}};
capitalChart.draw();

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

print(f'\nDone. The HTML (draft_picks_2026.html) loads data dynamically from docs/data/draft_capital_2026.json.')
