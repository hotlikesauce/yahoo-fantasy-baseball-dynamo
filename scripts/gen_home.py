import boto3, json, sys, io, os, requests
from collections import Counter
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-AllTimeRankings')

# ============================================================
# 0. Pull live 2026 team names from Yahoo API
# ============================================================
YAHOO_CONSUMER_KEY = os.getenv('YAHOO_CONSUMER_KEY')
YAHOO_CONSUMER_SECRET = os.getenv('YAHOO_CONSUMER_SECRET')
YAHOO_REFRESH_TOKEN = os.getenv('YAHOO_REFRESH_TOKEN')
YAHOO_LEAGUE_IDS_STR = os.getenv('YAHOO_LEAGUE_IDS')

league_ids = {}
for pair in YAHOO_LEAGUE_IDS_STR.split(','):
    year, lid = pair.split(':')
    league_ids[int(year)] = lid

LEAGUE_KEY_2026 = f"469.l.{league_ids[2026]}"
BASE_URL = "https://fantasysports.yahooapis.com/fantasy/v2"

# Get OAuth token
token_resp = requests.post("https://api.login.yahoo.com/oauth2/get_token", data={
    'client_id': YAHOO_CONSUMER_KEY, 'client_secret': YAHOO_CONSUMER_SECRET,
    'refresh_token': YAHOO_REFRESH_TOKEN, 'grant_type': 'refresh_token'
})
yahoo_token = token_resp.json()['access_token']
yahoo_headers = {'Authorization': f'Bearer {yahoo_token}', 'Accept': 'application/json'}

# Pull 2026 standings for team names + managers
standings_resp = requests.get(f"{BASE_URL}/league/{LEAGUE_KEY_2026}/standings?format=json", headers=yahoo_headers)
standings_data = standings_resp.json()['fantasy_content']['league']
teams_2026 = []
teams_raw = standings_data[1]['standings'][0]['teams']
for tidx in teams_raw:
    if tidx == 'count':
        continue
    team_list = teams_raw[tidx]['team']
    basic = team_list[0]
    team_name = manager = None
    for item in basic:
        if not isinstance(item, dict):
            continue
        if 'name' in item:
            team_name = item['name']
        elif 'managers' in item:
            mgrs = item['managers']
            if isinstance(mgrs, list):
                manager = mgrs[0].get('manager', {}).get('nickname', '?')
    # Normalize manager name for profile links
    if manager:
        manager = manager.strip().title().split()[0] if ' ' in manager.strip() else manager.strip().title()
    # Yahoo nickname overrides (Yahoo account name != league name)
    MANAGER_OVERRIDES = {'Michael': 'Mikey'}
    if manager in MANAGER_OVERRIDES:
        manager = MANAGER_OVERRIDES[manager]
    teams_2026.append({'name': team_name, 'manager': manager})

# Sort alphabetically by manager for consistent display
teams_2026.sort(key=lambda t: (t['manager'] or '').lower())
print(f"2026 teams from Yahoo API:")
for t in teams_2026:
    print(f"  {t['name']} ({t['manager']})")

# ============================================================
# 1. Pull all champion data
# ============================================================
champs = []
r = table.scan(FilterExpression='attribute_exists(Champion)')
champs.extend(r['Items'])
while 'LastEvaluatedKey' in r:
    r = table.scan(FilterExpression='attribute_exists(Champion)', ExclusiveStartKey=r['LastEvaluatedKey'])
    champs.extend(r['Items'])
champs.sort(key=lambda x: x['Year'])

current_champ = champs[-1] if champs else None
print(f"Current champion: {current_champ['Team']} ({current_champ.get('Manager','?')}) - {current_champ['Year']}")

# Title counts
titles = Counter(c.get('Manager', '?') for c in champs)
title_leader = titles.most_common(1)[0]
title_list = titles.most_common()

# ============================================================
# 2. Pull top seasons (all-time best scores)
# ============================================================
all_items = []
r = table.scan()
all_items.extend(r['Items'])
while 'LastEvaluatedKey' in r:
    r = table.scan(ExclusiveStartKey=r['LastEvaluatedKey'])
    all_items.extend(r['Items'])

# Filter to items with Score_Sum
scored = [i for i in all_items if 'Score_Sum' in i]
scored.sort(key=lambda x: float(x['Score_Sum']), reverse=True)
top_season = scored[0] if scored else None

# Unique years and managers
all_years = sorted(set(i['Year'] for i in all_items))
all_managers = sorted(set(i.get('Manager', '?') for i in all_items if i.get('Manager', '?') != '?'))

print(f"Top season: {top_season['Team']} ({top_season['Year']}) - {float(top_season['Score_Sum']):.0f}")
print(f"Years: {all_years[0]}-{all_years[-1]} ({len(all_years)} seasons)")
print(f"Managers: {len(all_managers)}")

# ============================================================
# 3. Recent champions (last 5)
# ============================================================
recent_champs = champs[-10:]
recent_champs_html = ""
for c in reversed(recent_champs):
    score = c.get('Score_Sum', c.get('Score', ''))
    try:
        score = f"{float(score):.0f}"
    except (ValueError, TypeError):
        score = "N/A"
    recent_champs_html += f'<div class="champ-row"><span class="champ-year">{c["Year"]}</span><span class="champ-name">{c["Team"]}</span><span class="champ-mgr">{c.get("Manager","?")}</span></div>'

# ============================================================
# 4. Title leaderboard
# ============================================================
title_board_html = ""
for mgr, count in title_list[:10]:
    bar_width = int(count / title_list[0][1] * 100)
    title_board_html += f'<div class="title-row"><span class="title-name">{mgr}</span><div class="title-bar-bg"><div class="title-bar" style="width:{bar_width}%">{count}</div></div></div>'

# ============================================================
# 5. Generate HTML
# ============================================================
html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
<link rel="stylesheet" href="common.css">
<title>Summertime Sadness Fantasy Baseball</title>
<style>
  .year20-badge {{
    display: inline-block;
    background: linear-gradient(135deg, #fbbf24, #f59e0b);
    color: #0f172a;
    font-weight: 900;
    font-size: 1.4em;
    letter-spacing: 0.12em;
    padding: 10px 32px;
    border-radius: 50px;
    margin-bottom: 20px;
    box-shadow: 0 0 30px rgba(251, 191, 36, 0.3), 0 0 60px rgba(251, 191, 36, 0.1);
    animation: glow20 3s ease-in-out infinite alternate;
  }}
  @keyframes glow20 {{
    from {{ box-shadow: 0 0 20px rgba(251, 191, 36, 0.2), 0 0 40px rgba(251, 191, 36, 0.1); }}
    to {{ box-shadow: 0 0 30px rgba(251, 191, 36, 0.4), 0 0 60px rgba(251, 191, 36, 0.15); }}
  }}

  .hero {{
    text-align: center;
    padding: 60px 24px 48px;
    position: relative;
  }}
  .hero h1 {{
    font-size: 3em;
    font-weight: 800;
    background: linear-gradient(135deg, #3b82f6, #8b5cf6);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 8px;
  }}
  .hero p {{
    color: #64748b;
    font-size: 1.15em;
  }}
  .hero .est {{
    color: #475569;
    font-size: 0.85em;
    margin-top: 4px;
  }}

  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
    margin: 32px 0;
  }}
  .stat-card {{
    background: #1e293b;
    border-radius: 12px;
    padding: 24px;
    border: 1px solid #334155;
    transition: border-color 0.2s, transform 0.2s;
  }}
  .stat-card:hover {{
    border-color: #475569;
    transform: translateY(-2px);
  }}
  .stat-card .label {{
    color: #64748b;
    font-size: 0.8em;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 8px;
  }}
  .stat-card .value {{
    font-size: 1.8em;
    font-weight: 700;
  }}
  .stat-card .sub {{
    color: #94a3b8;
    font-size: 0.9em;
    margin-top: 4px;
  }}
  .champ-row {{
    display: flex;
    align-items: center;
    padding: 10px 0;
    border-bottom: 1px solid #1e293b;
    gap: 12px;
  }}
  .champ-row:last-child {{ border-bottom: none; }}
  .champ-year {{
    color: #a78bfa;
    font-weight: 700;
    font-size: 0.95em;
    min-width: 40px;
  }}
  .champ-name {{
    color: #e2e8f0;
    flex: 1;
    font-size: 0.95em;
  }}
  .champ-mgr {{
    color: #64748b;
    font-size: 0.85em;
  }}

  .title-row {{
    display: flex;
    align-items: center;
    padding: 8px 0;
    gap: 12px;
  }}
  .title-name {{
    min-width: 80px;
    color: #e2e8f0;
    font-size: 0.95em;
    font-weight: 500;
  }}
  .title-bar-bg {{
    flex: 1;
    background: #0f172a;
    border-radius: 6px;
    height: 28px;
    overflow: hidden;
  }}
  .title-bar {{
    background: linear-gradient(90deg, #3b82f6, #8b5cf6);
    height: 100%;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding-right: 10px;
    font-weight: 700;
    font-size: 0.9em;
    color: white;
    min-width: 30px;
  }}

  .nav-cards {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
    margin: 32px 0;
  }}
  .nav-card {{
    background: #1e293b;
    border-radius: 12px;
    padding: 28px;
    border: 1px solid #334155;
    text-decoration: none;
    transition: border-color 0.2s, transform 0.2s;
    display: block;
  }}
  .nav-card:hover {{
    border-color: #3b82f6;
    transform: translateY(-3px);
  }}
  .nav-card h3 {{
    color: #e2e8f0;
    font-size: 1.1em;
    margin: 0 0 8px;
    border-bottom: none;
    padding-bottom: 0;
  }}
  .nav-card p {{
    color: #94a3b8;
    font-size: 0.9em;
    line-height: 1.5;
  }}
  .nav-card .arrow {{
    color: #3b82f6;
    font-size: 0.85em;
    margin-top: 12px;
    display: block;
  }}

  .coming-soon {{
    text-align: center;
    padding: 40px;
    color: #475569;
    font-size: 0.95em;
  }}

  .standings-table {{
    width: 100%;
    border-collapse: collapse;
  }}
  .standings-table th {{
    background: #1e293b;
    color: #64748b;
    font-weight: 600;
    text-align: left;
    padding: 10px 14px;
    font-size: 0.8em;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    border-bottom: 2px solid #334155;
  }}
  .standings-table th:nth-child(n+3) {{ text-align: center; }}
  .standings-table td {{
    padding: 10px 14px;
    border-bottom: 1px solid #1e293b;
    font-size: 0.92em;
  }}
  .standings-table td:nth-child(n+3) {{ text-align: center; color: #94a3b8; }}
  .standings-table tr:hover {{ background: #1e293b55; }}
  .standings-table .team-name {{ font-weight: 700; color: #e2e8f0; }}
  .standings-table .mgr-link {{ color: #64748b; font-size: 0.85em; margin-left: 8px; text-decoration: none; }}
  .standings-table .mgr-link:hover {{ color: #3b82f6; text-decoration: underline; }}
  .standings-table .rank-num {{ color: #64748b; font-weight: 500; width: 30px; }}
  .standings-table .team-name {{ font-size: 0.88em; white-space: nowrap; }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>

<div class="hero">
  <div class="year20-badge">YEAR 20</div>
  <h1>Summertime Sadness</h1>
  <p>Fantasy Baseball Analytics</p>
  <p class="est">Est. 2007 &middot; Entering Our 20th Year</p>
</div>

<div class="container">

<div class="two-col">
  <div>
    <h2 style="margin-top:0">2026 Standings</h2>
    <div class="panel">
      <table class="standings-table">
        <thead>
          <tr>
            <th>#</th>
            <th style="text-align:left">Team</th>
            <th>W-L-T</th>
            <th>xWins</th>
          </tr>
        </thead>
        <tbody id="standingsBody">
          {''.join(f'<tr><td class="rank-num">{i+1}</td><td><span class="team-name">{t["name"]}</span><a href="manager_profiles.html#{t["manager"]}" class="mgr-link">{t["manager"]}</a></td><td class="wlt-cell" data-team="{t["name"]}">&mdash;</td><td class="xw-cell" data-team="{t["name"]}">&mdash;</td></tr>' for i, t in enumerate(teams_2026))}
        </tbody>
      </table>
    </div>
  </div>
  <div>
    <h2 style="margin-top:0">2026 Power Rankings</h2>
    <div class="panel">
      <table class="standings-table">
        <thead>
          <tr>
            <th>#</th>
            <th style="text-align:left">Team</th>
            <th>Score</th>
            <th>Trend</th>
          </tr>
        </thead>
        <tbody id="powerBody">
          <tr><td colspan="4" style="text-align:center;color:#475569;padding:20px">Loading...</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</div>

<script>
(async function() {{
  const TRENDS_URL = 'https://gm2rzjyfympxxazymzzf4esehi0udsga.lambda-url.us-west-2.on.aws/';

  try {{
    const trendsResp = await fetch(TRENDS_URL);
    const trends = await trendsResp.json();

    // --- xWins from trends ---
    if (trends && trends.cumulativeXwins && trends.statWeeks && trends.statWeeks.length) {{
      const lastWeek = trends.statWeeks[trends.statWeeks.length - 1];
      const cumXwins = {{}};
      trends.teams.forEach(t => {{
        const val = trends.cumulativeXwins[t.tn] && trends.cumulativeXwins[t.tn][lastWeek] != null
          ? parseFloat(trends.cumulativeXwins[t.tn][lastWeek]) : null;
        cumXwins[t.name] = val;
      }});
      document.querySelectorAll('.xw-cell').forEach(cell => {{
        const xw = cumXwins[cell.dataset.team];
        cell.textContent = xw != null ? xw.toFixed(1) : '—';
      }});
    }}

    // --- W-L-T from compute_season_trends: derived from weekly_results, completed weeks only ---
    const rows2026 = trends && trends.standings ? trends.standings : [];
    if (rows2026.length) {{
      const standingsMap = {{}};
      rows2026.forEach(s => {{ standingsMap[s.name] = s; }});

      // Sort by W-L-T pct
      const sorted = [...rows2026].sort((a, b) => {{
        const pa = (a.wins + a.ties * 0.5) / Math.max(1, a.wins + a.losses + a.ties);
        const pb = (b.wins + b.ties * 0.5) / Math.max(1, b.wins + b.losses + b.ties);
        return pb - pa;
      }});
      const rankMap = {{}};
      sorted.forEach((s, i) => {{ rankMap[s.name] = i + 1; }});

      document.querySelectorAll('.wlt-cell').forEach(cell => {{
        const s = standingsMap[cell.dataset.team];
        if (s) cell.textContent = s.wins + '-' + s.losses + (s.ties ? '-' + s.ties : '');
      }});

      const tbody = document.getElementById('standingsBody');
      const tbRows = Array.from(tbody.querySelectorAll('tr'));
      tbRows.sort((a, b) => (rankMap[a.querySelector('.wlt-cell').dataset.team] || 99) - (rankMap[b.querySelector('.wlt-cell').dataset.team] || 99));
      tbRows.forEach((r, i) => {{ r.querySelector('.rank-num').textContent = i + 1; tbody.appendChild(r); }});
    }}

    // --- Power Rankings from trends data ---
    if (trends && trends.teams && trends.cumulativePowerScores) {{
      const lastWeek = trends.statWeeks[trends.statWeeks.length - 1];
      // teams array is already sorted by cumulative power score
      const powerRanked = trends.teams.map((t, i) => {{
        const score = trends.weeklyPowerScores[t.tn] && trends.weeklyPowerScores[t.tn][lastWeek] != null
          ? parseFloat(trends.weeklyPowerScores[t.tn][lastWeek]) : null;
        const powerRank = i + 1;
        // Actual standings rank from completed-week standings
        let actualRank = null;
        if (rows2026.length) {{
          const idx = rows2026.findIndex(s => s.name === t.name);
          if (idx >= 0) actualRank = idx + 1;
        }}
        const trend = actualRank ? actualRank - powerRank : null;
        return {{ name: t.name, score, powerRank, trend }};
      }});

      const powerBody = document.getElementById('powerBody');
      powerBody.innerHTML = '';
      powerRanked.forEach((t, i) => {{
        let trendStr = '&mdash;';
        if (t.trend !== null) {{
          if (t.trend > 0) trendStr = '<span style="color:#ef4444">&#x2193;' + t.trend + '</span>';
          else if (t.trend < 0) trendStr = '<span style="color:#22c55e">&#x2191;' + Math.abs(t.trend) + '</span>';
          else trendStr = '<span style="color:#64748b">&#8212;</span>';
        }}
        powerBody.innerHTML += `<tr><td class="rank-num">${{i+1}}</td><td><span class="team-name">${{t.name}}</span></td><td>${{t.score != null ? t.score.toFixed(0) : '&mdash;'}}</td><td>${{trendStr}}</td></tr>`;
      }});
    }}
  }} catch(e) {{
    console.error('Failed to load live data:', e);
  }}
}})();
</script>

<div class="stats-grid">
  <div class="stat-card">
    <div class="label">2025 Champion</div>
    <div class="value gold">{current_champ['Team'] if current_champ else 'TBD'}</div>
    <div class="sub">{current_champ.get('Manager','?') if current_champ else ''} &middot; {len([c for c in champs if c.get('Manager') == current_champ.get('Manager')])} career titles</div>
  </div>
  <div class="stat-card">
    <div class="label">Most Titles</div>
    <div class="value blue">{title_leader[0]} ({title_leader[1]})</div>
    <div class="sub">{', '.join(f'{m} ({c})' for m, c in title_list[1:4])}</div>
  </div>
  <div class="stat-card">
    <div class="label">All-Time Best Season</div>
    <div class="value green">{float(top_season['Score_Sum']):.0f}</div>
    <div class="sub">{top_season['Team'].replace('🏆','').strip()} &middot; {top_season.get('Manager','?')} ({top_season['Year']})</div>
  </div>
  <div class="stat-card">
    <div class="label">League History</div>
    <div class="value purple">20th Year</div>
    <div class="sub">Est. 2007 &middot; {len(champs)} champions crowned</div>
  </div>
</div>

<div class="two-col">
  <div class="panel">
    <h3>Recent Champions</h3>
    {recent_champs_html}
  </div>
  <div class="panel">
    <h3>Title Leaders</h3>
    {title_board_html}
  </div>
</div>

<h2>Explore</h2>
<div class="nav-cards">
  <a href="all_time_rankings.html" class="nav-card">
    <h3>All-Time Records</h3>
    <p>Top 20 greatest seasons, champions by year, career averages, and the complete historical record from 2007-2025.</p>
    <span class="arrow">View records &rarr;</span>
  </a>
  <a href="season_trends_2026.html" class="nav-card">
    <h3>2026 Season Trends</h3>
    <p>Weekly power scores, power rankings, expected category wins, hot/cold streaks, and season bests.</p>
    <span class="arrow">View trends &rarr;</span>
  </a>
  <a href="luck_analysis_2026.html" class="nav-card">
    <h3>2026 Luck & Matchup Analysis</h3>
    <p>Luck scores, all-play records, xWins, schedule strength, what-if standings, best/worst matchups, and weekly dominators.</p>
    <span class="arrow">View analysis &rarr;</span>
  </a>
  <div class="nav-card" style="cursor: default; opacity: 0.5;">
    <h3>Weekly Recaps</h3>
    <p>AI-generated weekly summaries with highlights, upsets, and storylines. Coming soon.</p>
    <span class="arrow" style="color: #475569;">Coming soon</span>
  </div>
</div>

</div>
</body>
</html>"""

with open(r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\index.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\nGenerated docs/index.html')
