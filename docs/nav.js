(function() {
  // Inject CSS
  const style = document.createElement('style');
  style.textContent = `
    .ss-nav {
      background: #0f172a;
      border-bottom: 1px solid #1e293b;
      padding: 0 24px;
      position: sticky;
      top: 0;
      z-index: 1000;
      font-family: 'Segoe UI', system-ui, sans-serif;
    }
    .ss-nav-inner {
      max-width: 1400px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      height: 52px;
      gap: 0;
    }
    .ss-nav-brand {
      font-size: 1.05em;
      font-weight: 700;
      background: linear-gradient(135deg, #3b82f6, #8b5cf6);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      text-decoration: none;
      margin-right: 32px;
      white-space: nowrap;
    }
    .ss-nav-links {
      display: flex;
      align-items: center;
      gap: 0;
      list-style: none;
      margin: 0;
      padding: 0;
      height: 100%;
    }
    .ss-nav-item {
      position: relative;
      height: 100%;
      display: flex;
      align-items: center;
    }
    .ss-nav-link {
      color: #94a3b8;
      text-decoration: none;
      padding: 0 16px;
      font-size: 0.9em;
      font-weight: 500;
      height: 100%;
      display: flex;
      align-items: center;
      transition: color 0.15s;
      white-space: nowrap;
    }
    .ss-nav-link:hover {
      color: #e2e8f0;
    }
    .ss-nav-link.active {
      color: #3b82f6;
      box-shadow: inset 0 -2px 0 #3b82f6;
    }
    .ss-nav-link.has-dropdown::after {
      content: '\\25BE';
      margin-left: 5px;
      font-size: 0.75em;
    }
    .ss-dropdown {
      display: none;
      position: absolute;
      top: 100%;
      left: 0;
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 6px 0;
      min-width: 220px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    }
    .ss-nav-item:hover .ss-dropdown {
      display: block;
    }
    .ss-dropdown a {
      display: block;
      padding: 10px 18px;
      color: #e2e8f0;
      text-decoration: none;
      font-size: 0.88em;
      transition: background 0.15s;
    }
    .ss-dropdown a:hover {
      background: #334155;
    }
    .ss-dropdown a.active {
      color: #3b82f6;
    }
    .ss-dropdown a.disabled {
      color: #475569;
      cursor: default;
      pointer-events: none;
    }
    .ss-dropdown .ss-divider {
      height: 1px;
      background: #334155;
      margin: 4px 0;
    }
    .ss-dropdown .ss-label {
      padding: 6px 18px 4px;
      font-size: 0.72em;
      text-transform: uppercase;
      color: #64748b;
      letter-spacing: 0.05em;
    }
  `;
  document.head.appendChild(style);

  // Determine current page
  const path = window.location.pathname.split('/').pop() || 'index.html';

  function isActive(page) {
    return path === page ? ' active' : '';
  }

  // Build nav
  const nav = document.getElementById('nav');
  if (!nav) return;

  nav.innerHTML = `
    <nav class="ss-nav">
      <div class="ss-nav-inner">
        <a href="index.html" class="ss-nav-brand">Summertime Sadness</a>
        <ul class="ss-nav-links">
          <li class="ss-nav-item">
            <a href="index.html" class="ss-nav-link${isActive('index.html')}">Home</a>
          </li>
          <li class="ss-nav-item">
            <a href="all_time_rankings.html" class="ss-nav-link${isActive('all_time_rankings.html')}">All-Time Records</a>
          </li>
          <li class="ss-nav-item">
            <a href="h2h_records.html" class="ss-nav-link${isActive('h2h_records.html')}">H2H Records</a>
          </li>
          <li class="ss-nav-item">
            <a href="manager_profiles.html" class="ss-nav-link${isActive('manager_profiles.html')}">Manager Profiles</a>
          </li>
          <li class="ss-nav-item">
            <a class="ss-nav-link has-dropdown${['draft_picks_2026.html','trade_analyzer.html'].includes(path) ? ' active' : ''}">2026 Season</a>
            <div class="ss-dropdown">
              <a href="trade_analyzer.html" class="${isActive('trade_analyzer.html').trim()}">Trade Analyzer</a>
              <a href="draft_picks_2026.html" class="${isActive('draft_picks_2026.html').trim()}">Draft Capital</a>
              <div class="ss-divider"></div>
              <a class="disabled">Season Trends - Coming Soon</a>
              <a class="disabled">Luck & Matchup Analysis - Coming Soon</a>
            </div>
          </li>
          <li class="ss-nav-item">
            <a class="ss-nav-link has-dropdown${['season_trends_2025.html','luck_analysis_2025.html','season_trends_2024.html','season_trends_2023.html','season_trends_2022.html'].includes(path) ? ' active' : ''}">Year-over-Year</a>
            <div class="ss-dropdown">
              <div class="ss-label">2025 Season</div>
              <a href="season_trends_2025.html" class="${isActive('season_trends_2025.html').trim()}">Season Trends</a>
              <a href="luck_analysis_2025.html" class="${isActive('luck_analysis_2025.html').trim()}">Luck & Matchup Analysis</a>
              <div class="ss-divider"></div>
              <div class="ss-label">Previous Seasons</div>
              <a href="season_trends_2024.html" class="${isActive('season_trends_2024.html').trim()}">2024 Season Trends</a>
              <a href="season_trends_2023.html" class="${isActive('season_trends_2023.html').trim()}">2023 Season Trends</a>
              <a href="season_trends_2022.html" class="${isActive('season_trends_2022.html').trim()}">2022 Season Trends</a>
            </div>
          </li>
        </ul>
      </div>
    </nav>
  `;
})();
