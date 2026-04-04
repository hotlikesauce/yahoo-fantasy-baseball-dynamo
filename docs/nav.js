(function() {
  const style = document.createElement('style');
  style.textContent = `
    /* ── Shared ─────────────────────────────────────────────────────────── */
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
      flex-shrink: 0;
    }

    /* ── Desktop nav ─────────────────────────────────────────────────────── */
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
      cursor: pointer;
      user-select: none;
    }
    .ss-nav-link:hover { color: #e2e8f0; }
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
    .ss-nav-item:hover .ss-dropdown { display: block; }
    .ss-dropdown a {
      display: block;
      padding: 10px 18px;
      color: #e2e8f0;
      text-decoration: none;
      font-size: 0.88em;
      transition: background 0.15s;
    }
    .ss-dropdown a:hover { background: #334155; }
    .ss-dropdown a.active { color: #3b82f6; }
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

    /* ── Hamburger button (mobile only) ──────────────────────────────────── */
    .ss-hamburger {
      display: none;
      margin-left: auto;
      background: none;
      border: none;
      cursor: pointer;
      padding: 8px;
      color: #94a3b8;
      flex-direction: column;
      gap: 5px;
    }
    .ss-hamburger span {
      display: block;
      width: 22px;
      height: 2px;
      background: currentColor;
      border-radius: 2px;
      transition: all 0.2s;
    }
    .ss-hamburger.open span:nth-child(1) { transform: translateY(7px) rotate(45deg); }
    .ss-hamburger.open span:nth-child(2) { opacity: 0; }
    .ss-hamburger.open span:nth-child(3) { transform: translateY(-7px) rotate(-45deg); }

    /* ── Mobile panel ────────────────────────────────────────────────────── */
    .ss-mobile-menu {
      display: none;
      background: #0f172a;
      border-top: 1px solid #1e293b;
      padding: 12px 0 20px;
      max-height: calc(100vh - 52px);
      overflow-y: auto;
    }
    .ss-mobile-menu.open { display: block; }

    /* Section header (acts like the top-level nav items) */
    .ss-mob-section-btn {
      width: 100%;
      background: none;
      border: none;
      text-align: left;
      padding: 12px 24px;
      color: #94a3b8;
      font-size: 0.95em;
      font-weight: 600;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-family: 'Segoe UI', system-ui, sans-serif;
      transition: color 0.15s;
    }
    .ss-mob-section-btn:hover { color: #e2e8f0; }
    .ss-mob-section-btn.active { color: #3b82f6; }
    .ss-mob-section-btn .ss-mob-arrow {
      font-size: 0.7em;
      transition: transform 0.2s;
    }
    .ss-mob-section-btn.expanded .ss-mob-arrow { transform: rotate(180deg); }

    /* Direct links (no sub-items) */
    .ss-mob-link {
      display: block;
      padding: 12px 24px;
      color: #94a3b8;
      text-decoration: none;
      font-size: 0.95em;
      font-weight: 600;
      transition: color 0.15s;
    }
    .ss-mob-link:hover { color: #e2e8f0; }
    .ss-mob-link.active { color: #3b82f6; }

    /* Accordion sub-items */
    .ss-mob-sub {
      display: none;
      background: #0a0f1e;
      border-top: 1px solid #1e293b;
      border-bottom: 1px solid #1e293b;
      padding: 6px 0;
    }
    .ss-mob-sub.open { display: block; }
    .ss-mob-sub a {
      display: block;
      padding: 10px 36px;
      color: #cbd5e1;
      text-decoration: none;
      font-size: 0.88em;
      transition: color 0.15s;
    }
    .ss-mob-sub a:hover { color: #e2e8f0; }
    .ss-mob-sub a.active { color: #3b82f6; }
    .ss-mob-sub a.disabled { color: #475569; pointer-events: none; }
    .ss-mob-sub .ss-mob-label {
      padding: 8px 36px 4px;
      font-size: 0.7em;
      text-transform: uppercase;
      color: #475569;
      letter-spacing: 0.06em;
    }
    .ss-mob-sub .ss-mob-divider {
      height: 1px;
      background: #1e293b;
      margin: 4px 0;
    }

    /* ── Responsive switch ───────────────────────────────────────────────── */
    @media (max-width: 768px) {
      .ss-nav-links { display: none; }
      .ss-hamburger { display: flex; }
    }
  `;
  document.head.appendChild(style);

  const path = window.location.pathname.split('/').pop() || 'index.html';
  function isActive(page) { return path === page ? ' active' : ''; }
  function sectionActive(pages) { return pages.includes(path) ? ' active' : ''; }

  const nav = document.getElementById('nav');
  if (!nav) return;

  const pages2026    = ['draft_picks_2026.html','draft_results_2026.html','trade_analyzer.html','keepers_2026.html','live_standings_2026.html','season_trends_2026.html','luck_analysis_2026.html','positional_strength_2026.html'];
  const pagesYoY     = ['season_trends_2025.html','luck_analysis_2025.html','draft_picks_2025.html','season_trends_2024.html','season_trends_2023.html','season_trends_2022.html'];

  nav.innerHTML = `
    <nav class="ss-nav">
      <div class="ss-nav-inner">
        <a href="index.html" class="ss-nav-brand">Summertime Sadness</a>

        <!-- Desktop links -->
        <ul class="ss-nav-links">
          <li class="ss-nav-item">
            <a class="ss-nav-link has-dropdown${sectionActive(pages2026)}">2026 Season</a>
            <div class="ss-dropdown">
              <a href="live_standings_2026.html" class="${isActive('live_standings_2026.html').trim()}">Live Standings</a>
              <a href="season_trends_2026.html" class="${isActive('season_trends_2026.html').trim()}">Season Trends</a>
              <a href="luck_analysis_2026.html" class="${isActive('luck_analysis_2026.html').trim()}">Luck Analysis</a>
              <a href="trade_analyzer.html" class="${isActive('trade_analyzer.html').trim()}">Trade Analyzer</a>
              <a href="positional_strength_2026.html" class="${isActive('positional_strength_2026.html').trim()}">Positional Strength</a>
              <div class="ss-divider"></div>
              <a href="draft_picks_2026.html" class="${isActive('draft_picks_2026.html').trim()}">Draft Capital</a>
              <a href="draft_results_2026.html" class="${isActive('draft_results_2026.html').trim()}">Draft Results</a>
              <a href="keepers_2026.html" class="${isActive('keepers_2026.html').trim()}">Keepers</a>
            </div>
          </li>
          <li class="ss-nav-item">
            <a class="ss-nav-link has-dropdown${sectionActive(pagesYoY)}">Year-over-Year</a>
            <div class="ss-dropdown">
              <div class="ss-label">2025 Season</div>
              <a href="season_trends_2025.html" class="${isActive('season_trends_2025.html').trim()}">Season Trends</a>
              <a href="draft_picks_2025.html" class="${isActive('draft_picks_2025.html').trim()}">Draft Capital</a>
              <a href="luck_analysis_2025.html" class="${isActive('luck_analysis_2025.html').trim()}">Luck &amp; Matchup Analysis</a>
              <div class="ss-divider"></div>
              <div class="ss-label">Previous Seasons</div>
              <a href="season_trends_2024.html" class="${isActive('season_trends_2024.html').trim()}">2024 Season Trends</a>
              <a href="season_trends_2023.html" class="${isActive('season_trends_2023.html').trim()}">2023 Season Trends</a>
              <a href="season_trends_2022.html" class="${isActive('season_trends_2022.html').trim()}">2022 Season Trends</a>
            </div>
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
        </ul>

        <!-- Hamburger -->
        <button class="ss-hamburger" id="ssHamburger" aria-label="Menu">
          <span></span><span></span><span></span>
        </button>
      </div>

      <!-- Mobile panel -->
      <div class="ss-mobile-menu" id="ssMobileMenu">

        <!-- 2026 Season accordion -->
        <button class="ss-mob-section-btn${sectionActive(pages2026)}" data-target="mob2026">
          2026 Season <span class="ss-mob-arrow">&#9660;</span>
        </button>
        <div class="ss-mob-sub${pages2026.includes(path) ? ' open' : ''}" id="mob2026">
          <a href="live_standings_2026.html" class="${isActive('live_standings_2026.html').trim()}">Live Standings</a>
          <a href="season_trends_2026.html" class="${isActive('season_trends_2026.html').trim()}">Season Trends</a>
          <a href="luck_analysis_2026.html" class="${isActive('luck_analysis_2026.html').trim()}">Luck Analysis</a>
          <a href="trade_analyzer.html" class="${isActive('trade_analyzer.html').trim()}">Trade Analyzer</a>
          <a href="positional_strength_2026.html" class="${isActive('positional_strength_2026.html').trim()}">Positional Strength</a>
          <div class="ss-mob-divider"></div>
          <a href="draft_picks_2026.html" class="${isActive('draft_picks_2026.html').trim()}">Draft Capital</a>
          <a href="draft_results_2026.html" class="${isActive('draft_results_2026.html').trim()}">Draft Results</a>
          <a href="keepers_2026.html" class="${isActive('keepers_2026.html').trim()}">Keepers</a>
        </div>

        <!-- Year-over-Year accordion -->
        <button class="ss-mob-section-btn${sectionActive(pagesYoY)}" data-target="mobYoY">
          Year-over-Year <span class="ss-mob-arrow">&#9660;</span>
        </button>
        <div class="ss-mob-sub${pagesYoY.includes(path) ? ' open' : ''}" id="mobYoY">
          <div class="ss-mob-label">2025 Season</div>
          <a href="season_trends_2025.html" class="${isActive('season_trends_2025.html').trim()}">Season Trends</a>
          <a href="draft_picks_2025.html" class="${isActive('draft_picks_2025.html').trim()}">Draft Capital</a>
          <a href="luck_analysis_2025.html" class="${isActive('luck_analysis_2025.html').trim()}">Luck &amp; Matchup Analysis</a>
          <div class="ss-mob-divider"></div>
          <div class="ss-mob-label">Previous Seasons</div>
          <a href="season_trends_2024.html" class="${isActive('season_trends_2024.html').trim()}">2024 Season Trends</a>
          <a href="season_trends_2023.html" class="${isActive('season_trends_2023.html').trim()}">2023 Season Trends</a>
          <a href="season_trends_2022.html" class="${isActive('season_trends_2022.html').trim()}">2022 Season Trends</a>
        </div>

        <!-- Flat links -->
        <a href="all_time_rankings.html" class="ss-mob-link${isActive('all_time_rankings.html')}">All-Time Records</a>
        <a href="h2h_records.html" class="ss-mob-link${isActive('h2h_records.html')}">H2H Records</a>
        <a href="manager_profiles.html" class="ss-mob-link${isActive('manager_profiles.html')}">Manager Profiles</a>
      </div>
    </nav>
  `;

  // Hamburger toggle
  const hamburger = document.getElementById('ssHamburger');
  const mobileMenu = document.getElementById('ssMobileMenu');
  hamburger.addEventListener('click', function() {
    hamburger.classList.toggle('open');
    mobileMenu.classList.toggle('open');
  });

  // Accordion toggles — auto-expand current section, toggle others
  document.querySelectorAll('.ss-mob-section-btn').forEach(function(btn) {
    const sub = document.getElementById(btn.dataset.target);
    if (sub && sub.classList.contains('open')) {
      btn.classList.add('expanded');
    }
    btn.addEventListener('click', function() {
      const isOpen = sub.classList.contains('open');
      // Collapse all
      document.querySelectorAll('.ss-mob-sub').forEach(function(s) { s.classList.remove('open'); });
      document.querySelectorAll('.ss-mob-section-btn').forEach(function(b) { b.classList.remove('expanded'); });
      // Open clicked if it was closed
      if (!isOpen) {
        sub.classList.add('open');
        btn.classList.add('expanded');
      }
    });
  });

})();
