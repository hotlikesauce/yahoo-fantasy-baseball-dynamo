(function () {
  'use strict';

  // Captured during synchronous load: document.currentScript is null inside
  // deferred callbacks, so anything that needs the asset base computes it here.
  const ASSET_BASE = document.currentScript
    ? document.currentScript.src.replace(/nav\.js.*$/, '')
    : '';

  // ── 8-bit palette map ────────────────────────────────────────────────────
  // Old slate/Tailwind colour -> its 8-bit replacement. The single source of
  // truth: scripts/patch_page_heads.py reads this block (between the markers)
  // to rewrite colours inside each page, and the Chart.js plugin below uses it
  // for colours that arrive at runtime from the Lambdas. No value on the
  // right may also appear on the left, so remapping is idempotent.
  /*PALETTE*/
  const PALETTE = {
    "#0f172a": "#0d0d2b", "#1e293b": "#1f2160", "#334155": "#2b2e7a", "#475569": "#5c5c94",
    "#64748b": "#6e6ea8", "#94a3b8": "#9a9ac8", "#cbd5e1": "#c8c8e8", "#e2e8f0": "#f4f4f4",
    "#f1f5f9": "#ffffff", "#020617": "#07071a", "#0a0f1e": "#07071a", "#0b1324": "#0d0d2b",
    "#101827": "#0d0d2b", "#16203a": "#16163f", "#16233a": "#16163f", "#1e3a5f": "#2b2e7a",
    "#0b1a2a": "#0d0d2b", "#3b82f6": "#3b6bff", "#2563eb": "#2f4fe0", "#1d4ed8": "#2440c0",
    "#60a5fa": "#6b9bff", "#93c5fd": "#a8c4ff", "#2f77d6": "#3a62e8", "#8b5cf6": "#8a5cff",
    "#a78bfa": "#a98cff", "#c4b5fd": "#cbb8ff", "#22c55e": "#2ecc40", "#4ade80": "#5dff6a",
    "#86efac": "#9dffa6", "#bbf7d0": "#d0ffd4", "#14532d": "#145a1f", "#ef4444": "#ff3b3b",
    "#f87171": "#ff6b6b", "#fca5a5": "#ffa3a3", "#7f1d1d": "#7a1010", "#f59e0b": "#ffa41b",
    "#fbbf24": "#ffe066", "#fcd34d": "#ffe58a", "#d97706": "#e08a00", "#b45309": "#b85c00",
    "#92400e": "#8a4a00", "#fef3c7": "#fff4cc", "#06b6d4": "#3bd6ff", "#38bdf8": "#5ce1ff",
    "#4fc3f7": "#63d8ff", "#7dd3fc": "#8fe6ff", "#f97316": "#ff8c1a", "#7c2d12": "#7a2a00",
    "#eab308": "#ffd23f", "#a855f7": "#c45cff", "#4c1d95": "#4a1aa0", "#14b8a6": "#1ee8b0",
    "#14a89a": "#1cd6a0", "#5eead4": "#6ff5dc", "#ec4899": "#ff5ec8", "#f472b6": "#ff86d8",
    "#e11d48": "#ff2d6f", "#9f1239": "#a0124a", "#ffe4e6": "#ffe8ee", "#10b981": "#19d27f",
    "#34d399": "#3bf09a"
  };
  /*END PALETTE*/

  const RGB = {};
  Object.keys(PALETTE).forEach(function (k) {
    const h = (x) => parseInt(x, 16);
    const n = PALETTE[k];
    RGB[[h(k.substr(1, 2)), h(k.substr(3, 2)), h(k.substr(5, 2))].join(',')] =
      [h(n.substr(1, 2)), h(n.substr(3, 2)), h(n.substr(5, 2))].join(',');
  });
  // Map one colour string (hex, hex+alpha, rgb()/rgba()) to the 8-bit palette.
  function remapColor(c) {
    if (typeof c !== 'string') return c;
    return c
      // #rrggbb, or #rrggbbaa (the alpha pair is kept as-is).
      .replace(/#([0-9a-f]{6})(?=[0-9a-f]{2}(?![0-9a-f])|(?![0-9a-f]))/gi, function (m, hex) { return PALETTE['#' + hex.toLowerCase()] || m; })
      .replace(/(rgba?\(\s*)(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/gi, function (m, pre, r, g, b) {
        const n = RGB[r + ',' + g + ',' + b];
        return n ? pre + n : m;
      });
  }
  window.SSFB_remapColor = remapColor;

  // ── Settings: CRT scanlines (default on) and chiptune sound (default off) ─
  function loadSetting(key, dflt) { try { const v = localStorage.getItem('ssfb8-' + key); return v === null ? dflt : v === '1'; } catch (e) { return dflt; } }
  function saveSetting(key, val) { try { localStorage.setItem('ssfb8-' + key, val ? '1' : '0'); } catch (e) { /* storage blocked */ } }
  // Applied immediately (this script runs at the top of <body>) so the
  // scanlines never flash in after the page has painted.
  document.documentElement.classList.toggle('crt', loadSetting('crt', true));
  let soundOn = loadSetting('sound', false);
  let audio = null;
  function blip(freq, ms) {
    if (!soundOn) return;
    try {
      audio = audio || new (window.AudioContext || window.webkitAudioContext)();
      const o = audio.createOscillator(), g = audio.createGain(), t = (ms || 70) / 1000;
      o.type = 'square';
      o.frequency.value = freq || 660;
      g.gain.setValueAtTime(0.05, audio.currentTime);
      g.gain.exponentialRampToValueAtTime(0.0001, audio.currentTime + t);
      o.connect(g); g.connect(audio.destination);
      o.start(); o.stop(audio.currentTime + t);
    } catch (e) { /* no audio */ }
  }
  function coin() { blip(988, 60); setTimeout(function () { blip(1319, 160); }, 70); }
  window.SSFB_sound = { blip: blip, coin: coin };

  // ── <head> extras every page gets ────────────────────────────────────────
  (function injectHead() {
    function addOnce(selector, tag, attrs) {
      if (document.querySelector(selector)) return;
      const el = document.createElement(tag);
      Object.keys(attrs).forEach(function (k) { el.setAttribute(k, attrs[k]); });
      document.head.appendChild(el);
    }
    addOnce('link[rel="icon"][sizes="32x32"]',   'link', { rel: 'icon', type: 'image/png', sizes: '32x32',   href: ASSET_BASE + 'icon-32.png' });
    addOnce('link[rel="icon"][sizes="192x192"]', 'link', { rel: 'icon', type: 'image/png', sizes: '192x192', href: ASSET_BASE + 'icon-192.png' });
    addOnce('link[rel="apple-touch-icon"]',      'link', { rel: 'apple-touch-icon', sizes: '192x192', href: ASSET_BASE + 'icon-192.png' });
    addOnce('link[rel="manifest"]',              'link', { rel: 'manifest', href: ASSET_BASE + 'manifest.json' });
    addOnce('meta[name="theme-color"]',          'meta', { name: 'theme-color', content: '#000000' });
    // Pixel fonts. Pages stamped by patch_page_heads.py already request these
    // in <head>; this covers any page that was not.
    addOnce('link[href*="family=Press+Start+2P"]', 'link', {
      rel: 'stylesheet', href: 'https://fonts.googleapis.com/css2?family=Press+Start+2P&family=VT323&display=swap',
    });

    // Site-wide column sorting. Loaded here so every page picks it up without
    // its own <script> tag; it no-ops on tables a page already sorts itself.
    if (!document.querySelector('script[data-table-sort]')) {
      const ts = document.createElement('script');
      ts.src = ASSET_BASE + 'table-sort.js';
      ts.defer = true;
      ts.setAttribute('data-table-sort', '');
      document.head.appendChild(ts);
    }

    // Speculation Rules: Chrome/Edge start loading a page the moment you hover
    // (or touch) a link to it, so the click feels instant. Pages that poll
    // or write data are only prefetched (HTML downloaded, no scripts run) so a
    // hover never fires their API calls. Unsupported browsers ignore this.
    const NO_PRERENDER = ['/championship_hub_2026.html', '/live_standings_2026.html', '/daily_game.html'];
    if (window.HTMLScriptElement && HTMLScriptElement.supports &&
        HTMLScriptElement.supports('speculationrules') &&
        !document.querySelector('script[type="speculationrules"]')) {
      const rules = document.createElement('script');
      rules.type = 'speculationrules';
      rules.textContent = JSON.stringify({
        prerender: [{
          where: { and: [{ href_matches: '/*' }, { not: { href_matches: NO_PRERENDER } }] },
          eagerness: 'moderate',
        }],
        prefetch: [{
          where: { href_matches: NO_PRERENDER },
          eagerness: 'moderate',
        }],
      });
      document.head.appendChild(rules);
    }
  })();

  // ── Pixel charts ─────────────────────────────────────────────────────────
  // Every page's Chart.js charts get the 8-bit treatment without touching
  // their code: pixel fonts, square points, straight line segments, square
  // bars, a black tooltip with a gold frame, and palette-mapped colours.
  (function pixelCharts() {
    let done = false;
    function apply() {
      const C = window.Chart;
      if (done || !C || !C.defaults) return;
      done = true;
      const D = C.defaults;
      D.font.family = "'VT323', 'Courier New', monospace";
      D.font.size = 16;
      D.color = '#9a9ac8';
      D.borderColor = 'rgba(92, 92, 148, 0.35)';
      D.elements.point.pointStyle = 'rect';
      D.elements.line.tension = 0;
      D.elements.line.borderJoinStyle = 'miter';
      D.elements.bar.borderRadius = 0;
      const tip = D.plugins.tooltip;
      tip.backgroundColor = '#000000';
      tip.borderColor = '#ffd23f';
      tip.borderWidth = 2;
      tip.cornerRadius = 0;
      tip.padding = 10;
      tip.titleFont = { family: "'Press Start 2P', monospace", size: 9 };
      tip.bodyFont = { family: "'VT323', monospace", size: 18 };
      tip.caretSize = 0;
      D.plugins.legend.labels.boxWidth = 10;
      D.plugins.legend.labels.boxHeight = 10;

      const KEYS = ['borderColor', 'backgroundColor', 'pointBackgroundColor', 'pointBorderColor',
        'hoverBackgroundColor', 'hoverBorderColor', 'pointHoverBackgroundColor', 'pointHoverBorderColor'];
      C.register({
        id: 'ssfbPixelize',
        beforeUpdate: function (chart) {
          (chart.data.datasets || []).forEach(function (ds) {
            KEYS.forEach(function (k) {
              if (typeof ds[k] === 'string') ds[k] = remapColor(ds[k]);
              else if (Array.isArray(ds[k])) ds[k] = ds[k].map(remapColor);
            });
            if (ds.tension) ds.tension = 0;
            if (ds.cubicInterpolationMode) ds.cubicInterpolationMode = 'default';
            if (ds.pointStyle === undefined || ds.pointStyle === 'circle') ds.pointStyle = 'rect';
            if (ds.borderRadius) ds.borderRadius = 0;
          });
          const o = chart.options || {};
          if (o.plugins && o.plugins.tooltip) o.plugins.tooltip.cornerRadius = 0;
          // Legend markers: squares, not circles.
          if (o.plugins && o.plugins.legend && o.plugins.legend.labels) o.plugins.legend.labels.pointStyle = 'rect';
        },
      });
      // Canvas text is drawn once with whatever font is loaded at the time,
      // so redraw every chart after the pixel fonts arrive.
      if (document.fonts && document.fonts.ready) {
        document.fonts.ready.then(function () {
          Object.values(C.instances || {}).forEach(function (ch) { try { ch.update('none'); } catch (e) { /* ignore */ } });
        });
      }
    }
    apply();
    if (!done) {
      document.addEventListener('DOMContentLoaded', apply);
      window.addEventListener('load', apply);
    }
  })();

  // ── Styles ───────────────────────────────────────────────────────────────
  const style = document.createElement('style');
  style.textContent = `
    /* ── HUD bar ─────────────────────────────────────────────────────────── */
    /* The wrapper div is what sticks (the bar's own parent is exactly as tall
       as the bar, so a sticky .ss-nav would have nowhere to stick). Height is
       fixed at 52px: several pages pin their own bars at top:52px. */
    #nav { position: sticky; top: 0; z-index: 1000; }
    .ss-nav {
      position: relative;
      view-transition-name: ss-nav;
      background: #000;
      border-bottom: 4px solid #2b2e7a;
      padding: 0 16px;
      font-family: 'Press Start 2P', 'Courier New', monospace;
      font-size: 9px;
    }
    .ss-nav-inner {
      max-width: 1400px;
      margin: 0 auto;
      display: flex;
      align-items: center;
      height: 48px;
      gap: 0;
    }
    .ss-nav-brand {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 12px;
      color: #ffd23f;
      text-decoration: none;
      margin-right: 22px;
      white-space: nowrap;
      flex-shrink: 0;
    }
    .ss-nav-brand svg { width: 20px; height: 20px; }
    .ss-nav-brand:hover { color: #ffffff; }

    .ss-skip {
      position: absolute; left: 12px; top: -60px; z-index: 2;
      background: #ffd23f; color: #000; padding: 10px 14px;
      text-decoration: none; transition: top 0.15s steps(2);
    }
    .ss-skip:focus { top: 8px; }

    /* ── Desktop links ───────────────────────────────────────────────────── */
    .ss-nav-links { display: flex; align-items: center; list-style: none; margin: 0; padding: 0; height: 100%; }
    .ss-nav-item { position: relative; height: 100%; display: flex; align-items: center; }
    .champ-item { color: #ffd23f !important; }
    .champ-item:hover { color: #ffffff !important; }

    .ss-nav-link {
      position: relative;
      color: #9a9ac8;
      text-decoration: none;
      padding: 0 12px 0 22px;
      font: inherit;
      height: 100%;
      display: flex;
      align-items: center;
      white-space: nowrap;
      cursor: pointer;
      user-select: none;
      background: none;
      border: 0;
    }
    /* The ▶ menu cursor appears beside whatever is hovered or current. */
    .ss-nav-link::before {
      content: '\\25B6';
      position: absolute;
      left: 8px;
      color: #ffd23f;
      opacity: 0;
    }
    .ss-nav-link:hover, .ss-nav-item.open > .ss-nav-link { color: #ffffff; }
    .ss-nav-link:hover::before, .ss-nav-item.open > .ss-nav-link::before,
    .ss-nav-link.active::before { opacity: 1; }
    .ss-nav-link.active { color: #ffd23f; }
    .ss-nav-link.has-dropdown::after { content: '\\25BE'; margin-left: 6px; }
    .ss-nav-link:focus-visible, .ss-dropdown a:focus-visible, .ss-hud-btn:focus-visible, .ss-hamburger:focus-visible {
      outline: 3px solid #ffd23f; outline-offset: -3px;
    }

    /* Dropdowns: hover (mouse), click/tap, or keyboard. A notched pixel box. */
    .ss-dropdown {
      display: none;
      position: absolute;
      top: calc(100% + 4px);
      left: 4px;
      background: #1f2160;
      padding: 8px 4px;
      min-width: 250px;
      box-shadow: 0 -4px 0 0 #000, 0 4px 0 0 #000, -4px 0 0 0 #000, 4px 0 0 0 #000, 8px 8px 0 0 rgba(0,0,0,.5);
    }
    .ss-nav-item.open > .ss-dropdown { display: block; }
    @media (hover: hover) { .ss-nav-item:hover > .ss-dropdown { display: block; } }
    /* Invisible bridge across the 4px gap so the menu does not close mid-move. */
    .ss-dropdown::before { content: ''; position: absolute; left: 0; right: 0; top: -12px; height: 12px; }
    .ss-dropdown a {
      position: relative;
      display: block;
      padding: 10px 12px 10px 26px;
      color: #f4f4f4;
      text-decoration: none;
      line-height: 1.5;
    }
    .ss-dropdown a::before { content: '\\25B6'; position: absolute; left: 10px; color: #ffd23f; opacity: 0; }
    .ss-dropdown a:hover { background: #2b2e7a; color: #ffd23f; }
    .ss-dropdown a:hover::before, .ss-dropdown a.active::before { opacity: 1; }
    .ss-dropdown a.active { color: #ffd23f; }
    .ss-divider { height: 4px; margin: 6px 8px; background: repeating-linear-gradient(90deg, #5c5c94 0 4px, transparent 4px 8px); }
    .ss-label { padding: 10px 12px 4px; font-size: 7px; color: #9a9ac8; letter-spacing: 1px; }

    /* ── HUD toggles ─────────────────────────────────────────────────────── */
    .ss-hud { display: flex; gap: 8px; margin-left: auto; padding-left: 12px; flex-shrink: 0; }
    .ss-hud-btn {
      font: inherit;
      font-size: 8px;
      color: #f4f4f4;
      background: #1f2160;
      border: 0;
      padding: 8px 8px;
      cursor: pointer;
      white-space: nowrap;
      box-shadow: inset -2px -2px 0 rgba(0,0,0,.45), inset 2px 2px 0 rgba(255,255,255,.18);
    }
    .ss-hud-btn[aria-pressed="true"] { background: #1f9e33; }
    .ss-hud-btn:active { box-shadow: inset 2px 2px 0 rgba(0,0,0,.45); }

    /* ── Hamburger (narrow screens) ──────────────────────────────────────── */
    .ss-hamburger {
      display: none;
      margin-left: 10px;
      background: #1f2160;
      border: 0;
      cursor: pointer;
      padding: 9px 8px;
      color: #f4f4f4;
      flex-direction: column;
      gap: 4px;
      box-shadow: inset -2px -2px 0 rgba(0,0,0,.45), inset 2px 2px 0 rgba(255,255,255,.18);
    }
    .ss-hamburger span { display: block; width: 18px; height: 3px; background: currentColor; }
    .ss-hamburger.open { background: #1f9e33; }

    /* ── Mobile panel ────────────────────────────────────────────────────── */
    .ss-mobile-menu {
      display: none;
      background: #000;
      border-top: 4px solid #2b2e7a;
      margin: 0 -16px;
      padding: 10px 0 18px;
      max-height: calc(100vh - 52px);
      max-height: calc(100dvh - 52px);
      overflow-y: auto;
      overscroll-behavior: contain;
    }
    .ss-mobile-menu.open { display: block; }
    .ss-mob-section-btn, .ss-mob-link {
      position: relative;
      width: 100%;
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: none;
      border: 0;
      text-align: left;
      padding: 14px 20px 14px 34px;
      color: #9a9ac8;
      font: inherit;
      font-size: 10px;
      line-height: 1.5;
      text-decoration: none;
      cursor: pointer;
    }
    .ss-mob-section-btn::before, .ss-mob-link::before { content: '\\25B6'; position: absolute; left: 14px; color: #ffd23f; opacity: 0; }
    .ss-mob-section-btn:hover, .ss-mob-link:hover { color: #ffffff; }
    .ss-mob-section-btn.active, .ss-mob-link.active { color: #ffd23f; }
    .ss-mob-link.active::before, .ss-mob-section-btn[aria-expanded="true"]::before { opacity: 1; }
    .ss-mob-arrow { font-size: 8px; }
    .ss-mob-section-btn[aria-expanded="true"] .ss-mob-arrow { transform: rotate(180deg); }
    .ss-mob-sub { display: none; background: #0d0d2b; padding: 6px 0; }
    .ss-mob-sub.open { display: block; }
    .ss-mob-sub a { position: relative; display: block; padding: 12px 20px 12px 48px; color: #f4f4f4; text-decoration: none; font-size: 9px; line-height: 1.5; }
    .ss-mob-sub a::before { content: '\\25B6'; position: absolute; left: 30px; color: #ffd23f; opacity: 0; }
    .ss-mob-sub a.active { color: #ffd23f; }
    .ss-mob-sub a.active::before, .ss-mob-sub a:hover::before { opacity: 1; }
    .ss-mob-sub .ss-label { padding: 10px 20px 4px 48px; }

    /* ── Responsive switch ───────────────────────────────────────────────── */
    @media (max-width: 1000px) {
      .ss-nav-links { display: none; }
      .ss-hamburger { display: flex; }
    }
    @media (min-width: 1001px) { .ss-mobile-menu { display: none !important; } }
    @media (max-width: 420px) {
      .ss-nav-brand span { display: none; }
      .ss-hud-btn { font-size: 7px; padding: 8px 6px; }
    }

    /* ── Yahoo Fantasy attribution ───────────────────────────────────────── */
    /* Required by the Yahoo Fantasy Sports API Access and Use Agreement.
       The logo must stay unmodified: no recolor, rotation, effects, or change
       to proportion. The white plate is a background behind it, not a change to
       it - the official SVG draws "fantasy" in black, which would be illegible
       on the dark page. */
    .yahoo-attribution {
      margin: 64px 0 0;
      padding: 36px 24px 28px;
      border-top: 4px solid #2b2e7a;
      background: #000;
      text-align: center;
    }
    .yahoo-attribution .logo-plate { display: inline-block; background: #ffffff; padding: 14px 22px; margin-bottom: 14px; line-height: 0; }
    .yahoo-attribution img { display: block; width: 240px; max-width: 100%; height: auto; }
    .yahoo-attribution p { color: #9a9ac8; font-size: 20px; margin: 0; }
    .yahoo-attribution a { color: #a98cff; }
    .yahoo-attribution .credits { margin-top: 16px; font-family: 'Press Start 2P', monospace; font-size: 8px; color: #5c5c94; }
  `;
  document.head.appendChild(style);

  // ── Yahoo Fantasy attribution footer ──────────────────────────────────────
  // The API Access and Use Agreement requires attribution in the footer of
  // EVERY page displaying Yahoo Fantasy Information, with a hyperlink to an
  // official Yahoo Fantasy page. Injected centrally so no page can be missed.
  // Runs before the #nav guard below so a page without a nav still gets it.
  (function injectAttribution() {
    function build() {
      if (document.querySelector('.yahoo-attribution')) return;
      const footer = document.createElement('footer');
      footer.className = 'yahoo-attribution';
      footer.innerHTML =
        '<a class="logo-plate" href="https://baseball.fantasysports.yahoo.com/" ' +
        'target="_blank" rel="noopener noreferrer">' +
        '<img src="' + ASSET_BASE + 'yahoo-fantasy-logo.svg" alt="Yahoo Fantasy" ' +
        'width="240" height="33" loading="lazy" decoding="async"></a>' +
        '<p>Fantasy data provided by ' +
        '<a href="https://baseball.fantasysports.yahoo.com/" target="_blank" ' +
        'rel="noopener noreferrer">Yahoo Fantasy</a></p>' +
        '<p class="credits">&copy; 2007&ndash;2026 SUMMERTIME SADNESS LEAGUE</p>';
      document.body.appendChild(footer);
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build);
    else build();
  })();

  // ── Loose tables ─────────────────────────────────────────────────────────
  // A table dropped straight into .container (no .table-scroll wrapper) is
  // wider than a phone screen and drags the whole page sideways. Wrap each one
  // so it scrolls inside its own box instead.
  (function wrapLooseTables() {
    function wrap() {
      document.querySelectorAll('.container > table').forEach(function (t) {
        const box = document.createElement('div');
        box.className = 'table-scroll';
        t.parentNode.insertBefore(box, t);
        box.appendChild(t);
      });
    }
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', wrap);
    else wrap();
  })();

  // ── Google Analytics 4 ───────────────────────────────────────────────────
  // gtag.js understands prerendering: a page loaded on hover is not counted
  // as a view until the visitor actually opens it.
  (function () {
    const GA_ID = 'G-P2P9GH1FGQ';
    const s = document.createElement('script');
    s.async = true;
    s.src = 'https://www.googletagmanager.com/gtag/js?id=' + GA_ID;
    document.head.appendChild(s);
    window.dataLayer = window.dataLayer || [];
    function gtag() { window.dataLayer.push(arguments); }
    window.gtag = gtag;
    gtag('js', new Date());
    gtag('config', GA_ID);
  })();

  const nav = document.getElementById('nav');
  if (!nav) return;

  // ── Menu definition ──────────────────────────────────────────────────────
  // The ONE place pages are listed. Desktop dropdowns and the mobile
  // accordion are both built from this, so a new page is a one-line change.
  // `short` is the desktop label (the pixel font is wide); mobile uses `title`.
  const link = function (href, text, cls, short) { return { href: href, text: text, cls: cls || '', short: short }; };
  const label = function (text) { return { label: text }; };
  const DIVIDER = { divider: true };

  const MENU = [
    { title: '2026 SEASON', short: '2026', id: 's2026', items: [
      link('live_standings_2026.html',      'LIVE STANDINGS'),
      link('season_trends_2026.html',       'SEASON TRENDS'),
      link('luck_analysis_2026.html',       'LUCK ANALYSIS'),
      link('championship_hub_2026.html',    '&#127942; CHAMPIONSHIP HUB', 'champ-item'),
      link('playoff_hub_2026.html',         'PLAYOFF HUB'),
      link('playoff_scenarios_2026.html',   'PLAYOFF SCENARIOS'),
      link('superlatives_2026.html',        'SUPERLATIVES'),
      link('trade_analyzer.html',           'TRADE ANALYZER'),
      link('trade_grades_2026.html',        'TRADE GRADES'),
      link('positional_strength_2026.html', 'POSITIONAL STRENGTH'),
      DIVIDER,
      link('draft_picks_2026.html',         'DRAFT CAPITAL'),
      link('draft_results_2026.html',       'DRAFT RESULTS'),
      link('keepers_2026.html',             'KEEPERS'),
    ] },
    { title: 'PAST SEASONS', short: 'PAST', id: 'syoy', items: [
      label('2025 SEASON'),
      link('season_trends_2025.html', 'SEASON TRENDS'),
      link('draft_picks_2025.html',   'DRAFT CAPITAL'),
      link('luck_analysis_2025.html', 'LUCK &amp; MATCHUPS'),
      DIVIDER,
      label('EARLIER'),
      link('season_trends_2024.html', '2024 SEASON TRENDS'),
      link('season_trends_2023.html', '2023 SEASON TRENDS'),
      link('season_trends_2022.html', '2022 SEASON TRENDS'),
    ] },
    link('all_time_rankings.html', 'ALL-TIME RECORDS', '', 'RECORDS'),
    link('h2h_records.html',       'H2H RECORDS',      '', 'H2H'),
    link('manager_profiles.html',  'MANAGER PROFILES', '', 'MANAGERS'),
    link('daily_game.html',        'DAILY GAME',       '', 'DAILY GAME'),
  ];

  // GitHub Pages also serves "/h2h_records" without the extension.
  let path = window.location.pathname.split('/').pop() || 'index.html';
  if (path.indexOf('.') === -1) path += '.html';

  const isCurrent = function (item) { return item.href === path; };
  const sectionHasCurrent = function (sec) { return sec.items.some(isCurrent); };

  function anchor(item, baseCls, text) {
    const cur = isCurrent(item);
    const cls = [baseCls, item.cls, cur ? 'active' : ''].filter(Boolean).join(' ');
    return '<a href="' + item.href + '"' + (cls ? ' class="' + cls + '"' : '') +
      (cur ? ' aria-current="page"' : '') + ' data-blip>' + (text || item.text) + '</a>';
  }
  function subItems(items) {
    return items.map(function (it) {
      if (it.divider) return '<div class="ss-divider" role="separator"></div>';
      if (it.label) return '<div class="ss-label">' + it.label + '</div>';
      return anchor(it, '');
    }).join('');
  }

  const desktop = MENU.map(function (entry) {
    if (!entry.items) return '<li class="ss-nav-item">' + anchor(entry, 'ss-nav-link', entry.short) + '</li>';
    const act = sectionHasCurrent(entry) ? ' active' : '';
    return '<li class="ss-nav-item has-menu">' +
      '<button type="button" class="ss-nav-link has-dropdown' + act + '" aria-expanded="false" aria-controls="dd-' + entry.id + '" data-blip>' +
      entry.short + '</button>' +
      '<div class="ss-dropdown" id="dd-' + entry.id + '">' + subItems(entry.items) + '</div></li>';
  }).join('');

  const mobile = MENU.map(function (entry) {
    if (!entry.items) return anchor(entry, 'ss-mob-link');
    const here = sectionHasCurrent(entry);
    return '<button type="button" class="ss-mob-section-btn' + (here ? ' active' : '') + '" ' +
      'aria-expanded="' + here + '" aria-controls="mob-' + entry.id + '" data-blip>' +
      entry.title + ' <span class="ss-mob-arrow" aria-hidden="true">&#9660;</span></button>' +
      '<div class="ss-mob-sub' + (here ? ' open' : '') + '" id="mob-' + entry.id + '">' + subItems(entry.items) + '</div>';
  }).join('');

  // 12x12 pixel baseball for the brand mark.
  const BALL = ['....WWWW....', '..WWWWWWWW..', '.WRWWWWWWRW.', '.WWRWWWWRWW.', 'WWRWWWWWWRWS', 'WWWRWWWWRWWS',
    'WWRWWWWWWRWS', 'WWWRWWWWRWWS', '.WRWWWWWWRS.', '.WWRWWWWRSS.', '..WWWWSSSS..', '....SSSS....'];
  const BC = { W: '#f4f4f4', R: '#ff3b3b', S: '#9a9ac8' };
  let ball = '';
  BALL.forEach(function (row, y) { for (let x = 0; x < 12; x++) if (BC[row[x]]) ball += '<rect x="' + x + '" y="' + y + '" width="1" height="1" fill="' + BC[row[x]] + '"/>'; });

  nav.innerHTML =
    '<nav class="ss-nav" aria-label="Main">' +
      '<a class="ss-skip" href="#">SKIP TO CONTENT</a>' +
      '<div class="ss-nav-inner">' +
        '<a href="index.html" class="ss-nav-brand" data-blip' + (path === 'index.html' ? ' aria-current="page"' : '') + '>' +
          '<svg class="pix" viewBox="0 0 12 12" aria-hidden="true">' + ball + '</svg><span>SSFB</span></a>' +
        '<ul class="ss-nav-links">' + desktop + '</ul>' +
        '<div class="ss-hud">' +
          '<button type="button" class="ss-hud-btn" data-toggle="sound" aria-pressed="false">SOUND:OFF</button>' +
          '<button type="button" class="ss-hud-btn" data-toggle="crt" aria-pressed="false">CRT:OFF</button>' +
        '</div>' +
        '<button type="button" class="ss-hamburger" id="ssHamburger" aria-label="Open menu" aria-expanded="false" aria-controls="ssMobileMenu">' +
          '<span></span><span></span><span></span>' +
        '</button>' +
      '</div>' +
      '<div class="ss-mobile-menu" id="ssMobileMenu">' + mobile + '</div>' +
    '</nav>';

  // ── Behaviour ────────────────────────────────────────────────────────────
  const menuItems = Array.prototype.slice.call(nav.querySelectorAll('.ss-nav-item.has-menu'));
  const hamburger = document.getElementById('ssHamburger');
  const mobileMenu = document.getElementById('ssMobileMenu');

  function setDropdown(item, open) {
    item.classList.toggle('open', open);
    item.querySelector('.ss-nav-link').setAttribute('aria-expanded', String(open));
  }
  function closeDropdowns(except) {
    menuItems.forEach(function (it) { if (it !== except) setDropdown(it, false); });
  }
  function setMobile(open) {
    hamburger.classList.toggle('open', open);
    mobileMenu.classList.toggle('open', open);
    hamburger.setAttribute('aria-expanded', String(open));
    hamburger.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
  }

  menuItems.forEach(function (item) {
    item.querySelector('.ss-nav-link').addEventListener('click', function () {
      const open = !item.classList.contains('open');
      closeDropdowns(item);
      setDropdown(item, open);
    });
    item.addEventListener('mouseleave', function () { setDropdown(item, false); });
    item.addEventListener('focusout', function (e) {
      if (!item.contains(e.relatedTarget)) setDropdown(item, false);
    });
  });

  document.addEventListener('click', function (e) {
    if (!nav.contains(e.target)) closeDropdowns();
  });

  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    const openItem = nav.querySelector('.ss-nav-item.open');
    if (openItem) {
      setDropdown(openItem, false);
      openItem.querySelector('.ss-nav-link').focus();
    }
    if (mobileMenu.classList.contains('open')) {
      setMobile(false);
      hamburger.focus();
    }
  });

  hamburger.addEventListener('click', function () {
    setMobile(!mobileMenu.classList.contains('open'));
    blip(mobileMenu.classList.contains('open') ? 880 : 440, 60);
  });

  const mobButtons = Array.prototype.slice.call(nav.querySelectorAll('.ss-mob-section-btn'));
  mobButtons.forEach(function (btn) {
    btn.addEventListener('click', function () {
      const sub = document.getElementById(btn.getAttribute('aria-controls'));
      const wasOpen = sub.classList.contains('open');
      mobButtons.forEach(function (b) {
        b.setAttribute('aria-expanded', 'false');
        document.getElementById(b.getAttribute('aria-controls')).classList.remove('open');
      });
      if (!wasOpen) {
        sub.classList.add('open');
        btn.setAttribute('aria-expanded', 'true');
      }
    });
  });

  // HUD toggles.
  nav.querySelectorAll('[data-toggle]').forEach(function (btn) {
    const key = btn.getAttribute('data-toggle');
    const sync = function (on) {
      btn.setAttribute('aria-pressed', String(on));
      btn.textContent = key.toUpperCase() + ':' + (on ? 'ON' : 'OFF');
    };
    sync(key === 'sound' ? soundOn : document.documentElement.classList.contains('crt'));
    btn.addEventListener('click', function () {
      if (key === 'sound') { soundOn = !soundOn; saveSetting('sound', soundOn); sync(soundOn); if (soundOn) coin(); }
      else { const on = document.documentElement.classList.toggle('crt'); saveSetting('crt', on); sync(on); blip(440, 60); }
    });
  });

  // Menu blips on hover / focus / click, for anything marked data-blip.
  document.addEventListener('pointerover', function (e) { if (e.target.closest && e.target.closest('[data-blip]')) blip(660, 40); });
  document.addEventListener('focusin', function (e) { if (e.target.closest && e.target.closest('[data-blip]')) blip(660, 40); });
  document.addEventListener('click', function (e) { if (e.target.closest && e.target.closest('a[data-blip]')) blip(880, 90); });

  // Coming back with the Back button restores the page from cache exactly as
  // it was left, including an open menu. Start clean instead.
  window.addEventListener('pageshow', function (e) {
    if (e.persisted) { setMobile(false); closeDropdowns(); }
  });

  nav.querySelector('.ss-skip').addEventListener('click', function (e) {
    e.preventDefault();
    const target = document.querySelector('main, h1, .container');
    if (!target) return;
    if (!target.hasAttribute('tabindex')) target.setAttribute('tabindex', '-1');
    target.focus();
  });
})();
