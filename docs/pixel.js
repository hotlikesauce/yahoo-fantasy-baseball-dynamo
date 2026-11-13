/* Summertime Sadness - 8-bit pixel helpers (home + records pages).
   Pixel-art sprites, the title-screen stadium, arrow-key menus, and a loader that reads the real league records
   straight out of all_time_rankings.html so there is one source of data. */
(function () {
  'use strict';

  // ── Palette for sprite maps ──────────────────────────────────────────────
  const PAL = {
    K: '#000000', W: '#f4f4f4', S: '#9a9ac8', D: '#5c5c94', Y: '#ffd23f',
    O: '#ff8c1a', R: '#ff3b3b', G: '#2ecc40', g: '#1f9e33', C: '#3bd6ff',
    B: '#3b6bff', P: '#8a5cff', p: '#ff5ec8', N: '#c8763c', n: '#7a4320',
  };

  // 12x12 sprites. '.' is transparent.
  const SPRITES = {
    trophy: [
      '..YYYYYYYY..', 'YYYWYYYYYYYY', 'Y.YWYYYYYO.Y', 'Y.YWYYYYYO.Y',
      '.YYWYYYYYOY.', '..YYYYYYOO..', '...YYYYYO...', '.....YO.....',
      '.....YO.....', '....OOOO....', '...nnnnnn...', '...nnnnnn...',
    ],
    ball: [
      '....WWWW....', '..WWWWWWWW..', '.WRWWWWWWRW.', '.WWRWWWWRWW.',
      'WWRWWWWWWRWS', 'WWWRWWWWRWWS', 'WWRWWWWWWRWS', 'WWWRWWWWRWWS',
      '.WRWWWWWWRS.', '.WWRWWWWRSS.', '..WWWWSSSS..', '....SSSS....',
    ],
    chart: [
      'W...........', 'W.........GG', 'W.........Gg', 'W......CC.Gg',
      'W......CB.Gg', 'W...YY.CB.Gg', 'W...YO.CB.Gg', 'W.ppYO.CB.Gg',
      'W.pPYO.CB.Gg', 'W.pPYO.CB.Gg', 'WWWWWWWWWWWW', '............',
    ],
    dice: [
      '.WWWWWWWWWW.', 'WWWWWWWWWWWS', 'WWKKWWWWKKWS', 'WWKKWWWWKKWS',
      'WWWWWWWWWWWS', 'WWWWWKKWWWWS', 'WWWWWKKWWWWS', 'WWWWWWWWWWWS',
      'WWKKWWWWKKWS', 'WWKKWWWWKKWS', 'WSSSSSSSSSSS', '.SSSSSSSSSS.',
    ],
    swap: [
      '........C...', '........CC..', 'CCCCCCCCCCC.', 'CCCCCCCCCCCC',
      '........CC..', '........C...', '...O........', '..OO........',
      '.OOOOOOOOOOO', 'OOOOOOOOOOOO', '..OO........', '...O........',
    ],
    star: [
      '.....YY.....', '.....YY.....', '....YYYY....', 'YYYYYWYYYYYY',
      '.YYYYWYYYYY.', '..YYYYYYYO..', '...YYYYYO...', '..YYYYYYYO..',
      '..YYO..YYO..', '.YYO....YYO.', '.YO......YO.', '............',
    ],
    bracket: [
      'WWWW........', '...W........', '...WWWW.....', '...W..W.....',
      'WWWW..W.....', '......WWWYYY', 'WWWW..W..YYY', '...W..W.....',
      '...WWWW.....', '...W........', 'WWWW........', '............',
    ],
    fire: [
      '.....R......', '....RR......', '....RRR..R..', '...RRRR.RR..',
      '..RRORRRRR..', '..RROORRRR..', '.RROOOORRRR.', '.RROOYOORRR.',
      '.RROYYYOORR.', '..ROYYYYOR..', '..RROYYORR..', '...RRRRRR...',
    ],
    ice: [
      '.....C......', '...C.C.C....', '....CCC.....', '.C...C...C..',
      '..C..C..C...', 'CCCCCWCCCCC.', '..C..C..C...', '.C...C...C..',
      '....CCC.....', '...C.C.C....', '.....C......', '............',
    ],
    cap: [
      '...BBBBB....', '..BBBBBBB...', '..BBBBBBBBB.', '...NNNNN....',
      '...NKNKN....', '...NNNNN....', '....NNN.....', '..RRRRRRR...',
      '.RRRRRRRRR..', '.RRRWRRRRR..', '.NRRRRRRRN..', '..WW...WW...',
    ],
    bats: [
      'nn........nn', 'nnn......nnn', '.nnn....nnn.', '..nNn..nNn..',
      '...nNnnNn...', '....NNNN....', '....NNNN....', '...N.NN.N...',
      '..N......N..', '.W........W.', 'WW........WW', 'W..........W',
    ],
    book: [
      '.PPPPPPPPP..', '.PWWWWWWWPK.', '.PWKKKKKWPK.', '.PWWWWWWWPK.',
      '.PWKKKKWWPK.', '.PWWWWWWWPK.', '.PWKKKKKWPK.', '.PWWWWWWWPK.',
      '.PPPPPPPPPK.', '..KKKKKKKKK.', '............', '............',
    ],
    diamond: [
      '.....WW.....', '....NNNN....', '...NNggNN...', '..NNggggNN..',
      '.NNggggggNN.', 'WNggggNgggNW', 'WNgggNNgggNW', '.NNggggggNN.',
      '..NNggggNN..', '...NNggNN...', '....NNNN....', '.....WW.....',
    ],
    coin: [
      '....YYYY....', '..YYYYYYYY..', '.YYWWYYYYOY.', '.YWYYYYYYOY.',
      'YYWYYOOYYYOY', 'YYWYYOYYYYOY', 'YYYYYOYYYYOY', 'YYYYYOOYYYOY',
      '.YYYYYYYYOY.', '.YOYYYYYOOY.', '..YYOOOOYY..', '....YYYY....',
    ],
  };

  function sprite(name, size) {
    const map = SPRITES[name];
    if (!map) return '';
    const h = map.length, w = map[0].length, s = size || 48;
    let rects = '';
    for (let y = 0; y < h; y++) {
      for (let x = 0; x < w; x++) {
        const c = PAL[map[y][x]];
        if (c) rects += '<rect x="' + x + '" y="' + y + '" width="1" height="1" fill="' + c + '"/>';
      }
    }
    return '<svg class="pix" viewBox="0 0 ' + w + ' ' + h + '" width="' + s + '" height="' + s +
      '" aria-hidden="true">' + rects + '</svg>';
  }

  // Replace every <i data-sprite="name" data-size="32"></i> with its SVG.
  function paintSprites(root) {
    (root || document).querySelectorAll('[data-sprite]').forEach(function (el) {
      el.outerHTML = sprite(el.getAttribute('data-sprite'), +el.getAttribute('data-size') || 48);
    });
  }

  // ── Title-screen stadium, drawn on a 160x60 pixel grid ───────────────────
  function stadium() {
    const W = 160, H = 60;
    const out = [];
    const r = function (x, y, w, h, c) { out.push('<rect x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" fill="' + c + '"/>'); };
    // Deterministic pseudo-random so the crowd is the same on every load.
    let seed = 2007;
    const rnd = function () { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };

    // Night sky in stepped bands.
    [['#07071a', 0, 10], ['#0d0d2b', 10, 7], ['#16163f', 17, 5], ['#22184a', 22, 4], ['#3a1f5c', 26, 3]]
      .forEach(function (b) { r(0, b[1], W, b[2], b[0]); });
    // Stars.
    for (let i = 0; i < 46; i++) {
      const x = Math.floor(rnd() * W), y = Math.floor(rnd() * 24);
      r(x, y, 1, 1, rnd() > 0.82 ? '#ffd23f' : '#9a9ac8');
    }
    // Moon with a crater.
    r(134, 5, 6, 6, '#f4f4d0'); r(133, 6, 8, 4, '#f4f4d0'); r(136, 7, 2, 2, '#c8c8a8');
    // Light towers.
    [[10, '#ffd23f'], [146, '#ffd23f']].forEach(function (t) {
      r(t[0] + 2, 8, 2, 22, '#5c5c94');
      r(t[0] - 1, 4, 8, 5, '#000000');
      for (let i = 0; i < 3; i++) for (let j = 0; j < 2; j++) r(t[0] + i * 2, 5 + j * 2, 1, 1, t[1]);
      r(t[0] - 3, 9, 12, 1, 'rgba(255,210,63,0.25)');
    });
    // Stands with crowd.
    r(0, 29, W, 11, '#16163f');
    for (let y = 30; y < 39; y += 2) {
      for (let x = (y % 4 === 0 ? 0 : 1); x < W; x += 2) {
        const v = rnd();
        r(x, y, 1, 1, v > 0.9 ? '#ff3b3b' : v > 0.8 ? '#3bd6ff' : v > 0.7 ? '#ffd23f' : v > 0.55 ? '#f4f4f4' : '#2b2e7a');
      }
    }
    // Outfield wall with yellow top line.
    r(0, 40, W, 3, '#1f6f3a'); r(0, 40, W, 1, '#ffd23f');
    // Grass with mowing stripes.
    for (let y = 43, i = 0; y < H; y += 3, i++) r(0, y, W, 3, i % 2 ? '#2ecc40' : '#27b337');
    // Infield dirt diamond (home at bottom centre).
    const cx = 80, top = 44, bot = 59, mid = (top + bot) / 2;
    for (let y = top; y <= bot; y++) {
      const half = Math.round((7.5 - Math.abs(y - mid)) * 2.6);
      if (half > 0) r(cx - half, y, half * 2, 1, '#c8763c');
      const inner = Math.round((5.5 - Math.abs(y - mid)) * 2.6);
      if (inner > 0) r(cx - inner, y, inner * 2, 1, (y % 3 ? '#2ecc40' : '#27b337'));
    }
    // Bases and mound.
    r(cx - 1, top, 2, 2, '#f4f4f4'); r(cx - 20, mid, 2, 2, '#f4f4f4'); r(cx + 18, mid, 2, 2, '#f4f4f4');
    r(cx - 1, bot - 1, 2, 2, '#f4f4f4'); r(cx - 2, mid, 4, 2, '#d9935a');
    // A home run ball arcing over the wall, with its trail.
    [[96, 50], [104, 42], [112, 35], [120, 30], [128, 27]].forEach(function (p, i) { r(p[0], p[1], 1, 1, 'rgba(244,244,244,' + (0.2 + i * 0.15) + ')'); });
    r(133, 24, 2, 2, '#f4f4f4');

    return '<svg class="pix title-scene" viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="xMidYMid slice" role="img" ' +
      'aria-label="A pixel-art ballpark at night">' + out.join('') + '</svg>';
  }

  // Sound lives in nav.js (it owns the HUD SOUND toggle); delegate to it.
  function blip(f, ms) { if (window.SSFB_sound) window.SSFB_sound.blip(f, ms); }
  function coin() { if (window.SSFB_sound) window.SSFB_sound.coin(); }

  // Arrow keys move the cursor around a grid of menu tiles, like a stage select.
  function gridNav(grid) {
    if (!grid) return;
    grid.addEventListener('keydown', function (e) {
      const keys = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: 'up', ArrowDown: 'down' };
      if (!(e.key in keys)) return;
      const items = Array.prototype.slice.call(grid.querySelectorAll('a'));
      const i = items.indexOf(document.activeElement);
      if (i < 0) return;
      const cols = getComputedStyle(grid).gridTemplateColumns.split(' ').length;
      let j = i;
      if (keys[e.key] === 'up') j = i - cols;
      else if (keys[e.key] === 'down') j = i + cols;
      else j = i + keys[e.key];
      if (j >= 0 && j < items.length) { e.preventDefault(); items[j].focus(); }
    });
  }

  // ── Data helpers ─────────────────────────────────────────────────────────
  // Reads every <h3> + <table> pair out of the existing All-Time Records page.
  // Pass a Document to read one already in hand (the records page reads itself).
  function parseRecords(doc) {
      const out = {};
      doc.querySelectorAll('h3').forEach(function (h) {
        let t = h.nextElementSibling;
        while (t && t.tagName !== 'TABLE' && t.tagName !== 'H3') t = t.nextElementSibling;
        if (!t || t.tagName !== 'TABLE') return;
        const rows = Array.prototype.slice.call(t.querySelectorAll('tr')).filter(function (tr) { return tr.querySelector('td'); });
        out[h.textContent.trim()] = rows.map(function (tr) {
          return Array.prototype.map.call(tr.querySelectorAll('td'), function (td) {
            return { text: td.textContent.replace(/\u{1F3C6}/gu, '').replace(/\s+x\d+$/, '').trim(), champ: !!td.querySelector('.champ'), raw: td.textContent.trim() };
          });
        });
      });
      return out;
  }
  function loadRecords() {
    return fetch('all_time_rankings.html').then(function (r) {
      if (!r.ok) throw new Error('HTTP ' + r.status);
      return r.text();
    }).then(function (html) { return parseRecords(new DOMParser().parseFromString(html, 'text/html')); });
  }

  function placeLabel(n) {
    const s = n === 1 ? 'ST' : n === 2 ? 'ND' : n === 3 ? 'RD' : 'TH';
    const cls = n === 1 ? 'p1' : n === 2 ? 'p2' : n === 3 ? 'p3' : 'pn';
    return '<span class="place ' + cls + '">' + n + s + '</span>';
  }
  function meter(value, max, segs) {
    segs = segs || 20;
    const on = Math.max(0, Math.min(segs, Math.round((value / max) * segs)));
    let h = '<span class="meter' + (on === segs ? ' full' : '') + '" role="img" aria-label="' + Math.round(value) + ' of ' + Math.round(max) + '">';
    for (let i = 0; i < segs; i++) h += '<i' + (i < on ? ' class="on"' : '') + '></i>';
    return h + '</span>';
  }
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  window.SSFB8 = {
    sprite: sprite, paintSprites: paintSprites, stadium: stadium, blip: blip, coin: coin,
    gridNav: gridNav, loadRecords: loadRecords, parseRecords: parseRecords, placeLabel: placeLabel, meter: meter, esc: esc,
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { paintSprites(); });
  else paintSprites();
})();
