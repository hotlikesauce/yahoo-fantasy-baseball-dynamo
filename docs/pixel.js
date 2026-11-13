/* Summertime Sadness - 8-bit pixel helpers (home + records pages).
   Shaded pixel-art sprites, the title-screen ballpark, arrow-key menus, and a loader that reads the real league records
   straight out of all_time_rankings.html so there is one source of data. */
(function () {
  'use strict';

  // ── Sprites ──────────────────────────────────────────────────────────────
  // Icons are the system's shaded colour emoji, drawn into a tiny canvas at
  // half the display size and scaled back up with hard pixel edges. That gives
  // detailed, softly shaded pixel art (2px pixels) instead of flat 12x12 maps.
  const EMOJI = {
    trophy: '\u{1F3C6}', ball: '⚾', chart: '\u{1F4C8}', dice: '\u{1F3B2}', swap: '\u{1F504}',
    star: '⭐', bracket: '\u{1F3DF}️', fire: '\u{1F525}', ice: '\u{1F9CA}', cap: '\u{1F9E2}',
    bats: '\u{1F94A}', book: '\u{1F4DC}', coin: '\u{1FA99}', diamond: '\u{1F48E}',
  };
  const EMOJI_FONT = '"Segoe UI Emoji", "Apple Color Emoji", "Noto Color Emoji", sans-serif';
  const spriteCache = {};
  function sprite(name, size) {
    size = size || 48;
    const key = name + ':' + size;
    if (spriteCache[key]) return spriteCache[key];
    const res = Math.max(10, Math.round(size / 2));
    const c = document.createElement('canvas');
    c.width = c.height = res;
    const g = c.getContext('2d');
    g.textAlign = 'center';
    g.textBaseline = 'middle';
    g.font = Math.round(res * 0.86) + 'px ' + EMOJI_FONT;
    g.fillText(EMOJI[name] || EMOJI.ball, res / 2, res / 2 + res * 0.06);
    // Hard-edge the alpha so the silhouette reads as crisp pixels.
    const id = g.getImageData(0, 0, res, res), p = id.data;
    for (let i = 3; i < p.length; i += 4) p[i] = p[i] > 100 ? 255 : 0;
    g.putImageData(id, 0, 0);
    const html = '<img class="pix sprite" src="' + c.toDataURL() + '" width="' + size + '" height="' + size + '" alt="" aria-hidden="true">';
    spriteCache[key] = html;
    return html;
  }

  // Replace every <i data-sprite="name" data-size="32"></i> with its image.
  function paintSprites(root) {
    (root || document).querySelectorAll('[data-sprite]').forEach(function (el) {
      el.outerHTML = sprite(el.getAttribute('data-sprite'), +el.getAttribute('data-size') || 48);
    });
  }

  // ── Title-screen ballpark ────────────────────────────────────────────────
  // Painted pixel by pixel at 480x180 and shown scaled up with hard edges, so
  // each pixel is ~3px on a desktop screen. Lighting (tower floodlights, moon,
  // field glow) is computed as real falloff, then the whole frame is reduced
  // to a limited colour depth with an ordered (Bayer) dither: smooth shading,
  // still visibly pixel art.
  function stadium(host) {
    const W = 480, H = 180;
    const c = document.createElement('canvas');
    c.width = W; c.height = H;
    c.className = 'pix title-scene';
    c.setAttribute('role', 'img');
    c.setAttribute('aria-label', 'A pixel-art ballpark at night under floodlights');
    const R = new Float32Array(W * H), G = new Float32Array(W * H), B = new Float32Array(W * H);
    let seed = 2007;
    const rnd = function () { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
    const clamp = function (v, a, b) { return v < a ? a : v > b ? b : v; };
    const mix = function (a, b, t) { return a + (b - a) * t; };
    const put = function (x, y, r, g, b, a) {
      x |= 0; y |= 0;
      if (x < 0 || y < 0 || x >= W || y >= H) return;
      const i = y * W + x; a = a == null ? 1 : a;
      R[i] = mix(R[i], r, a); G[i] = mix(G[i], g, a); B[i] = mix(B[i], b, a);
    };
    const glow = function (cx, cy, rad, r, g, b, k) {
      for (let y = Math.max(0, (cy - rad) | 0); y < Math.min(H, cy + rad); y++) {
        for (let x = Math.max(0, (cx - rad) | 0); x < Math.min(W, cx + rad); x++) {
          const d = Math.hypot(x - cx, (y - cy) * 1.15) / rad;
          if (d >= 1) continue;
          const f = Math.pow(1 - d, 2.2) * k, i = y * W + x;
          R[i] += r * f; G[i] += g * f; B[i] += b * f;
        }
      }
    };
    const standsTop = function (x) { const t = (x - W / 2) / (W / 2); return 76 - 20 * t * t; };
    const WALL = 120, FIELD = 128;

    // Night sky: deep blue overhead, warming to violet at the horizon.
    const sky = [[0, [6, 6, 26]], [40, [16, 14, 52]], [70, [44, 24, 84]], [96, [86, 40, 104]]];
    for (let y = 0; y < FIELD; y++) {
      let a = sky[0], b = sky[sky.length - 1];
      for (let k = 0; k < sky.length - 1; k++) if (y >= sky[k][0] && y <= sky[k + 1][0]) { a = sky[k]; b = sky[k + 1]; }
      const t = b[0] === a[0] ? 0 : clamp((y - a[0]) / (b[0] - a[0]), 0, 1);
      for (let x = 0; x < W; x++) put(x, y, mix(a[1][0], b[1][0], t), mix(a[1][1], b[1][1], t), mix(a[1][2], b[1][2], t));
    }
    // Stars, a few with a soft halo and a twinkle cross.
    for (let s = 0; s < 220; s++) {
      const x = rnd() * W, y = rnd() * 62, v = 120 + rnd() * 135, warm = rnd() > 0.85;
      put(x, y, v, v * (warm ? 0.9 : 0.95), warm ? v * 0.6 : v);
      if (rnd() > 0.93) {
        glow(x, y, 4, 120, 120, 160, 0.6);
        [[-1, 0], [1, 0], [0, -1], [0, 1]].forEach(function (o) { put(x + o[0], y + o[1], v * 0.5, v * 0.5, v * 0.6); });
      }
    }
    // Moon with a halo and shaded craters.
    glow(410, 22, 46, 70, 70, 110, 0.9);
    for (let y = -9; y <= 9; y++) for (let x = -9; x <= 9; x++) {
      if (Math.hypot(x, y) > 9) continue;
      const shade = 0.78 + 0.22 * clamp((-x - y) / 12, -1, 1);
      put(410 + x, 22 + y, 236 * shade, 232 * shade, 206 * shade);
    }
    [[407, 19, 2.2], [413, 25, 1.6], [411, 18, 1.1], [406, 26, 1.3]].forEach(function (cr) {
      for (let y = -3; y <= 3; y++) for (let x = -3; x <= 3; x++) if (Math.hypot(x, y) <= cr[2]) put(cr[0] + x, cr[1] + y, 190, 186, 166, 0.7);
    });
    // Home run ball in flight with a fading trail.
    for (let i = 0; i < 26; i++) {
      const t = i / 25, x = mix(300, 368, t), y = 104 - Math.sin(t * Math.PI * 0.62) * 70;
      put(x, y, 230, 230, 240, 0.15 + t * 0.5);
    }
    glow(369, 40, 7, 140, 140, 150, 0.8);
    for (let y = -1; y <= 1; y++) for (let x = -1; x <= 1; x++) if (Math.abs(x) + Math.abs(y) < 2) put(369 + x, 40 + y, 250, 250, 250);
    put(370, 40, 220, 60, 60, 0.6);

    // Grandstand: a shaded bowl packed with crowd pixels, lit from above.
    for (let x = 0; x < W; x++) {
      const top = standsTop(x) | 0;
      for (let y = top; y < WALL; y++) {
        const depth = (y - top) / (WALL - top);
        const base = 14 + 22 * depth;
        put(x, y, base * 0.9, base * 0.9, base * 1.5);
        if ((y - top) % 3 !== 2 && rnd() > 0.18) {
          const v = rnd(), lit = 0.45 + 0.55 * depth;
          const col = v > 0.92 ? [230, 60, 60] : v > 0.85 ? [70, 150, 240] : v > 0.78 ? [240, 200, 70] : v > 0.6 ? [220, 214, 200] : [120, 110, 150];
          put(x, y, col[0] * lit, col[1] * lit, col[2] * lit, 0.75);
        }
      }
      put(x, top - 1, 70, 70, 110); put(x, top - 2, 30, 30, 58);   // roof lip
    }
    // Light towers with floodlight banks, and the glow they throw.
    [70, 410, 168, 312].forEach(function (tx, n) {
      const top = standsTop(tx) | 0, big = n < 2, bankY = top - (big ? 34 : 20);
      for (let y = bankY + 4; y < top; y++) { put(tx, y, 90, 90, 120); put(tx + 1, y, 54, 54, 80); }
      const bw = big ? 16 : 10;
      for (let y = 0; y < 6; y++) for (let x = -bw / 2; x < bw / 2; x++) {
        const on = (x + bw) % 3 !== 0 && y % 2 === 1;
        put(tx + x, bankY + y, on ? 255 : 30, on ? 248 : 30, on ? 214 : 44);
      }
      glow(tx, bankY + 3, big ? 120 : 70, 255, 236, 180, big ? 0.55 : 0.32);
      glow(tx, bankY + 3, big ? 26 : 16, 255, 250, 230, 0.9);
    });

    // Outfield wall: padded green panels, a yellow home-run line, ad boards.
    for (let y = WALL; y < FIELD; y++) for (let x = 0; x < W; x++) {
      const panel = Math.floor(x / 24) % 2;
      if (y === WALL) put(x, y, 250, 210, 70);
      else put(x, y, 18 + panel * 4, 70 + panel * 6 + (y - WALL) * 2, 44 + panel * 4);
    }
    [[40, [200, 50, 60]], [150, [40, 90, 200]], [290, [210, 150, 30]], [400, [120, 60, 190]]].forEach(function (ad) {
      for (let y = WALL + 2; y < FIELD - 2; y++) for (let x = ad[0]; x < ad[0] + 38; x++) put(x, y, ad[1][0], ad[1][1], ad[1][2], 0.7);
      for (let x = ad[0] + 4; x < ad[0] + 34; x += 2) if (rnd() > 0.35) put(x, WALL + 4, 240, 240, 240, 0.8);
    });

    // Grass: mowing checkerboard in perspective.
    const home = [W / 2, H - 4];
    for (let y = FIELD; y < H; y++) {
      const p = (y - FIELD) / (H - FIELD);
      for (let x = 0; x < W; x++) {
        const u = (x - W / 2) / (0.6 + p * 1.4);
        const stripe = (Math.floor(u / 18) + Math.floor((y - FIELD) / (4 + p * 6))) % 2 === 0;
        put(x, y, 26 + p * 8, (stripe ? 118 : 100) + p * 26, 40 + p * 6);
      }
    }
    // Infield dirt arc around a grass diamond, plus the home-plate circle.
    const icx = W / 2, icy = 160;
    for (let y = FIELD; y < H; y++) for (let x = 0; x < W; x++) {
      const dx = (x - icx) / 74, dy = (y - icy) / 24;
      const dirt = dx * dx + dy * dy <= 1 && y > 142;
      const inner = Math.abs(x - icx) / 40 + Math.abs(y - (icy - 2)) / 14 <= 1;
      const circle = Math.hypot((x - home[0]) / 1.8, y - home[1]) < 9;
      if ((dirt && !inner) || circle) {
        const n = rnd() * 14;
        put(x, y, 168 + n, 104 + n * 0.7, 62 + n * 0.4);
      }
    }
    // Foul lines toward the corners.
    for (let t = 0; t <= 1; t += 0.0015) {
      put(mix(home[0], 10, t), mix(home[1], FIELD + 1, t), 240, 240, 230, 0.9);
      put(mix(home[0], W - 10, t), mix(home[1], FIELD + 1, t), 240, 240, 230, 0.9);
    }
    // Mound and rubber, bases, plate.
    for (let y = -3; y <= 3; y++) for (let x = -6; x <= 6; x++) if ((x * x) / 36 + (y * y) / 9 <= 1) put(icx + x, icy - 2 + y, 190, 124, 76);
    put(icx, icy - 3, 250, 250, 245); put(icx - 1, icy - 3, 250, 250, 245);
    [[icx, icy - 16], [icx - 40, icy - 2], [icx + 39, icy - 2]].forEach(function (b) {
      for (let y = 0; y < 2; y++) for (let x = 0; x < 3; x++) put(b[0] + x - 1, b[1] + y, 250, 250, 245);
    });
    for (let x = -2; x <= 2; x++) put(home[0] + x, home[1] - 1, 250, 250, 245);
    for (let x = -1; x <= 1; x++) put(home[0] + x, home[1], 250, 250, 245);

    // Floodlight wash on the field, haze above the stands, and a vignette.
    glow(W / 2, 150, 190, 60, 64, 36, 0.55);
    for (let x = 0; x < W; x++) {
      const t = standsTop(x) | 0;
      for (let y = t - 10; y < t; y++) { const i = y * W + x, f = (y - (t - 10)) / 10 * 0.12; R[i] += 60 * f; G[i] += 50 * f; B[i] += 70 * f; }
    }
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
      const d = Math.hypot((x - W / 2) / (W / 2), (y - H / 2) / (H / 1.6));
      const f = 1 - clamp((d - 0.7) * 0.6, 0, 0.45), i = y * W + x;
      R[i] *= f; G[i] *= f; B[i] *= f;
    }

    // Reduce to a limited colour depth with a 4x4 ordered dither.
    const BAYER = [0, 8, 2, 10, 12, 4, 14, 6, 3, 11, 1, 9, 15, 7, 13, 5];
    const STEP = 14;
    const g2 = c.getContext('2d'), img = g2.createImageData(W, H), px = img.data;
    for (let y = 0; y < H; y++) for (let x = 0; x < W; x++) {
      const i = y * W + x, o = i * 4, t = (BAYER[(y & 3) * 4 + (x & 3)] / 16 - 0.5) * STEP;
      px[o] = clamp(Math.round((R[i] + t) / STEP) * STEP, 0, 255);
      px[o + 1] = clamp(Math.round((G[i] + t) / STEP) * STEP, 0, 255);
      px[o + 2] = clamp(Math.round((B[i] + t) / STEP) * STEP, 0, 255);
      px[o + 3] = 255;
    }
    g2.putImageData(img, 0, 0);
    if (host) { host.innerHTML = ''; host.appendChild(c); }
    return c;
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
