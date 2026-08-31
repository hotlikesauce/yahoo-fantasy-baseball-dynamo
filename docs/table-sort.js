/**
 * Site-wide column sorting.
 *
 * Loaded by nav.js, so every page gets it without editing 24 HTML files.
 *
 * Two things make this harder than the usual snippet:
 *
 *  1. Most tables on this site are built by page JavaScript AFTER load - the
 *     superlatives board, the playoff hub, luck analysis. A one-shot scan on
 *     DOMContentLoaded would miss nearly all of them, so a MutationObserver
 *     picks up tables as they appear.
 *
 *  2. Ten pages already ship their own sorter, using `th.sortable` with their
 *     own click handlers. Binding a second handler to those would sort twice
 *     per click and fight the page. So any table that already has a
 *     `th.sortable` is left alone entirely - this fills the gaps rather than
 *     taking over.
 *
 * Opt a table out with `data-nosort` on the <table>.
 */
(function () {
  'use strict';

  var PLACEHOLDER = /^[-–—−.\s]*$/;          // "", "-", "—", "–"
  var ISO_DATE = /^\d{4}-\d{2}-\d{2}$/;
  var RECORD = /^\d{1,3}-\d{1,3}(-\d{1,3})?$/;              // 141-104-7, 12-0

  function text(cell) {
    return (cell ? cell.textContent : '').replace(/ /g, ' ').trim();
  }

  /** A plain number, tolerating +, %, $, commas, and a trailing unit. */
  function num(s) {
    var t = s.replace(/[\s,$]/g, '');
    if (!t || PLACEHOLDER.test(t)) return null;
    var m = t.match(/^([-+−]?)(\d*\.?\d+)%?$/);
    if (m) return (m[1] === '-' || m[1] === '−' ? -1 : 1) * parseFloat(m[2]);
    // "4 wks", "3rd", "12 moves" - lead with the number
    var lead = t.match(/^([-+−]?\d*\.?\d+)/);
    return lead ? parseFloat(lead[1].replace('−', '-')) : null;
  }

  /**
   * W-L-T as a win percentage. Sorting 141-104-7 by its first number alone
   * would rank a 10-0 team below a 9-90 one, which is the opposite of useful.
   */
  function record(s) {
    var p = s.split('-').map(Number);
    var w = p[0] || 0, l = p[1] || 0, t = p[2] || 0;
    var g = w + l + t;
    return g ? (w + 0.5 * t) / g : null;
  }

  /** A row that spans the table - a cut line, a section break - never sorts. */
  function isSeparator(tr, cols) {
    var cells = tr.children;
    if (!cells.length) return true;
    if (cells.length < cols) return true;
    for (var i = 0; i < cells.length; i++) {
      if ((cells[i].colSpan || 1) > 1) return true;
    }
    return false;
  }

  function columnType(rows, idx) {
    var seen = 0, dates = 0, records = 0, nums = 0;
    for (var i = 0; i < rows.length; i++) {
      var s = text(rows[i].children[idx]);
      if (!s || PLACEHOLDER.test(s)) continue;
      seen++;
      if (ISO_DATE.test(s)) dates++;
      else if (RECORD.test(s)) records++;
      if (num(s) !== null) nums++;
    }
    if (!seen) return 'str';
    if (dates / seen >= 0.6) return 'date';
    if (records / seen >= 0.6) return 'record';
    if (nums / seen >= 0.6) return 'num';
    return 'str';
  }

  function valueOf(tr, idx, type) {
    var s = text(tr.children[idx]);
    if (!s || PLACEHOLDER.test(s)) return null;
    if (type === 'date') return Date.parse(s) || null;
    if (type === 'record') return record(s);
    if (type === 'num') return num(s);
    return s.toLowerCase();
  }

  function attach(table) {
    if (table.dataset.sortReady) return;

    var tbody = table.tBodies[0];
    if (!tbody) return;
    // A page that already sorts this table keeps it.
    if (table.querySelector('th.sortable')) return;
    if (table.hasAttribute('data-nosort')) return;

    // Several pages (all_time_rankings, keepers) write `<table><tr><th>...`
    // with no <thead>. Browsers invent a <tbody> in that case but never a
    // <thead>, so the header row lands among the data rows - requiring
    // table.tHead skipped those tables silently. Fall back to a leading
    // all-<th> row inside the body, and keep it out of the sort.
    var head = null, skipFirstBodyRow = false;
    if (table.tHead && table.tHead.rows.length) {
      head = table.tHead.rows[table.tHead.rows.length - 1];   // labels sit deepest
    } else if (tbody.rows.length) {
      var first = tbody.rows[0];
      var allTh = first.cells.length > 0;
      for (var i = 0; i < first.cells.length; i++) {
        if (first.cells[i].tagName !== 'TH') { allTh = false; break; }
      }
      if (allTh) { head = first; skipFirstBodyRow = true; }
    }
    if (!head) return;

    var cols = head.cells.length;
    if (cols < 2) return;

    var bodyRows = Array.prototype.slice.call(tbody.rows);
    if (skipFirstBodyRow) bodyRows = bodyRows.slice(1);
    var dataRows = bodyRows.filter(function (r) { return !isSeparator(r, cols); });
    if (dataRows.length < 2) return;

    table.dataset.sortReady = '1';
    var original = bodyRows.slice();   // header row excluded when it is in the body

    Array.prototype.forEach.call(head.cells, function (th, idx) {
      if ((th.colSpan || 1) > 1) return;
      th.classList.add('sortable');
      th.tabIndex = 0;
      th.setAttribute('role', 'button');
      th.setAttribute('aria-sort', 'none');

      function run() {
        var was = th.classList.contains('asc') ? 'asc'
                : th.classList.contains('desc') ? 'desc' : null;
        // asc -> desc -> back to the order the page built, which is how the
        // cut-line and section rows get to be meaningful again.
        var dir = was === 'asc' ? 'desc' : was === 'desc' ? null : 'asc';

        Array.prototype.forEach.call(head.cells, function (h) {
          h.classList.remove('asc', 'desc');
          if (h.classList.contains('sortable')) h.setAttribute('aria-sort', 'none');
        });

        var frag = document.createDocumentFragment();
        if (!dir) {
          original.forEach(function (r) { r.hidden = false; frag.appendChild(r); });
          tbody.appendChild(frag);
          return;
        }

        th.classList.add(dir);
        th.setAttribute('aria-sort', dir === 'asc' ? 'ascending' : 'descending');

        var type = columnType(dataRows, idx);
        var sign = dir === 'asc' ? 1 : -1;
        var rows = dataRows.slice();
        rows.sort(function (a, b) {
          var va = valueOf(a, idx, type), vb = valueOf(b, idx, type);
          // Blanks sink to the bottom either way - a missing value is not the
          // smallest value, and flipping it to the top on one click is noise.
          if (va === null && vb === null) return 0;
          if (va === null) return 1;
          if (vb === null) return -1;
          if (type === 'str') return sign * String(va).localeCompare(String(vb));
          return sign * (va - vb);
        });

        // Separator rows mean nothing once the order changes, so they go away
        // until the third click restores the original order.
        original.forEach(function (r) {
          if (dataRows.indexOf(r) === -1) r.hidden = true;
        });
        rows.forEach(function (r) { r.hidden = false; frag.appendChild(r); });
        tbody.appendChild(frag);
      }

      th.addEventListener('click', run);
      th.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); run(); }
      });
    });
  }

  function scan(root) {
    var tables = (root || document).querySelectorAll('table');
    Array.prototype.forEach.call(tables, attach);
  }

  function start() {
    scan(document);
    // Pages render their tables from JSON after load, and the playoff hub
    // rebuilds cards on every dropdown change, so watch rather than scan once.
    if (!window.MutationObserver) return;
    var pending = false;
    new MutationObserver(function () {
      if (pending) return;
      pending = true;
      requestAnimationFrame(function () { pending = false; scan(document); });
    }).observe(document.body, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
