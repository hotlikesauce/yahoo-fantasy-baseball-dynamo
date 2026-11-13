"""Stamp shared <head> tags into every page in docs/.

Run after any gen_*.py script rewrites a page:

    python scripts/patch_page_heads.py

Idempotent - safe to run any number of times. It does three things:

1. Pins Chart.js to one exact version. A bare ".../npm/chart.js" URL serves
   whatever "latest" is, so a future Chart.js 5 would silently break every
   chart on the site overnight. One pinned URL is also cached once and shared
   by every page.

2. Writes a block of meta tags right after <title>, between
   <!-- head-meta --> markers (replaced on each run). Link previews in
   iMessage / Discord / Slack / Twitter read these from the raw HTML and never
   run JavaScript, so they cannot be injected by nav.js like the rest of the
   shared head bits. The block also requests the two pixel fonts early.

3. Applies the 8-bit palette: every old slate/Tailwind colour in the page
   (hex, hex+alpha, rgb/rgba) is swapped for its replacement from the PALETTE
   block in docs/nav.js, and the old Segoe UI font stack becomes `inherit`.
   No replacement colour is also a source colour, so re-running is a no-op.

4. Cache-busts the shared assets: every reference to common.css, nav.js and
   pixel.js gets ?v=<hash of that file's contents>. GitHub Pages lets browsers
   cache these for 10 minutes, so without this a visitor gets a new page with
   an old stylesheet after every deploy. Re-run this script after editing any
   of those three files.
"""
import hashlib
import json
import time
import html
import re
from pathlib import Path

DOCS = Path(__file__).resolve().parent.parent / "docs"
SITE = "https://" + (DOCS / "CNAME").read_text(encoding="utf-8").strip()
SITE_NAME = "Summertime Sadness Fantasy Baseball"

NAV_JS = (DOCS / "nav.js").read_text(encoding="utf-8")
PALETTE = json.loads(re.search(r"/\*PALETTE\*/\s*const PALETTE = (\{.*?\});\s*/\*END PALETTE\*/", NAV_JS, re.S).group(1))
assert not set(PALETTE) & set(PALETTE.values()), "a replacement colour is also a source colour"
def _rgb(h): return ",".join(str(int(h[i:i + 2], 16)) for i in (1, 3, 5))
RGB = {_rgb(k): _rgb(v) for k, v in PALETTE.items()}
HEX_RE = re.compile(r"#([0-9a-fA-F]{6})(?=[0-9a-fA-F]{2}(?![0-9a-fA-F])|(?![0-9a-fA-F]))")
RGB_RE = re.compile(r"(rgba?\(\s*)(\d+)\s*,\s*(\d+)\s*,\s*(\d+)", re.I)
OLD_FONT_RE = re.compile(r"""font-family:\s*['"]Segoe UI['"],\s*system-ui,\s*sans-serif""")


def repalette(s: str) -> str:
    s = HEX_RE.sub(lambda m: PALETTE.get("#" + m.group(1).lower(), m.group(0)), s)
    s = RGB_RE.sub(lambda m: (m.group(1) + RGB[k]) if (k := f"{m.group(2)},{m.group(3)},{m.group(4)}") in RGB else m.group(0), s)
    return OLD_FONT_RE.sub("font-family: inherit", s)


ASSETS = ("common.css", "nav.js", "pixel.js")
VERSION = {a: hashlib.sha1((DOCS / a).read_bytes()).hexdigest()[:8] for a in ASSETS if (DOCS / a).exists()}
ASSET_RE = re.compile(r"""((?:href|src)=["'])(common\.css|nav\.js|pixel\.js)(?:\?v=[0-9a-f]*)?(["'])""")


def bust(s: str) -> str:
    return ASSET_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}?v={VERSION[m.group(2)]}{m.group(3)}", s)


CHART_JS = "https://cdn.jsdelivr.net/npm/chart.js@4.6.1/dist/chart.umd.min.js"
CHART_RE = re.compile(r"https://cdn\.jsdelivr\.net/npm/chart\.js(?:@[\w.\-]+)?(?:/dist/chart\.umd(?:\.min)?\.js)?(?=[\"'])")

DEFAULT_DESC = "Stats, standings, history and tools for the Summertime Sadness fantasy baseball league, est. 2007."
DESCRIPTIONS = {
    "index.html": "Standings, power rankings, champions and title leaders for the Summertime Sadness fantasy baseball league, est. 2007.",
    "all_time_rankings.html": "The greatest seasons in league history, champions by year and career averages since 2007.",
    "h2h_records.html": "Every manager's all-time head-to-head record against every other manager.",
    "manager_profiles.html": "Career stats, season history and head-to-head records for every manager in league history.",
    "daily_game.html": "Guess the MLB team behind last night's top fantasy stat line.",
    "championship_hub_2026.html": "Live scoring and lineups for the 2026 two-week championship final.",
    "live_standings_2026.html": "Live 2026 standings and this week's matchups.",
    "season_trends_2026.html": "Weekly power scores, power rankings, expected category wins and hot/cold streaks for 2026.",
    "luck_analysis_2026.html": "Luck scores, all-play records, schedule strength and what-if standings for 2026.",
    "playoff_hub_2026.html": "2026 playoff bracket, title odds and matchup predictions.",
    "playoff_scenarios_2026.html": "Set remaining matchup results to see where the 2026 playoff cut line falls.",
    "superlatives_2026.html": "13 season awards for 2026.",
    "trade_analyzer.html": "Evaluate trades with player values, category impact and draft pick analysis.",
    "trade_grades_2026.html": "Grades for every completed 2026 trade, based on current player rankings and draft pick value.",
    "find_me_a_trade.html": "Pick a position you want and what you'll give up; every possible trade partner, ranked.",
    "positional_strength_2026.html": "How every 2026 roster stacks up, position by position.",
    "draft_picks_2026.html": "Who owns which picks in the 2027 draft, and how much draft capital each team holds.",
    "draft_results_2026.html": "The full 2026 draft board, round by round.",
    "keepers_2026.html": "Every team's 2026 keeper pair, ranked by Yahoo preseason rank.",
    "draft_picks_2025.html": "2025 draft capital by team and the full draft board.",
    "season_trends_2025.html": "Weekly power scores, rankings and expected category wins for the 2025 season.",
    "luck_analysis_2025.html": "Luck scores, all-play records, blowouts and closest matchups from 2025.",
    "season_trends_2024.html": "Weekly power scores and rankings for the 2024 season.",
    "season_trends_2023.html": "Weekly power scores and rankings for the 2023 season.",
    "season_trends_2022.html": "Weekly power scores and rankings for the 2022 season.",
}

START, END = "<!-- head-meta -->", "<!-- /head-meta -->"
BLOCK_RE = re.compile(r"\r?\n?[ \t]*" + re.escape(START) + r".*?" + re.escape(END), re.S)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S | re.I)


def attr(s: str) -> str:
    return html.escape(html.unescape(s).strip(), quote=True)


def meta_block(page: str, title: str, indent: str, nl: str) -> str:
    desc = DESCRIPTIONS.get(page, DEFAULT_DESC)
    url = SITE + "/" + ("" if page == "index.html" else page)
    lines = [
        START,
        '<meta name="description" content="%s">' % attr(desc),
        # Paints the page dark before CSS arrives - no white flash between pages.
        '<meta name="color-scheme" content="dark">',
        '<meta name="theme-color" content="#000000">',
        '<link rel="preconnect" href="https://fonts.googleapis.com">',
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>',
        '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Press+Start+2P&family=VT323&display=swap">',
        '<link rel="manifest" href="manifest.json">',
        '<meta property="og:type" content="website">',
        '<meta property="og:site_name" content="%s">' % SITE_NAME,
        '<meta property="og:title" content="%s">' % attr(title),
        '<meta property="og:description" content="%s">' % attr(desc),
        '<meta property="og:url" content="%s">' % url,
        '<meta property="og:image" content="%s/icon-512.png">' % SITE,
        '<meta name="twitter:card" content="summary">',
        END,
    ]
    return nl + nl.join(indent + l for l in lines)


def patch(path: Path) -> list[str]:
    # newline="" on both ends: keep each file's own line endings untouched.
    with open(path, encoding="utf-8", newline="") as f:
        src = f.read()
    nl = "\r\n" if "\r\n" in src else "\n"
    out = CHART_RE.sub(CHART_JS, src)
    changes = ["chart.js pinned"] if out != src else []
    before = out
    out = bust(repalette(out))
    if out != before:
        changes.append("palette")

    m = TITLE_RE.search(out)
    if m:
        out = BLOCK_RE.sub("", out)
        m = TITLE_RE.search(out)
        line_start = out.rfind("\n", 0, m.start()) + 1
        indent = re.match(r"[ \t]*", out[line_start:]).group(0)
        out = out[: m.end()] + meta_block(path.name, m.group(1), indent, nl) + out[m.end():]
    if out != src:
        for attempt in range(5):  # Windows sometimes holds a brief lock on files open in an editor
            try:
                with open(path, "w", encoding="utf-8", newline="") as f:
                    f.write(out)
                break
            except OSError:
                if attempt == 4:
                    raise
                time.sleep(0.5)
        if m and "meta" not in changes:
            changes.append("meta")
    return changes


if __name__ == "__main__":
    for page in sorted(DOCS.glob("*.html")):
        changed = patch(page)
        print(f"{page.name:32s} {', '.join(changed) or 'unchanged'}")
