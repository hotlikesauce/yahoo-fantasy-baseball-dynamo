#!/usr/bin/env python3
"""
Render docs/live_standings_2026.html from the PUBLIC Yahoo league page.

The Yahoo Fantasy API has been dead app-wide since ~2026-07-26 (every endpoint
403s, refresh tokens are gone), so the league home page - which the league is
set to render to anonymous requests - is the only source left. Only /b1/<id>
works anonymously; /standings, /matchup and /scoreboard come back as empty
shells, and manager names are not public either, so those come from DynamoDB.

This script is now the MANUAL path. The scheduled path is AWS: the Lambda
snapshot-playoff-odds runs the same code on an EventBridge schedule, publishes
this page to GitHub itself, and owns the odds-over-time snapshots. Reach for
this when you want to see the numbers right now or preview a markup change -
the maths and the markup are shared modules, so both paths agree exactly.

Usage:
    python scripts/scrape_live_standings.py            # scrape + write page
    python scripts/scrape_live_standings.py --dry-run  # scrape + print, no write
"""

import os, sys, io, json, argparse
from datetime import datetime, timezone

from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
load_dotenv()

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(REPO, 'docs')
sys.path.insert(0, os.path.join(REPO, 'lambda', 'functions'))

import playoff_core as pc                                    # noqa: E402
from playoff_core import YEAR, SIMS, odds_value              # noqa: E402
from playoff_render import render, status_chip, fmt_pts, fmt_odds   # noqa: E402

HISTORY_PATH = os.path.join(DOCS, 'data', f'playoff_odds_history_{YEAR}.json')


def league_id():
    pairs = os.getenv('YAHOO_LEAGUE_IDS', '')
    for pair in pairs.split(','):
        if pair.startswith(f'{YEAR}:'):
            return pair.split(':')[1]
    sys.exit(f'ERROR: no {YEAR} league id in .env YAHOO_LEAGUE_IDS')


def load_history():
    """
    The snapshots the chart draws. DynamoDB is the record of truth - the AWS
    schedule writes it five times a day whether or not this machine is awake -
    and the local JSON is only a mirror so the published page keeps its data if
    the table is ever unreachable.
    """
    try:
        hist = pc.history_from_dynamo()
        if hist['points']:
            print(f'  history: {len(hist["points"])} snapshot(s) from DynamoDB')
            return hist
        print('  history: DynamoDB has no snapshots yet - falling back to the local mirror')
    except Exception as e:
        print(f'  history: DynamoDB unreachable ({e}) - using the local mirror')

    if os.path.exists(HISTORY_PATH):
        try:
            with io.open(HISTORY_PATH, encoding='utf-8') as f:
                return json.load(f)
        except (OSError, ValueError):
            pass
    return {'year': YEAR, 'slot_hours': list(pc.SLOT_HOURS), 'tracked': [], 'points': []}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='print, do not write')
    ap.add_argument('--skip-unchanged', action='store_true',
                    help='leave the files alone when only the clock moved')
    args = ap.parse_args()

    lid = league_id()
    print(f'Scraping public league page: https://baseball.fantasysports.yahoo.com/b1/{lid}')

    r = pc.collect(lid)
    teams, spots, week = r['teams'], r['spots'], r['week']
    meta, settings, matchups, progress = (r['meta'], r['settings'],
                                          r['matchups'], r['progress'])

    print(f'  {meta["league_name"]} · week {week} ({meta["status"]}) · '
          f'{len(teams)} teams · {len(matchups)} matchups')
    print(f'  Playoffs: {settings["raw"]} → regular season ends week '
          f'{r["last_regular_week"]}')
    print(f'  Week {week} is {progress * 100:.0f}% played — live category leads '
          f'are weighted, not banked')

    hist = load_history()
    if not hist.get('tracked'):
        hist['tracked'] = pc.pick_tracked(teams, spots)

    order = sorted(teams.values(), key=lambda t: -(t['pts'] + t['live']))
    print(f'\n{"#":>2} {"team":<28} {"rec":>12} {"live":>4} {"pts":>6} '
          f'{"range":>13} {"odds":>6}  status')
    for i, t in enumerate(order, 1):
        _, label = status_chip(t)
        print(f'{i:>2} {t["name"][:28]:<28} '
              f'{t["wins"]}-{t["losses"]}-{t["ties"]:<6} {t["live"]:>4} '
              f'{fmt_pts(t["pts"] + t["live"]):>6} '
              f'{fmt_pts(t["floor"]) + "-" + fmt_pts(t["ceiling"]):>13} '
              f'{odds_value(t):>5.1f}%  {label}'
              + (f'  (clinch w/ {t["magic"]})' if t.get('magic') else ''))

    tracked = ', '.join(teams[t]['name'] for t in hist['tracked'] if t in teams)
    print(f'\nRace chart tracks: {tracked}')
    print(f'  {len(hist["points"])} snapshot(s) on the chart')

    payload = {
        'year': YEAR,
        'week': week,
        'status': meta['status'],
        'yahoo_updated': meta['yahoo_updated'],
        'scraped_at': datetime.now(timezone.utc).isoformat(),
        'source': r['base'],
        'playoff_spots': spots,
        'playoff_format': settings['raw'],
        'last_regular_week': r['last_regular_week'],
        'sims': SIMS,
        'week_progress': round(progress, 4),
        'teams': [{k: v for k, v in t.items() if k != 'seed_counts'} for t in order],
        'matchups': matchups,
    }

    if args.dry_run:
        print('\n(dry run — nothing written)')
        return

    html_path = os.path.join(DOCS, f'live_standings_{YEAR}.html')
    page = render(payload, teams, spots, week, meta, settings['raw'],
                  matchups, progress, hist)

    # Same normalization the Lambda publisher uses, so "did anything actually
    # move?" means the same thing on both paths.
    if args.skip_unchanged and os.path.exists(html_path):
        import github_publish
        with io.open(html_path, encoding='utf-8') as f:
            if github_publish.normalize(f.read()) == github_publish.normalize(page):
                print('\nNo change since last scrape — nothing written')
                return

    os.makedirs(os.path.join(DOCS, 'data'), exist_ok=True)
    with io.open(os.path.join(DOCS, 'data', f'live_standings_{YEAR}.json'),
                 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    with io.open(HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)
    with io.open(html_path, 'w', encoding='utf-8') as f:
        f.write(page)

    print(f'\nWrote {html_path}')
    print('(the AWS schedule publishes this page too — a manual run here is '
          'only visible once you commit and push it)')


if __name__ == '__main__':
    main()
