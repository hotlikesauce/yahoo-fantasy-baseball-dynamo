#!/usr/bin/env python3
"""
Load this week's category/IP numbers from text pasted off Yahoo.

Why this exists: during live games Yahoo stops putting the matchup stat table
in the HTML. It is not in view-source and not in any XHR response either - most
likely it arrives over a websocket - so no HTTP scraper can reach it. But the
numbers are right there on screen, and the parser that reads them is already
written and validated. So: copy the page, paste it here.

This writes into the SAME DynamoDB cache the Lambda already falls back to, so
one paste lights up the IP toggles, both odds scenarios and the tracker.

Usage:
    python scripts/load_matchup_stats.py            # paste, then Ctrl+Z Enter
    python scripts/load_matchup_stats.py file.txt
    python scripts/load_matchup_stats.py --show     # what is cached now
"""

import os
import sys
import io
import re
import json
from decimal import Decimal

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'lambda', 'functions'))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import playoff_core as pc          # noqa: E402

# .env is not on the path for a bare script run
_env = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
if os.path.exists(_env):
    for _line in io.open(_env, encoding='utf-8'):
        if '=' in _line and not _line.strip().startswith('#'):
            _k, _, _v = _line.partition('=')
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# the 15 values that follow a team name in the pasted table
COLS = ['H/AB*', 'R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'IP*', 'TB',
        'ERA', 'WHIP', 'K/9', 'QS', 'SV+H', 'Score']
VALUE = re.compile(r'^-?[\d./]+$')


def team_lookup():
    """Team names as Yahoo spells them, longest first so substrings lose."""
    lid = None
    for pair in (os.getenv('YAHOO_LEAGUE_IDS') or '').split(','):
        if pair.startswith(f'{pc.YEAR}:'):
            lid = pair.split(':')[1]
    if not lid:
        sys.exit('no league id in .env YAHOO_LEAGUE_IDS')
    rows = pc.parse_standings(pc.get(
        f'https://baseball.fantasysports.yahoo.com/b1/{lid}'), lid)
    return sorted(({r['name']: r['team_id'] for r in rows}).items(),
                  key=lambda kv: -len(kv[0])), lid


def parse(text, names):
    """
    Walk the paste looking for a team name followed by its 15 numbers.

    Tolerant on purpose: the copied page carries 'logo' lines, blank lines and
    nav text between the cells, and the roster tables below repeat the same
    headers. Anything that is not a bare value is skipped, and a run that does
    not yield a usable IP is discarded rather than half-loaded.
    """
    lines = [ln.strip() for ln in text.splitlines()]
    out = {}
    for i, ln in enumerate(lines):
        tid = None
        for name, t in names:
            if ln == name:
                tid = t
                break
        if tid is None or tid in out:
            continue
        vals = []
        for nxt in lines[i + 1:i + 60]:
            if not nxt or nxt.lower() == 'logo':
                continue
            if VALUE.match(nxt):
                vals.append(nxt)
                if len(vals) == len(COLS):
                    break
            elif vals:
                break
        if len(vals) < len(COLS) - 1:
            continue
        row = dict(zip(COLS, vals))
        if not row.get('IP*') or '/' in row['IP*']:
            continue
        row['team_id'] = tid
        row['name'] = dict(names)[tid] if False else next(
            n for n, t in names if t == tid)
        row['ip'] = pc.ip_to_float(row['IP*'])
        out[tid] = row
    return out


def main():
    args = [a for a in sys.argv[1:] if not a.startswith('--')]
    flags = {a for a in sys.argv[1:] if a.startswith('--')}

    if '--show' in flags:
        cached, at = pc.load_cached_detail(22)
        print(f'cached at {at or "never"} — {len(cached)} teams')
        for tid, d in sorted(cached.items()):
            print(f'  {tid:>3} {d.get("name","?")[:26]:<26} IP {d.get("IP*","?"):>6}')
        return

    names, lid = team_lookup()
    print(f'{len(names)} teams known for league {lid}')

    if args:
        text = io.open(args[0], encoding='utf-8').read()
    else:
        print('\nPaste the matchup page(s), then press Ctrl+Z and Enter:\n')
        text = sys.stdin.read()

    rows = parse(text, names)
    if not rows:
        sys.exit('\nFound no team stat rows. Copy the whole matchup page '
                 '(Ctrl+A, Ctrl+C on the page itself) and paste all of it.')

    week = pc.parse_matchups(pc.get(
        f'https://baseball.fantasysports.yahoo.com/b1/{lid}'), lid)[0]

    print(f'\nparsed {len(rows)} of 12 teams for week {week}:\n')
    print(f'{"team":<26} {"IP":>7} {"vs 50":>8}   pitching values')
    print('-' * 78)
    for tid, d in sorted(rows.items(), key=lambda kv: kv[1]['ip']):
        gap = pc.MIN_IP - d['ip']
        print(f'{d["name"][:26]:<26} {d["IP*"]:>7} '
              f'{("OK" if gap <= 0 else f"-{gap:.2f}"):>8}   '
              + ' '.join(f'{c}={d.get(c,"?")}' for c in ('TB', 'ERA', 'WHIP', 'K/9', 'QS', 'SV+H')))

    missing = [n for n, t in names if t not in rows]
    if missing:
        print(f'\nstill missing: {", ".join(m[:24] for m in missing)}')
        print('(paste those matchup pages too and rerun)')

    if '--dry-run' in flags:
        print('\n(dry run - nothing written)')
        return

    pc.cache_detail(rows, week)
    back, at = pc.load_cached_detail(week)
    print(f'\nwrote {len(back)} teams to the cache at {at}')
    print('The next Lambda run picks these up; force one now with:')
    print('  aws lambda invoke --function-name snapshot-playoff-odds '
          '--payload "{}" --region us-west-2 out.json')


if __name__ == '__main__':
    main()
