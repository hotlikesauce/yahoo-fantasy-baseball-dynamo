"""
Pull a whole season out of Yahoo's PUBLIC fantasy API and cache it as one JSON.

Why this exists: the authenticated Yahoo API has been 403-ing app-wide since
July, but `pub-api-rw.fantasysports.yahoo.com` still answers for this league
without any credentials at all. It serves three things nothing else in this
repo has ever had:

  * every transaction of the season (add / drop / trade, with team + timestamp)
  * per-week category values for all 12 teams, innings pitched included
  * Yahoo's own per-category winner for every matchup

That is the raw material for the superlatives board and the playoff hub, so it
is fetched once, written to docs/data/league_activity_2026.json, and every
downstream generator reads the file rather than hammering Yahoo.

Usage:
    python scripts/fetch_league_activity.py            # refresh the cache
    python scripts/fetch_league_activity.py --weeks 22 # stop at a given week
"""

import argparse
import json
import time
from pathlib import Path
from urllib.request import Request, urlopen

YEAR = 2026
GAME_KEY = 469
LEAGUE_ID = '8614'
LEAGUE_KEY = '{}.l.{}'.format(GAME_KEY, LEAGUE_ID)

PUB_API = 'https://pub-api-rw.fantasysports.yahoo.com/fantasy/v2'
UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

OUT = Path(__file__).resolve().parent.parent / 'docs' / 'data' / 'league_activity_{}.json'.format(YEAR)

# Yahoo numbers its categories. These are the league's twelve scored ones plus
# H/AB and IP, which Yahoo displays but does not score - IP matters here anyway,
# because missing 50 in a week forfeits all five pitching categories.
STAT_IDS = {
    60: 'HAB', 7: 'R', 8: 'H', 12: 'HR', 13: 'RBI', 16: 'SB', 55: 'OPS',
    50: 'IP', 49: 'TB', 26: 'ERA', 27: 'WHIP', 57: 'K9', 83: 'QS', 89: 'SVH',
}
SCORED = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']
MIN_IP = 50.0


def get(url, tries=3):
    for attempt in range(tries):
        try:
            req = Request(url, headers={'User-Agent': UA})
            with urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode('utf-8', 'replace'))
        except Exception as e:
            if attempt == tries - 1:
                raise
            print('    retry {} after {}: {}'.format(attempt + 1, type(e).__name__, e))
            time.sleep(2 * (attempt + 1))


def flatten(block):
    """Yahoo nests team/player metadata as a list of single-key dicts."""
    out = {}
    for item in block:
        if isinstance(item, dict):
            out.update(item)
    return out


def ip_to_float(value):
    """Baseball innings: 55.1 is 55 and one third, not 55.1."""
    whole, _, outs = str(value or 0).strip().partition('.')
    try:
        return int(whole or 0) + int(outs or 0) / 3.0
    except ValueError:
        return 0.0


def tid_of(team_key):
    """'469.l.8614.t.7' -> 7"""
    return int(str(team_key).rsplit('.', 1)[-1])


# ==============================================================
# Settings / teams
# ==============================================================
def fetch_meta():
    lg = get('{}/league/{}/settings?format=json'.format(PUB_API, LEAGUE_KEY))['fantasy_content']['league']
    s = lg[1]['settings'][0]
    return {
        'league_name': lg[0].get('name'),
        'season': lg[0].get('season'),
        'current_week': int(lg[0].get('current_week', 0)),
        'start_week': int(lg[0].get('start_week', 1)),
        'end_week': int(lg[0].get('end_week', 0)),
        'playoff_start_week': int(s.get('playoff_start_week', 0)),
        'num_playoff_teams': int(s.get('num_playoff_teams', 6)),
        'reseeding': bool(int(s.get('uses_playoff_reseeding', 0))),
        'trade_end_date': s.get('trade_end_date'),
        'max_weekly_adds': int(s.get('max_weekly_adds', 0)),
    }


def fetch_standings():
    lg = get('{}/league/{}/standings?format=json'.format(PUB_API, LEAGUE_KEY))['fantasy_content']['league']
    teams = lg[1]['standings'][0]['teams']
    out = []
    for i in range(int(teams['count'])):
        t = teams[str(i)]['team']
        info = flatten(t[0])
        st = t[2]['team_standings'] if len(t) > 2 else t[1].get('team_standings', {})
        tot = st.get('outcome_totals', {})
        out.append({
            'team_id': tid_of(info['team_key']),
            'name': info.get('name', ''),
            'rank': int(st.get('rank', 0) or 0),
            'wins': int(tot.get('wins', 0) or 0),
            'losses': int(tot.get('losses', 0) or 0),
            'ties': int(tot.get('ties', 0) or 0),
            'pct': float(tot.get('percentage', 0) or 0),
            'moves': int(info.get('number_of_moves', 0) or 0),
            'trades': int(info.get('number_of_trades', 0) or 0),
        })
    out.sort(key=lambda r: r['rank'] or 99)
    return out


# ==============================================================
# Scoreboards
# ==============================================================
def fetch_week(week):
    """One week: both sides of all six matchups, every category value, plus IP."""
    lg = get('{}/league/{}/scoreboard;week={}?format=json'.format(PUB_API, LEAGUE_KEY, week))
    lg = lg['fantasy_content']['league']
    board = lg[1]['scoreboard']['0']['matchups']
    matchups = []
    for i in range(int(board.get('count', 0))):
        node = board.get(str(i), {}).get('matchup')
        if not node:
            continue
        # Yahoo's own per-category verdict. A category missing from this list is
        # a tie - Yahoo names a winner only when there is one - so the tie count
        # falls out of 12 minus the two win counts rather than being guessed.
        winners = {}
        for sw in node.get('stat_winners', []) or []:
            w = sw.get('stat_winner', {})
            name = STAT_IDS.get(int(w['stat_id']))
            if not name:
                continue
            winners[name] = tid_of(w['winner_team_key']) if w.get('winner_team_key') else None

        sides = []
        teams = node['0']['teams']
        for j in range(int(teams.get('count', 2))):
            t = teams.get(str(j), {}).get('team')
            if not t:
                continue
            info = flatten(t[0])
            row = {'team_id': tid_of(info['team_key']), 'name': info.get('name', ''), 'stats': {}}
            for s in t[1]['team_stats']['stats']:
                name = STAT_IDS.get(int(s['stat']['stat_id']))
                if name:
                    row['stats'][name] = s['stat']['value']
            row['ip'] = round(ip_to_float(row['stats'].get('IP', 0)), 2)
            row['ip_short'] = round(max(0.0, MIN_IP - row['ip']), 2)
            row['forfeit'] = row['ip'] < MIN_IP
            sides.append(row)
        if len(sides) != 2:
            continue

        a, b = sides
        a['cats'] = sum(1 for c in SCORED if winners.get(c) == a['team_id'])
        b['cats'] = sum(1 for c in SCORED if winners.get(c) == b['team_id'])
        matchups.append({
            'week': int(node.get('week', week)),
            'status': node.get('status', ''),
            'week_start': node.get('week_start'),
            'week_end': node.get('week_end'),
            'is_playoffs': node.get('is_playoffs') == '1',
            'is_consolation': node.get('is_consolation') == '1',
            'winner': tid_of(node['winner_team_key']) if node.get('winner_team_key') else None,
            'tied': bool(node.get('is_tied')),
            'stat_winners': winners,
            'a': a, 'b': b,
        })
    return matchups


# ==============================================================
# Transactions
# ==============================================================
def fetch_transactions():
    """
    Every add, drop and trade of the season, normalised to one row per move.

    A Yahoo 'add/drop' transaction carries two players; each is emitted
    separately so an add and the drop that paid for it both count. Trades name
    a source and destination team per player, which is what makes a per-manager
    trade count possible at all.
    """
    lg = get('{}/league/{}/transactions?format=json'.format(PUB_API, LEAGUE_KEY))['fantasy_content']['league']
    node = lg[1]['transactions']
    rows = []
    for i in range(int(node.get('count', 0))):
        tr = node.get(str(i), {}).get('transaction')
        if not tr:
            continue
        head = tr[0]
        if head.get('status') != 'successful':
            continue
        ts = int(head.get('timestamp', 0) or 0)
        players = (tr[1] or {}).get('players', {}) if len(tr) > 1 and isinstance(tr[1], dict) else {}
        # Yahoo serialises an empty players block as [] rather than {}, which
        # commissioner-only transactions (settings edits, roster reinstates) hit.
        if not isinstance(players, dict):
            continue
        for pi in range(int(players.get('count', 0) or 0)):
            p = players.get(str(pi), {}).get('player')
            if not p:
                continue
            pinfo = flatten(p[0])
            data = p[1]['transaction_data']
            if isinstance(data, list):
                data = data[0]
            rows.append({
                'transaction_id': head.get('transaction_id'),
                'type': head.get('type'),               # add / drop / add,drop / trade / commish
                'move': data.get('type'),               # add / drop / trade
                'timestamp': ts,
                'player': (pinfo.get('name') or {}).get('full', ''),
                'position': pinfo.get('display_position', ''),
                'position_type': pinfo.get('position_type', ''),
                'source_type': data.get('source_type'),
                'dest_type': data.get('destination_type'),
                'from_team': tid_of(data['source_team_key']) if data.get('source_team_key') else None,
                'to_team': tid_of(data['destination_team_key']) if data.get('destination_team_key') else None,
                'faab': head.get('faab_bid'),
            })
    rows.sort(key=lambda r: r['timestamp'])
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--weeks', type=int, default=None,
                    help='last week to fetch (default: the current week)')
    args = ap.parse_args()

    print('League {}'.format(LEAGUE_KEY))
    meta = fetch_meta()
    print('  {} - week {} of {}, playoffs start week {} ({} teams, reseeding={})'.format(
        meta['league_name'], meta['current_week'], meta['end_week'],
        meta['playoff_start_week'], meta['num_playoff_teams'], meta['reseeding']))

    last = args.weeks or meta['current_week']
    standings = fetch_standings()
    print('  standings: {} teams'.format(len(standings)))

    weeks = {}
    for w in range(meta['start_week'], last + 1):
        try:
            weeks[str(w)] = fetch_week(w)
            done = sum(1 for m in weeks[str(w)] if m['status'] == 'postevent')
            print('  week {:>2}: {} matchups ({} final)'.format(w, len(weeks[str(w)]), done))
        except Exception as e:
            print('  week {:>2}: FAILED {}: {}'.format(w, type(e).__name__, e))
        time.sleep(0.3)

    tx = fetch_transactions()
    print('  transactions: {} player moves'.format(len(tx)))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        'meta': meta,
        'fetched_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'standings': standings,
        'weeks': weeks,
        'transactions': tx,
    }, indent=1, ensure_ascii=False), encoding='utf-8')
    print('\nWrote {} ({:.0f} KB)'.format(OUT, OUT.stat().st_size / 1024))


if __name__ == '__main__':
    main()
