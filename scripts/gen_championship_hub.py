"""Build the Championship Hub payload.

Yahoo runs week one of the two-week final; week two (Sept 21-27) is scored
here. This writes the single JSON the hub page reads - rosters, cumulative
category standings, daily lineups, lock times, the frozen free agent pool and
the transaction log.

    python scripts/gen_championship_hub.py --demo 11,12
    python scripts/gen_championship_hub.py --teams 11,12        # the real thing
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except AttributeError:
    pass

from champ_hub import capture, config, mlb_api, players as P, scoring, yahoo_pub as Y

DOCS = Path(__file__).resolve().parent.parent / 'docs'
OUT = DOCS / 'data' / config.SNAPSHOT_JSON

# Yahoo's public API returns every manager nickname as "--hidden--", so the
# mapping is the league's own 2026 team-number table. Team numbers are stable
# within a season (they only shuffle between seasons), and six of these are
# confirmed by team names that never changed: Hatfield Hurlers, Ian Cumsler,
# Moniebol, OG, The Rosterbation Station and the kaomoji.
MANAGERS = {
    1: 'Taylor', 2: 'James', 3: 'Josh', 4: 'Bryant',
    5: 'Kurtis', 6: 'Mark', 7: 'Eric', 8: 'Austin',
    9: 'Greg', 10: 'Mikey', 11: 'Kevin', 12: 'Mike',
}

SLOT_ORDER = ['C', '1B', '2B', '3B', 'SS', 'OF', 'Util', 'SP', 'RP', 'P', 'BN', 'IL', 'NA']


def daterange(start, end):
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


def read_matchups(week):
    """Every matchup in a week, with Yahoo's own running category tally."""
    lg = Y.get('{}/league/{}/scoreboard;week={}?format=json'.format(
        Y.PUB_API, Y.LEAGUE_KEY, week))['fantasy_content']['league']
    board = lg[1]['scoreboard']['0']['matchups']
    if not isinstance(board, dict) or int(board.get('count', 0)) == 0:
        return []

    out = []
    for k in sorted((k for k in board if k.isdigit()), key=int):
        m = board[k]['matchup']
        teams = []
        for j in sorted((j for j in m['0']['teams'] if j.isdigit()), key=int):
            meta = Y.flatten(m['0']['teams'][j]['team'][0])
            teams.append({
                'team_id': int(str(meta['team_key']).rsplit('.', 1)[-1]),
                'team_key': meta['team_key'],
                'name': meta.get('name'),
            })
        # Yahoo publishes who is winning each category; count them per side.
        won = {t['team_key']: 0 for t in teams}
        for sw in m.get('stat_winners', []) or []:
            w = sw.get('stat_winner', {}).get('winner_team_key')
            if w in won:
                won[w] += 1
        for t in teams:
            t['manager'] = MANAGERS.get(t['team_id'])
            t['cats'] = won.get(t['team_key'], 0)
        out.append({
            'week': int(m.get('week', week)),
            'week_start': m.get('week_start'),
            'week_end': m.get('week_end'),
            'status': m.get('status'),
            'is_consolation': str(m.get('is_consolation', '0')) == '1',
            'teams': teams,
        })
    return out


def playoff_rounds():
    """Non-consolation matchups for every playoff week, oldest first."""
    first = config.YAHOO_CHAMPIONSHIP_WEEK - 2
    rounds = []
    for wk in range(first, config.YAHOO_CHAMPIONSHIP_WEEK + 1):
        games = [m for m in read_matchups(wk) if not m['is_consolation']]
        if games:
            rounds.append({'week': wk, 'matchups': games})
    return rounds


def eliminated(rounds):
    """Team ids that have lost a completed bracket game.

    Yahoo does not flag which non-consolation game is the final, and it leaves
    the fifth-place game unflagged too - week 24 carries three of them for a
    six-team bracket. Tracking who has already lost separates the live bracket
    from the placement games without guessing.
    """
    out = set()
    for rnd in rounds:
        for m in rnd['matchups']:
            if m['status'] != 'postevent' or len(m['teams']) != 2:
                continue
            a, b = m['teams']
            if a['cats'] > b['cats']:
                out.add(b['team_id'])
            elif b['cats'] > a['cats']:
                out.add(a['team_id'])
    return out


def detect_finalists():
    """Who is playing for the title, if Yahoo knows yet.

    Yahoo does not create the championship matchup until the semifinals
    resolve, so before then this returns nothing and the hub shows the bracket.
    """
    rounds = playoff_rounds()
    dead = eliminated(rounds)
    for rnd in rounds:
        if rnd['week'] != config.YAHOO_CHAMPIONSHIP_WEEK:
            continue
        for m in rnd['matchups']:
            ids = [t['team_id'] for t in m['teams']]
            if len(ids) == 2 and not (set(ids) & dead):
                return ids
    return None


def live_bracket():
    """The bracket as it stands, with placement games filtered out."""
    rounds = playoff_rounds()
    dead = eliminated(rounds)
    for rnd in rounds:
        keep = []
        for m in rnd['matchups']:
            ids = {t['team_id'] for t in m['teams']}
            # A game between two already-eliminated teams is a placement game.
            m['for_title'] = not (ids <= dead)
            if m['for_title']:
                keep.append(m)
        rnd['matchups'] = keep
    return [r for r in rounds if r['matchups']]


def build_rosters(team_ids, index, abbr_ids, on_date=None):
    """Current roster for each side, bridged to MLB ids."""
    rosters, missing = {}, []
    for tid in team_ids:
        matched, unmatched = P.match(Y.team_roster(tid, date=on_date), index, abbr_ids)
        rosters[tid] = matched
        for u in unmatched:
            missing.append((tid, u['name']))
    return rosters, missing


def score_range(team_ids, rosters, dates, index, abbr_ids):
    """Walk the dates once, accumulating both teams and a per-day trend."""
    totals = {tid: scoring.empty_totals() for tid in team_ids}
    known = {tid: {p['player_key']: p for p in rosters[tid]} for tid in team_ids}
    trend = []
    # Today is deliberately excluded: the browser scores the current day live
    # from MLB box scores, so a stale server-side copy of it never fights with
    # what the page is showing.
    today = config.league_today()
    for d in dates:
        if d >= today:
            break
        print('  scoring {} ...'.format(d), flush=True)
        day_lines = mlb_api.boxscore_lines(d)
        for tid in team_ids:
            # Yahoo remembers what each manager actually started that day.
            day_roster = Y.team_roster(tid, date=d)
            fresh = [p for p in day_roster if p['player_key'] not in known[tid]]
            if fresh:
                fixed, _ = P.match(fresh, index, abbr_ids, use_search=False)
                for p in fixed:
                    known[tid][p['player_key']] = p
            lineup = {p['player_key']: p.get('selected_position') for p in day_roster}
            scoring.accumulate(totals[tid], day_lines, lineup, known[tid])
        snap = {tid: scoring.finalize(totals[tid], weeks=2) for tid in team_ids}
        m = scoring.matchup(snap[team_ids[0]], snap[team_ids[1]])
        trend.append({'date': d, 'a_cats': m['a_cats'], 'b_cats': m['b_cats']})
    return totals, trend


def lock_times(dates):
    """{date: {mlb_team_id: first pitch ISO}} - drives the per-player lock."""
    out = {}
    today = config.league_today()
    for d in dates:
        if d >= today:
            out[d] = {str(k): v for k, v in mlb_api.first_pitch_by_team(d).items()}
    return out


def probables(dates):
    """{date: {mlb_person_id: matchup}} - who is announced to start each day."""
    out = {}
    for d in dates:
        starters = mlb_api.probable_pitchers(d)
        if starters:
            out[d] = {str(k): v for k, v in starters.items()}
    return out


def slim(p):
    """Only the player fields the page renders."""
    return {
        'key': p.get('player_key'), 'name': p.get('name'),
        'team': p.get('team_abbr'), 'pos': p.get('display_position'),
        'eligible': p.get('eligible_positions', []),
        'type': p.get('position_type'), 'mlb_id': p.get('mlb_id'),
        'mlb_team_id': p.get('mlb_team_id'), 'headshot': p.get('headshot'),
        'status': p.get('status'), 'slot': p.get('selected_position'),
    }


def base_rules():
    return {
        'scoring_mode': config.SCORING_MODE,
        'min_ip_total': config.MIN_IP_TOTAL,
        'max_adds': config.MAX_ADDS,
        'drop_lockout_hours': config.DROP_LOCKOUT_HOURS,
        'week1': [config.WEEK1_START, config.WEEK1_END],
        'week2': [config.WEEK2_START, config.WEEK2_END],
        'roster_slots': Y.roster_positions(),
        'slot_order': SLOT_ORDER,
        'categories': scoring.CATS,
        'hitting': scoring.HITTING,
        'pitching': scoring.PITCHING,
        'lower_is_better': sorted(scoring.LOWER_IS_BETTER),
    }


def write_pending(names):
    """The hub before the finalists are known: rules, dates and live bracket."""
    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'status': 'pending',
        'demo': False,
        'rules': base_rules(),
        'bracket': live_bracket(),
        'freeze_at': config.FREEZE_AT,
        'team_names': {str(k): v for k, v in names.items()},
        'managers': {str(k): v for k, v in MANAGERS.items()},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=1), encoding='utf-8')
    print('Wrote {} (bracket only)'.format(OUT))
    for rnd in payload['bracket']:
        print('  Week {}:'.format(rnd['week']))
        for m in rnd['matchups']:
            print('    ' + ' vs '.join(
                '{} ({}) {}'.format(t['manager'] or t['name'], t['team_id'], t['cats'])
                for t in m['teams']) + '   [{}]'.format(m['status']))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--teams', help='the two finalist team ids, e.g. 11,12')
    ap.add_argument('--demo', help='same, but scored over a past window so the UI has real data')
    ap.add_argument('--fa-limit', type=int, default=0, help='0 = the entire pool')
    args = ap.parse_args()

    names = Y.teams()
    spec = args.teams or args.demo
    demo = bool(args.demo)

    if not spec:
        # No teams named: ask Yahoo who is in the final. Until the semifinals
        # resolve it cannot say, so publish the bracket and wait.
        found = detect_finalists()
        if not found:
            print('Yahoo has not set the championship matchup yet — writing the bracket.')
            write_pending(names)
            return
        team_ids = found
        print('Finalists detected from Yahoo: {} vs {}'.format(*[names[t] for t in team_ids]))
    else:
        team_ids = [int(x) for x in spec.split(',')]

    print('Championship Hub: {} vs {}{}'.format(
        names[team_ids[0]], names[team_ids[1]], '  [DEMO]' if demo else ''))

    print('Loading player index ...')
    index = P.mlb_player_index()
    abbr_ids = P.team_abbr_to_id()

    print('Loading rosters ...')
    rosters, missing = build_rosters(team_ids, index, abbr_ids)
    for tid, name in missing:
        print('  WARNING no MLB id for {} (team {})'.format(name, tid))

    if demo:
        # A finished stretch, so the scoreboard renders with genuine numbers.
        w1 = daterange('2026-08-31', '2026-09-06')
        w2 = daterange('2026-09-07', '2026-09-13')
    else:
        w1 = daterange(config.WEEK1_START, config.WEEK1_END)
        w2 = daterange(config.WEEK2_START, config.WEEK2_END)

    print('Scoring the championship window ...')
    totals, trend = score_range(team_ids, rosters, w1 + w2, index, abbr_ids)
    finals = {tid: scoring.finalize(totals[tid], weeks=2) for tid in team_ids}
    board = scoring.matchup(finals[team_ids[0]], finals[team_ids[1]])

    print('Loading free agent pool ...')
    fa_raw = capture.full_free_agent_pool() if args.fa_limit <= 0 else Y.free_agents(limit=args.fa_limit)
    fa, _ = P.match(fa_raw, index, abbr_ids, use_search=False)

    # Before week one, today is a playground test day: give it starters and locks.
    test_days = [config.league_today()] if not demo and config.league_today() < config.WEEK1_START else []

    rules = base_rules()
    rules['week1'] = [w1[0], w1[-1]]
    rules['week2'] = [w2[0], w2[-1]]

    payload = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'status': 'set',
        'demo': demo,
        'rules': rules,
        'sides': {
            'a': {'team_id': team_ids[0], 'name': names[team_ids[0]],
                  'manager': MANAGERS.get(team_ids[0]), 'adds_used': 0},
            'b': {'team_id': team_ids[1], 'name': names[team_ids[1]],
                  'manager': MANAGERS.get(team_ids[1]), 'adds_used': 0},
        },
        'scoreboard': board,
        'totals': {'a': finals[team_ids[0]], 'b': finals[team_ids[1]]},
        # Raw, unfinalised component sums through the last completed day. The
        # page adds today's live components to these and does the ratio maths
        # itself, so a live scoreboard and a final one are the same arithmetic.
        'baseline': {'a': totals[team_ids[0]], 'b': totals[team_ids[1]]},
        'baseline_through': (date.fromisoformat(config.league_today())
                             - timedelta(days=1)).isoformat(),
        'trend': trend,
        'week2_dates': w2,
        'rosters': {'a': [slim(p) for p in rosters[team_ids[0]]],
                    'b': [slim(p) for p in rosters[team_ids[1]]]},
        'lineups': {'a': {}, 'b': {}},
        # Headshots dropped for the pool: 2,000+ URLs the page never shows.
        'free_agents': [{k: v for k, v in slim(p).items() if k != 'headshot'} for p in fa],
        'transactions': [],
        'locks': lock_times(test_days + w1 + w2),
        'probables': probables(test_days + w1 + w2),
    }

    # A demo payload is written alongside the real one, so the page can serve
    # both: the live state, and ?demo for reviewing the populated layout.
    out = OUT.with_name(OUT.stem + '_demo' + OUT.suffix) if demo else OUT
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=1), encoding='utf-8')
    print('\nWrote {} ({:.0f} KB)'.format(out, out.stat().st_size / 1024))
    print('Scoreboard: {} {} - {} {}'.format(
        names[team_ids[0]], board['a_cats'], board['b_cats'], names[team_ids[1]]))


if __name__ == '__main__':
    main()
