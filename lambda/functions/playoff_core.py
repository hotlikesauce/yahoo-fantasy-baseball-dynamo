"""
Shared engine for the live playoff race: fetch the public Yahoo league page,
parse it, work out who has clinched, and simulate the rest of the season.

Imported by snapshot_playoff_odds, the scheduled AWS function that is now the
only thing that computes any of this. The page renders client-side from what
this writes to DynamoDB.

Deliberately dependency-free (urllib + boto3, both already in the Lambda
runtime) so the same file runs locally and in Lambda with no layer attached.
"""

import os, re, json, html, random
from datetime import datetime
from urllib.request import Request, urlopen

try:                                    # Amazon Linux and Windows both have it
    from zoneinfo import ZoneInfo
    LEAGUE_TZ = ZoneInfo('America/Denver')
except Exception:                       # pragma: no cover - no tzdata
    LEAGUE_TZ = None

UA = ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

CATS_PER_WEEK = 12          # 6 batting + 6 pitching categories
SIMS = 100000               # Monte Carlo runs (enough to make a tenth of a
                            # percent mean something; the seed is fixed so the
                            # digit only moves when the data does)
YEAR = 2026

SLOT_HOURS = (0, 12, 15, 18, 21)   # midnight, noon, 3pm, 6pm, 9pm - league time


def now_local():
    """
    League-local time. Lambda runs in UTC, so the slot boundaries have to be
    pinned to a real zone or the 'noon' snapshot lands at 6am; ZoneInfo also
    keeps them honest across the November DST flip, which a fixed UTC offset
    would not.
    """
    return datetime.now(LEAGUE_TZ).replace(tzinfo=None) if LEAGUE_TZ else datetime.now()


# ==============================================================
# Fetch
# ==============================================================
def get(url):
    req = Request(url, headers={'User-Agent': UA})
    with urlopen(req, timeout=30) as r:
        return r.read().decode('utf-8', 'replace')

def clean(s):
    """Unescape entities and collapse whitespace in scraped text."""
    return re.sub(r'\s+', ' ', html.unescape(s)).strip()

# ==============================================================
# Parse: league home page
# ==============================================================

def parse_standings(page, lid):
    """Standings table: rank (* = Yahoo says clinched), team, W-L-T, pct, GB."""
    rows = []
    row_re = re.compile(
        r'<tr class="Linkable[^"]*"[^>]*data-target="/b1/%s/(\d+)">(.*?)</tr>' % lid,
        re.S)
    for m in row_re.finditer(page):
        tid, body = int(m.group(1)), m.group(2)
        cells = [clean(re.sub(r'<[^>]+>', ' ', c))
                 for c in re.findall(r'<td[^>]*>(.*?)</td>', body, re.S)]
        if len(cells) < 5:
            continue
        rank_txt = cells[0]
        wlt = re.match(r'(\d+)-(\d+)-(\d+)', cells[2])
        if not wlt:
            continue
        name_m = re.search(r'/b1/%s/%d">([^<]+)</a>' % (lid, tid), body)
        w, l, t = (int(x) for x in wlt.groups())
        rows.append({
            'team_id': tid,
            'name': clean(name_m.group(1)) if name_m else f'Team {tid}',
            'rank': int(re.sub(r'\D', '', rank_txt) or 0),
            'yahoo_clinched': '*' in rank_txt,
            'wins': w, 'losses': l, 'ties': t,
            'pct': cells[3],
            'gb': cells[4],
            'last_week': cells[5] if len(cells) > 5 else '',
        })
    rows.sort(key=lambda r: r['rank'])
    return rows

def parse_matchups(page, lid):
    """Current-week scoreboard: team ids, names, records and live category wins."""
    blocks = re.split(r"data-target='/b1/%s/matchup\?" % lid, page)[1:]
    week = None
    matchups = []
    for b in blocks:
        head = re.match(r'week=(\d+)&mid1=(\d+)&mid2=(\d+)', b)
        if not head:
            continue
        week = int(head.group(1))
        body = b[:6000]
        names = re.findall(r'/b1/%s/(\d+)">([^<]+)</a>' % lid, body)
        scores = re.findall(r"Fz-lg ?'>\s*(\d+)\s*</div>", body)
        if len(names) < 2 or len(scores) < 2:
            continue
        # records come from the standings table, not the scoreboard card
        sides = [{'team_id': int(names[i][0]),
                  'name': clean(names[i][1]),
                  'live': int(scores[i])} for i in (0, 1)]
        decided = sides[0]['live'] + sides[1]['live']
        matchups.append({
            'week': week,
            'a': sides[0], 'b': sides[1],
            'decided': decided,
            'remaining': max(0, CATS_PER_WEEK - decided),
        })
    return week, matchups

def parse_meta(page):
    status = 'unknown'
    m = re.search(r"""Ta-end Fz-xxs["']>\s*<span[^>]*>([^<]+)</span>""", page, re.S)
    if m:
        status = clean(m.group(1))
    updated = ''
    m = re.search(r'Last standings update:\s*([^<]+)<', page)
    if m:
        updated = clean(m.group(1))
    name = ''
    m = re.search(r'<title>([^<]*?)\s*\|', page)
    if m:
        name = clean(m.group(1))
    return {'status': status, 'yahoo_updated': updated, 'league_name': name}

def parse_playoff_settings(settings_page):
    """'Playoffs: 6 teams - Week 23, 24 and 25 (ends Sunday, Sep 20)'"""
    txt = re.sub(r'(?s)<script.*?</script>', '', settings_page)
    txt = clean(re.sub(r'<[^>]+>', ' ', txt))
    m = re.search(r'Playoffs:\s*(\d+)\s*teams?\s*-\s*Week\s*([\d,\s and]+)', txt)
    if not m:
        return {'spots': 6, 'first_playoff_week': None, 'raw': ''}
    weeks = [int(x) for x in re.findall(r'\d+', m.group(2))]
    return {
        'spots': int(m.group(1)),
        'first_playoff_week': min(weeks) if weeks else None,
        'playoff_weeks': weeks,
        'raw': clean(m.group(0)).replace('Playoffs:', '').strip(),
    }

# ==============================================================
# Managers (DynamoDB - Yahoo does not expose them publicly)
# ==============================================================

def load_managers():
    try:
        import boto3
        tbl = boto3.resource('dynamodb', region_name='us-west-2') \
                   .Table(f'FantasyBaseball-TeamInfo-{YEAR}')
        out = {}
        for item in tbl.scan().get('Items', []):
            mgr = (item.get('ManagerName') or '').strip()
            if mgr:
                out[int(item['TeamNumber'])] = mgr.split()[0].capitalize()
        return out
    except Exception as e:
        print(f'  (no manager names: {e})')
        return {}

# ==============================================================
# Playoff math
# ==============================================================

def week_progress(status, now=None):
    """
    How much of the scoring week is in the books, 0..1. Yahoo weeks run Monday
    through Sunday. Nothing is banked until the week closes: a team up 8-3 on
    Monday night can be down 3-8 by Sunday, so a live lead only counts for as
    much of the week as has actually been played.
    """
    if status.lower().startswith('final') or status.lower() == 'postevent':
        return 1.0
    if 'coming up' in status.lower() or 'pre' in status.lower():
        return 0.0
    now = now or now_local()
    elapsed = now.weekday() * 24 + now.hour + now.minute / 60.0
    return min(1.0, max(0.0, elapsed / (7 * 24)))

def build_teams(standings, matchups, weeks_left_after, spots, progress):
    """
    Merge the season record with this week's live score.

    `progress` is the share of the week already played. While the week is live
    every one of its 12 categories is still winnable, so the floor/ceiling range
    stays wide - only a finished week banks its categories.
    """
    opp, live, decided = {}, {}, {}
    for m in matchups:
        for side, other in ((m['a'], m['b']), (m['b'], m['a'])):
            opp[side['team_id']] = other['team_id']
            live[side['team_id']] = side['live']
            decided[side['team_id']] = m['decided']

    week_over = progress >= 1.0
    teams = {}
    for row in standings:
        tid = row['team_id']
        pts = row['wins'] + 0.5 * row['ties']
        games = row['wins'] + row['losses'] + row['ties']
        banked = live.get(tid, 0) if week_over else 0
        pool = 0 if week_over else CATS_PER_WEEK    # all 12 stay in play
        free = weeks_left_after * CATS_PER_WEEK     # later weeks (opponents unknown)
        teams[tid] = dict(row,
                          pts=pts,
                          games=games,
                          cat_pct=pts / games if games else 0.5,
                          live=live.get(tid, 0),
                          decided=decided.get(tid, 0),
                          banked=banked,
                          pool=pool,
                          free=free,
                          remaining=pool + free,
                          opponent=opp.get(tid),
                          floor=pts + banked,
                          ceiling=pts + banked + pool + free)
    return teams

def teams_above(teams, tid, final, mode):
    """
    How many teams can finish above `final` if everything else breaks the worst
    way ('max') or the best way ('min') for team `tid`.

    A matchup only moves its own two teams, so the extreme can be taken matchup
    by matchup instead of enumerating the whole league. Categories in weeks
    after this one ('free') have no known opponent, so they swing the full
    amount in max mode and none of it in min mode.
    """
    # clinch math counts a tie as a team above (the tiebreak could go either
    # way); elimination math gives the tie to the team being tested.
    beats = (lambda x: x >= final) if mode == 'max' else (lambda x: x > final)
    me = teams[tid]
    my_share = final - me['floor']          # cats I take out of my own matchup

    seen, total = set(), 0
    for a in teams.values():
        aid = a['team_id']
        if aid == tid or aid in seen:
            continue
        seen.add(aid)
        bonus = a['free'] if mode == 'max' else 0
        b = teams.get(a['opponent'])

        if b is None:                        # no matchup this week
            if beats(a['floor'] + (a['pool'] if mode == 'max' else 0) + bonus):
                total += 1
            continue

        if b['team_id'] == tid:              # a is my opponent - its share is forced
            if beats(a['floor'] + max(0, a['pool'] - my_share) + bonus):
                total += 1
            continue

        seen.add(b['team_id'])
        pool = a['pool']
        b_bonus = b['free'] if mode == 'max' else 0
        counts = [int(beats(a['floor'] + share + bonus)) +
                  int(beats(b['floor'] + pool - share + b_bonus))
                  for share in range(pool + 1)]
        total += max(counts) if mode == 'max' else min(counts)
    return total

def clinched(teams, tid, final, spots):
    return teams_above(teams, tid, final, 'max') <= spots - 1

def alive(teams, tid, final, spots):
    return teams_above(teams, tid, final, 'min') <= spots - 1

def playoff_status(teams, spots):
    """Clinched / eliminated / bubble, plus 'cats needed this week' numbers."""
    for t in teams.values():
        tid = t['team_id']
        t['clinched'] = clinched(teams, tid, t['floor'], spots)
        t['eliminated'] = not alive(teams, tid, t['ceiling'], spots)

        # smallest number of the still-undecided categories that locks a spot /
        # that keeps a path alive
        t['magic'] = t['need_alive'] = None
        for extra in range(t['remaining'] + 1):
            if t['magic'] is None and clinched(teams, tid, t['floor'] + extra, spots):
                t['magic'] = extra
            if t['need_alive'] is None and alive(teams, tid, t['floor'] + extra, spots):
                t['need_alive'] = extra
            if t['magic'] is not None and t['need_alive'] is not None:
                break
        if t['eliminated']:
            t['magic'] = t['need_alive'] = None
        if t['clinched']:
            t['need_alive'] = 0

def simulate(teams, spots, progress, sims=SIMS, seed=17):
    """
    Monte Carlo the season out.

    A category currently led is treated as locked in proportion to how much of
    the week has been played, and played out on form for the rest: on Monday
    night an 8-3 lead is barely worth more than a coin flip, by Sunday night it
    is worth almost exactly 8-3. Form is each team's season category win rate,
    compared Bradley-Terry style so a .600 team is not a coin flip against a
    .400 team.
    """
    rng = random.Random(seed)
    ids = list(teams)
    made = {tid: 0 for tid in ids}
    seed_counts = {tid: [0] * len(ids) for tid in ids}
    totals = {tid: 0.0 for tid in ids}

    pairs, seen = [], set()
    for t in teams.values():
        o = teams.get(t['opponent'])
        if o is None or t['team_id'] in seen:
            continue
        seen.update({t['team_id'], o['team_id']})
        sa, sb = t['cat_pct'], o['cat_pct']
        form = sa / (sa + sb) if (sa + sb) else 0.5
        held = progress + (1 - progress) * form        # cats this team leads now
        stolen = (1 - progress) * form                 # cats the opponent leads
        tied = t['pool'] - t['live'] - o['live']
        pairs.append((t['team_id'], o['team_id'], t['pool'],
                      t['live'] if t['pool'] else 0,
                      o['live'] if t['pool'] else 0,
                      max(0, tied) if t['pool'] else 0,
                      held, stolen, form))

    def draw(n, p):
        return sum(1 for _ in range(n) if rng.random() < p)

    for _ in range(sims):
        final = {tid: teams[tid]['floor'] for tid in ids}
        for a, b, pool, a_lead, b_lead, tied, held, stolen, form in pairs:
            if not pool:
                continue
            won = draw(a_lead, held) + draw(b_lead, stolen) + draw(tied, form)
            final[a] += won
            final[b] += pool - won
        for tid in ids:
            t = teams[tid]
            if t['free']:
                final[tid] += draw(t['free'], t['cat_pct'])
        # ties at the cut line go to a coin flip here (the real rule is
        # head-to-head categories, which we cannot see without the API)
        order = sorted(ids, key=lambda x: (-final[x], rng.random()))
        for i, tid in enumerate(order):
            seed_counts[tid][i] += 1
            if i < spots:
                made[tid] += 1
        for tid in ids:
            totals[tid] += final[tid]

    for tid in ids:
        teams[tid]['odds'] = 100.0 * made[tid] / sims
        teams[tid]['projected'] = totals[tid] / sims
        teams[tid]['avg_seed'] = sum((i + 1) * c for i, c in enumerate(seed_counts[tid])) / sims
        teams[tid]['seed_counts'] = seed_counts[tid]

# ==============================================================
# Snapshot slots
# ==============================================================

def odds_value(t):
    """
    The single number the page shows and the chart plots.

    Only the clinch maths earns a round 100: the simulation coming back
    100,000-for-100,000 is not the same thing as being unable to miss, so
    everyone else is held at 99.9 no matter how the rounding falls.
    """
    if t['eliminated']:
        return 0.0
    if t['clinched']:
        return 100.0
    return min(t['odds'], 99.9)

def chart_value(t):
    """
    What the odds-over-time chart plots - the RAW simulated number.

    odds_value() caps a non-clinched team at 99.9 so the table never shows a
    lock a team has not earned. That is a display rule, and baking it into the
    stored series was a mistake: a team sitting at 99.88 -> 99.94 -> 99.91 got
    written as 99.9 three times running and drew a dead-flat line. Clinched and
    eliminated stay pinned, because those two are maths, not presentation.
    """
    if t['eliminated']:
        return 0.0
    if t['clinched']:
        return 100.0
    # A stored 100.0 has to mean "clinched" and nothing else, or the chart
    # cannot tell a proof from 100,000 lucky samples. Hold everyone else a
    # hair below it; the reader sees four decimals, not a rounded lie.
    return min(t['odds'], 99.9999)


def current_slot(now=None):
    """The most recent noon / 3 / 6 / 9 / midnight boundary that has passed."""
    now = now or now_local()
    hour = max(h for h in SLOT_HOURS if h <= now.hour)   # 0 is always eligible
    return now.replace(hour=hour, minute=0, second=0, microsecond=0)

def slot_label(dt):
    hour = dt.hour
    clock = '12a' if hour == 0 else '12p' if hour == 12 else             (f'{hour - 12}p' if hour > 12 else f'{hour}a')
    return f'{dt.month}/{dt.day} {clock}'

def pick_tracked(teams, spots):
    """
    The teams fighting for the last spots - everyone still alive who has not
    already locked one up. Worked out once and then frozen in the history file,
    so a team that clinches mid-race stays on the chart (pinned at 100) instead
    of disappearing off it, and one that gets eliminated flatlines at 0 in view.
    """
    race = [t for t in teams.values() if not t['clinched'] and not t['eliminated']]
    if not race:                                   # everything already decided
        order = sorted(teams.values(), key=lambda t: -odds_value(t))
        race = order[max(0, spots - 2):spots + 2]
    return [t['team_id'] for t in sorted(race, key=lambda t: -odds_value(t))]


# ==============================================================
# One-shot: everything the page and the snapshot both need
# ==============================================================
def collect(lid, sims=SIMS):
    """Scrape, merge, and run the whole playoff picture for league `lid`."""
    base = f'https://baseball.fantasysports.yahoo.com/b1/{lid}'
    home = get(base)
    meta = parse_meta(home)
    standings = parse_standings(home, lid)
    week, matchups = parse_matchups(home, lid)
    if not standings or not matchups:
        raise RuntimeError('could not parse standings/matchups - '
                           'Yahoo markup may have changed')

    settings = parse_playoff_settings(get(f'{base}/settings'))
    spots = settings['spots']
    first_po = settings.get('first_playoff_week')
    last_reg_week = (first_po - 1) if first_po else week
    weeks_left_after = max(0, last_reg_week - week)

    progress = week_progress(meta['status'])
    teams = build_teams(standings, matchups, weeks_left_after, spots, progress)
    for tid, mgr in load_managers().items():
        if tid in teams:
            teams[tid]['manager'] = mgr

    playoff_status(teams, spots)
    simulate(teams, spots, progress, sims=sims)

    return {
        'base': base, 'meta': meta, 'teams': teams, 'matchups': matchups,
        'week': week, 'spots': spots, 'settings': settings,
        'last_regular_week': last_reg_week, 'progress': progress,
    }


# ==============================================================
# Snapshot history (DynamoDB is the record of truth)
# ==============================================================
ODDS_TABLE = os.environ.get('ODDS_TABLE', 'FantasyBaseball-PlayoffOdds')


def odds_table(region=None):
    import boto3
    return boto3.resource(
        'dynamodb',
        region_name=region or os.environ.get('AWS_REGION', 'us-west-2')
    ).Table(ODDS_TABLE)


def history_from_dynamo(year=None, region=None):
    """
    Every snapshot the schedule has written, shaped exactly like the local JSON
    mirror so the renderer cannot tell which one it was handed.
    """
    from boto3.dynamodb.conditions import Key
    year = str(year or YEAR)
    rows = odds_table(region).query(
        KeyConditionExpression=Key('Year').eq(year)).get('Items', [])

    meta, points = {}, []
    for row in rows:
        if row['Slot'] == 'meta':
            meta = row
        elif row['Slot'] != 'current':
            points.append(row)
    points.sort(key=lambda p: p['Slot'])

    return {
        'year': int(year),
        'slot_hours': [int(h) for h in meta.get('slot_hours', SLOT_HOURS)],
        'tracked': [int(t) for t in meta.get('tracked', [])],
        'points': [{
            'slot': p['Slot'],
            'label': p['label'],
            'week': int(p['week']),
            'recorded_at': p.get('recorded_at', ''),
            'odds': {k: float(v) for k, v in p['odds'].items()},
        } for p in points],
    }


# ==============================================================
# Per-category detail + the minimum-innings rule
# ==============================================================
BATTING_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS']
PITCHING_CATS = ['TB', 'ERA', 'WHIP', 'K/9', 'QS', 'SV+H']
ALL_CATS = BATTING_CATS + PITCHING_CATS
LOWER_IS_BETTER = {'TB', 'ERA', 'WHIP'}
MIN_IP = 50.0

_DETAIL_TABLE = re.compile(
    r'<table class="Table-plain Table Table-px-sm Table-mid Datatable.*?</table>', re.S)


def ip_to_float(s):
    """
    Innings pitched are written in baseball notation, not decimal: 49.1 is
    49 and 1/3 innings, and 49.2 is 49 and 2/3. Reading them as 49.1 and 49.2
    would put a team over the line that is actually two outs short of it.
    """
    try:
        whole, _, frac = str(s).strip().partition('.')
        return int(whole) + (int(frac[0]) / 3.0 if frac else 0.0)
    except (ValueError, IndexError):
        return 0.0


def parse_matchup_detail(page, lid):
    """The two stat rows from /matchup - every category, plus IP."""
    tbl = _DETAIL_TABLE.search(page)
    if not tbl:
        return []
    t = tbl.group(0)
    heads = [clean(re.sub(r'<[^>]+>', ' ', h))
             for h in re.findall(r'<th[^>]*>(.*?)</th>', t, re.S)]
    rows = []
    for r in re.findall(r'<tr[^>]*>(.*?)</tr>', t, re.S)[1:]:
        cells = [clean(re.sub(r'<[^>]+>', ' ', c))
                 for c in re.findall(r'<td[^>]*>(.*?)</td>', r, re.S)]
        if len(cells) < len(heads) - 2:
            continue
        d = dict(zip(heads, cells))
        tid = re.search(r'/b1/%s/(\d+)"' % lid, r)
        nm = re.search(r'/b1/%s/\d+">([^<]+)</a>' % lid, r)
        if not tid:
            continue
        d['team_id'] = int(tid.group(1))
        d['name'] = clean(html.unescape(nm.group(1))) if nm else f'Team {tid.group(1)}'
        d['ip'] = ip_to_float(d.get('IP*', 0))
        rows.append(d)
    return rows[:2]


def category_winner(a, b, cat):
    """Which team_id currently leads `cat`, or None if level."""
    try:
        va, vb = float(a[cat]), float(b[cat])
    except (KeyError, ValueError, TypeError):
        return None
    if va == vb:
        return None
    better = (va < vb) if cat in LOWER_IS_BETTER else (va > vb)
    return a['team_id'] if better else b['team_id']


UNREACHABLE_GAP = 12.0      # innings a team cannot make up in one day


def short_ids_for(detail, mode):
    """
    Which teams to treat as failing the minimum.

    'now'       nobody - the scoreboard as Yahoo has it
    'adjusted'  everyone currently under the line, however close
    'likely'    only those too far back to get there. A team two outs short
                will get those outs; a team twenty innings short will not, and
                treating those two the same is what made the blanket view
                credit the wrong manager.
    """
    if mode == 'now':
        return set()
    if mode == 'likely':
        return {t['team_id'] for t in detail.values()
                if MIN_IP - t['ip'] > UNREACHABLE_GAP}
    return {t['team_id'] for t in detail.values() if t['ip'] < MIN_IP}


def score_matchup(a, b, apply_min_ip=False, short_ids=None):
    """
    Categories won by each side.

    With apply_min_ip, a team that has not reached MIN_IP forfeits every
    PITCHING category it currently leads. If BOTH teams are short the category
    goes to nobody rather than swapping hands - neither has earned it, and
    handing it to the other short team would be arbitrary.
    """
    if short_ids is None:
        short_ids = {t['team_id'] for t in (a, b)
                     if apply_min_ip and t['ip'] < MIN_IP}
    short = {a['team_id']: a['team_id'] in short_ids,
             b['team_id']: b['team_id'] in short_ids}
    apply_min_ip = True if short_ids else apply_min_ip
    score = {a['team_id']: 0, b['team_id']: 0}
    per_cat = {}
    for cat in ALL_CATS:
        w = category_winner(a, b, cat)
        if apply_min_ip and w is not None and cat in PITCHING_CATS and short[w]:
            other = b['team_id'] if w == a['team_id'] else a['team_id']
            w = None if short[other] else other
        per_cat[cat] = w
        if w is not None:
            score[w] += 1
    return score, per_cat


def collect_details(base, week, matchups, lid):
    """Fetch every matchup page once and index the stat rows by team id."""
    detail, pairs = {}, []
    for m in matchups:
        a_id, b_id = m['a']['team_id'], m['b']['team_id']
        rows = parse_matchup_detail(
            get(f'{base}/matchup?week={week}&mid1={a_id}&mid2={b_id}'), lid)
        if len(rows) < 2:
            continue
        a, b = rows
        for r in (a, b):
            detail[r['team_id']] = r
        pairs.append((a, b))
    return detail, pairs


def _detail_cache_key(week):
    return f'ipcache#{week}'


def cache_detail(detail, week, region=None):
    """
    Keep the last good matchup scrape.

    Yahoo serves the current week's /matchup pages only intermittently - they
    worked at 08:00 and were empty shells by 10:50 - and that page is the only
    source of per-category values and innings pitched. Losing it must not turn
    every team into "0 IP, 50 short", which is what an empty dict does.
    """
    if not detail:
        return
    try:
        odds_table(region).put_item(Item=json.loads(json.dumps({
            'Year': str(YEAR), 'Slot': _detail_cache_key(week),
            'cached_at': now_local().isoformat(timespec='seconds'),
            'detail': {str(k): v for k, v in detail.items()},
        }, ensure_ascii=False), parse_float=__import__('decimal').Decimal))
    except Exception as e:
        print(f'  (could not cache matchup detail: {e})')


def load_cached_detail(week, region=None):
    """The last good scrape, or ({}, '') when there has never been one."""
    try:
        item = odds_table(region).get_item(
            Key={'Year': str(YEAR), 'Slot': _detail_cache_key(week)}).get('Item')
        if not item:
            return {}, ''
        out = {}
        for k, v in item['detail'].items():
            v = dict(v)
            v['ip'] = float(v.get('ip', 0) or 0)
            v['team_id'] = int(v.get('team_id', k))
            out[int(k)] = v
        return out, item.get('cached_at', '')
    except Exception as e:
        print(f'  (could not read cached matchup detail: {e})')
        return {}, ''


def combo_key(ids):
    """Stable name for a set of forfeiting teams. Empty set is 'none'."""
    return '-'.join(str(i) for i in sorted(ids)) or 'none'


def collect_both(lid, sims=SIMS):
    """
    The whole picture, computed once for every combination of the teams that
    genuinely cannot reach the innings minimum.

    Only teams further than UNREACHABLE_GAP from the line are candidates - a
    side two outs short will get those outs, and pretending otherwise credits
    the wrong manager. With three candidates that is eight scenarios, each
    fully simulated, so the page can toggle any combination without estimating.
    """
    base = f'https://baseball.fantasysports.yahoo.com/b1/{lid}'
    home = get(base)
    meta = parse_meta(home)
    standings = parse_standings(home, lid)
    week, matchups = parse_matchups(home, lid)
    if not standings or not matchups:
        raise RuntimeError('could not parse standings/matchups')

    settings = parse_playoff_settings(get(f'{base}/settings'))
    spots = settings['spots']
    first_po = settings.get('first_playoff_week')
    last_reg_week = (first_po - 1) if first_po else week
    weeks_left_after = max(0, last_reg_week - week)
    progress = week_progress(meta['status'])
    managers = load_managers()

    detail, pairs = collect_details(base, week, matchups, lid)
    if detail:
        ip_source, ip_as_of = 'live', now_local().isoformat(timespec='seconds')
        cache_detail(detail, week)
    else:
        detail, ip_as_of = load_cached_detail(week)
        ip_source = 'cached' if detail else 'unavailable'
        print(f'  matchup pages returned no data; IP source = {ip_source}'
              + (f' (from {ip_as_of})' if ip_as_of else ''))
        # rebuild the pairs from cached rows so scoring still works
        pairs = []
        seen = set()
        for m in matchups:
            a, b = detail.get(m['a']['team_id']), detail.get(m['b']['team_id'])
            if a and b and a['team_id'] not in seen:
                seen.update({a['team_id'], b['team_id']})
                pairs.append((a, b))

    per_cat = {}
    for a, b in pairs:
        _, cats = score_matchup(a, b)
        per_cat[a['team_id']] = cats
        per_cat[b['team_id']] = cats

    # who is even a candidate to forfeit
    candidates = sorted(t['team_id'] for t in detail.values()
                        if t.get('IP*') not in (None, '')
                        and MIN_IP - t['ip'] > UNREACHABLE_GAP)
    if len(candidates) > 4:                     # keep the powerset sane
        candidates = candidates[:4]

    combos = [set()]
    for cid in candidates:
        combos += [c | {cid} for c in combos]

    scenarios = {}
    for shorts in combos:
        live = {}
        for a, b in pairs:
            sc, _ = score_matchup(a, b, short_ids=shorts)
            live.update(sc)

        ms = []
        for m in matchups:
            mm = json.loads(json.dumps(m))
            for side in ('a', 'b'):
                mm[side]['live'] = live.get(mm[side]['team_id'], mm[side]['live'])
            mm['decided'] = mm['a']['live'] + mm['b']['live']
            mm['remaining'] = max(0, CATS_PER_WEEK - mm['decided'])
            ms.append(mm)

        teams = build_teams(standings, ms, weeks_left_after, spots, progress)
        for tid, mgr in managers.items():
            if tid in teams:
                teams[tid]['manager'] = mgr
        for tid, t in teams.items():
            d = detail.get(tid)
            known = d is not None and d.get('IP*') not in (None, '')
            gap = (MIN_IP - d['ip']) if known else None
            # None, not 0. An unknown innings count is not "zero innings", and
            # rendering it as 50-short libels every manager in the league.
            t['ip'] = d['ip'] if known else None
            t['ip_raw'] = d.get('IP*', '') if known else ''
            t['ip_short'] = round(max(0.0, gap), 2) if known else None
            t['meets_min_ip'] = (d['ip'] >= MIN_IP) if known else None
            t['ip_reachable'] = (gap <= UNREACHABLE_GAP) if known else None
            t['is_candidate'] = tid in candidates
            t['forfeits'] = tid in shorts
            t['cats'] = {c: (d or {}).get(c, '') for c in ALL_CATS}
            t['cats_led'] = [c for c, w in per_cat.get(tid, {}).items() if w == tid]
            t['pit_led'] = [c for c in PITCHING_CATS
                            if per_cat.get(tid, {}).get(c) == tid]
        playoff_status(teams, spots)
        simulate(teams, spots, progress, sims=sims)
        scenarios[combo_key(shorts)] = {'teams': teams, 'matchups': ms,
                                        'short_ids': sorted(shorts)}

    none_teams = scenarios['none']['teams']
    cand_info = [{
        'team_id': c,
        'name': none_teams[c]['name'],
        'manager': none_teams[c].get('manager', ''),
        'ip_raw': none_teams[c]['ip_raw'],
        'ip_short': none_teams[c]['ip_short'],
        'pit_led': none_teams[c]['pit_led'],
        'opponent': none_teams[c]['opponent'],
    } for c in candidates]

    return {
        'base': base, 'meta': meta, 'week': week, 'spots': spots,
        'settings': settings, 'last_regular_week': last_reg_week,
        'progress': progress, 'min_ip': MIN_IP,
        'unreachable_gap': UNREACHABLE_GAP,
        'candidates': candidates, 'candidate_info': cand_info,
        'ip_source': ip_source, 'ip_as_of': ip_as_of,
        'all_key': combo_key(candidates),
        'scenarios': scenarios,
    }


# ==============================================================
# 15-minute tracking slots (11am - 10pm league time)
# ==============================================================
SNAP_START_HOUR = 11
SNAP_END_HOUR = 22          # inclusive, on the hour only


def current_slot_15(now=None):
    """
    The 15-minute boundary this run belongs to, or None outside the window.

    Games are not being played at 4am, so tracking round the clock would only
    add flat line. 11am to 10pm is 45 points a day.
    """
    now = now or now_local()
    if now.hour < SNAP_START_HOUR or now.hour > SNAP_END_HOUR:
        return None
    if now.hour == SNAP_END_HOUR and now.minute > 0:
        return None
    return now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)


def slot_label_15(dt):
    h = dt.hour % 12 or 12
    return f'{dt.month}/{dt.day} {h}:{dt.minute:02d}{"a" if dt.hour < 12 else "p"}'
