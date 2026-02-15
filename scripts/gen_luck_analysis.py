import boto3, json, sys, io
from boto3.dynamodb.conditions import Key
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

# Categories
HIGH_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB', 'K9', 'QS', 'SVH']
LOW_CATS = ['ERA', 'WHIP']
ALL_CATS = HIGH_CATS + LOW_CATS

def h2h_result(stats_a, stats_b):
    wins_a = wins_b = 0
    for cat in HIGH_CATS:
        if cat in stats_a and cat in stats_b:
            if stats_a[cat] > stats_b[cat]: wins_a += 1
            elif stats_b[cat] > stats_a[cat]: wins_b += 1
    for cat in LOW_CATS:
        if cat in stats_a and cat in stats_b:
            if stats_a[cat] < stats_b[cat]: wins_a += 1
            elif stats_b[cat] < stats_a[cat]: wins_b += 1
    return wins_a, wins_b

# ============================================================
# 1. Build stable name mapping from power_ranks_season_trend
# ============================================================
name_to_tn = {}
tn_latest_name = {}

for week in range(1, 25):
    response = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'power_ranks_season_trend#{week}')
    )
    if response['Count'] == 0:
        break
    for item in response['Items']:
        tn = item['TeamNumber']
        name_to_tn[item['Team']] = tn
        tn_latest_name[tn] = item['Team']

print("Team mapping:")
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    print(f"  {tn}: {tn_latest_name[tn]}")

# Auto-fill gaps: for any data type/week, if some names don't map, resolve by elimination
all_tns_set = set(str(i) for i in range(1, 13))

def auto_map_week(items_list, week_stats=None, week_results=None):
    mapped = {}
    unmapped_names = []
    for item in items_list:
        name = item.get('Team', '')
        if not name:
            continue
        tn = name_to_tn.get(name)
        if tn:
            mapped[name] = tn
        else:
            unmapped_names.append(name)
    if not unmapped_names:
        return
    used_tns = set(mapped.values())
    missing_tns = sorted(all_tns_set - used_tns, key=int)
    if len(unmapped_names) != len(missing_tns):
        return
    if len(unmapped_names) == 1:
        name_to_tn[unmapped_names[0]] = missing_tns[0]
        print(f"  Auto-mapped: '{unmapped_names[0]}' -> TN {missing_tns[0]}")
    elif len(unmapped_names) >= 2 and week_stats and week_results:
        # Resolve using H2H stat comparison against known matchup results
        resolve_multi_unmapped(unmapped_names, missing_tns, week_stats, week_results)
    else:
        print(f"  WARNING: {len(unmapped_names)} unmapped: {unmapped_names} (need stats to resolve)")

def resolve_multi_unmapped(unmapped_names, missing_tns, week_stats, week_results):
    """Match unmapped names to missing TNs by simulating H2H vs known opponents."""
    stats_by_name = {}
    for item in week_stats:
        s = {}
        for cat in ALL_CATS:
            if cat in item:
                s[cat] = float(item[cat])
        stats_by_name[item['Team']] = s

    # For each missing TN, find their actual opponent and score from weekly_results
    tn_matchups = {}
    for item in week_results:
        team = item['Team']
        tn = name_to_tn.get(team)
        if tn and tn in missing_tns:
            opp_name = item['Opponent']
            opp_tn = name_to_tn.get(opp_name)
            if opp_tn:
                tn_matchups[tn] = {
                    'opp_tn': opp_tn,
                    'actual_score': float(item['Score']),
                    'actual_opp_score': float(item['Opponent_Score'])
                }

    if not tn_matchups:
        print(f"  WARNING: can't resolve {unmapped_names} - no matchup data")
        return

    # Find the opponent's stats in weekly_stats (using mapped names)
    tn_to_stats_name = {}
    for name, tn in name_to_tn.items():
        if name in stats_by_name:
            tn_to_stats_name[tn] = name

    # For each missing TN that has a known matchup, simulate each unmapped candidate
    best_assignment = {}
    for miss_tn, matchup in tn_matchups.items():
        opp_stats_name = tn_to_stats_name.get(matchup['opp_tn'])
        if not opp_stats_name or opp_stats_name not in stats_by_name:
            continue
        opp_stats = stats_by_name[opp_stats_name]
        actual_score = matchup['actual_score']

        best_name = None
        best_diff = 999
        for uname in unmapped_names:
            if uname not in stats_by_name:
                continue
            wa, wb = h2h_result(stats_by_name[uname], opp_stats)
            diff = abs(wa - actual_score)
            if diff < best_diff:
                best_diff = diff
                best_name = uname
        if best_name:
            best_assignment[miss_tn] = best_name

    # Assign best matches, avoiding conflicts
    assigned_names = set()
    assigned_tns = set()
    for miss_tn in missing_tns:
        if miss_tn in best_assignment and best_assignment[miss_tn] not in assigned_names:
            name = best_assignment[miss_tn]
            name_to_tn[name] = miss_tn
            assigned_names.add(name)
            assigned_tns.add(miss_tn)
            print(f"  Auto-mapped (H2H): '{name}' -> TN {miss_tn}")

    # Assign remaining by elimination
    remaining_names = [n for n in unmapped_names if n not in assigned_names]
    remaining_tns = [t for t in missing_tns if t not in assigned_tns]
    for name, tn in zip(remaining_names, remaining_tns):
        name_to_tn[name] = tn
        print(f"  Auto-mapped (elimination): '{name}' -> TN {tn}")

# ============================================================
# 2. Pull weekly_stats for H2H simulation
# ============================================================
weekly_stats = defaultdict(dict)

# First pass: auto-map single unknowns via elimination
for week in range(1, 30):
    response = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}')
    )
    if response['Count'] == 0:
        break
    if response['Count'] == 12:
        auto_map_week(response['Items'])

# Also auto-map from weekly_results
for week in range(1, 30):
    response = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_results#{week}')
    )
    if response['Count'] == 0:
        break
    auto_map_week(response['Items'])

# Second pass: resolve multi-unknowns using H2H comparison
for week in range(1, 30):
    ws_resp = table.query(IndexName='DataTypeWeekIndex', KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
    if ws_resp['Count'] == 0:
        break
    if ws_resp['Count'] < 12:
        continue
    # Check if any names still unmapped
    has_unmapped = any(name_to_tn.get(item['Team']) is None for item in ws_resp['Items'])
    if has_unmapped:
        wr_resp = table.query(IndexName='DataTypeWeekIndex', KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_results#{week}'))
        auto_map_week(ws_resp['Items'], week_stats=ws_resp['Items'], week_results=wr_resp['Items'])

# Second pass: load the stats
for week in range(1, 30):
    response = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}')
    )
    if response['Count'] == 0:
        break
    if response['Count'] < 12:
        print(f"  Skipping week {week} (only {response['Count']} teams)")
        continue
    for item in response['Items']:
        team = item['Team']
        tn = name_to_tn.get(team)
        if tn is None:
            print(f"  WARNING: still unmapped '{team}' in week {week}")
            continue
        stats = {}
        for cat in ALL_CATS:
            if cat in item:
                stats[cat] = float(item[cat])
        weekly_stats[week][tn] = stats

weeks = sorted(weekly_stats.keys())
print(f"\nLoaded {len(weeks)} weeks of stats")

# ============================================================
# 3. Pull weekly_results for actual matchup scores
# ============================================================
actual_results = defaultdict(dict)
for week in range(1, 30):
    response = table.query(
        IndexName='DataTypeWeekIndex',
        KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_results#{week}')
    )
    if response['Count'] == 0:
        break
    seen = set()
    for item in response['Items']:
        team = item['Team']
        tn = name_to_tn.get(team)
        w = int(item['Week'])
        if tn is None or (tn, w) in seen:
            continue
        seen.add((tn, w))
        actual_results[w][tn] = {
            'won': float(item['Score_Difference']) > 0,
            'tied': float(item['Score_Difference']) == 0,
            'cats_won': float(item['Score']),
            'cats_lost': float(item['Opponent_Score']),
            'opp_tn': name_to_tn.get(item['Opponent']),
            'opp_name': item['Opponent'],
            'team_name': team,
            'diff': float(item['Score_Difference'])
        }

# ============================================================
# 4. H2H simulation - all-play
# ============================================================
# Per-week all-play (tracks both matchup W/L and category wins)
weekly_allplay = defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 't': 0, 'cat_w': 0, 'cat_l': 0, 'cat_t': 0}))

for week in weeks:
    tns = list(weekly_stats[week].keys())
    for i, tn_a in enumerate(tns):
        for j, tn_b in enumerate(tns):
            if i == j:
                continue
            wa, wb = h2h_result(weekly_stats[week][tn_a], weekly_stats[week][tn_b])
            ties = 12 - wa - wb
            weekly_allplay[week][tn_a]['cat_w'] += wa
            weekly_allplay[week][tn_a]['cat_l'] += wb
            weekly_allplay[week][tn_a]['cat_t'] += ties
            if wa > wb:
                weekly_allplay[week][tn_a]['w'] += 1
            elif wb > wa:
                weekly_allplay[week][tn_a]['l'] += 1
            else:
                weekly_allplay[week][tn_a]['t'] += 1

# ============================================================
# 5. Build weekly luck data
# ============================================================
# For each week + team: all-play record, actual matchup score, luck tag
weekly_luck_data = []
team_luck_counts = defaultdict(lambda: {'lucky_w': 0, 'unlucky_l': 0, 'total_w': 0, 'total_l': 0})

for week in weeks:
    for tn in sorted(weekly_allplay[week].keys(), key=lambda x: int(x)):
        ap = weekly_allplay[week][tn]
        ap_total = ap['w'] + ap['l'] + ap['t']
        if ap_total == 0:
            continue
        ap_pct = (ap['w'] + ap['t'] * 0.5) / ap_total
        ap_rank = 1  # rank among all teams this week (1 = best all-play)

        # Count how many teams had better all-play this week
        for other_tn in weekly_allplay[week]:
            if other_tn == tn:
                continue
            other_ap = weekly_allplay[week][other_tn]
            other_pct = (other_ap['w'] + other_ap['t'] * 0.5) / (other_ap['w'] + other_ap['l'] + other_ap['t'])
            if other_pct > ap_pct:
                ap_rank += 1

        actual = actual_results.get(week, {}).get(tn)
        if actual:
            cats_w = int(actual['cats_won'])
            cats_l = int(actual['cats_lost'])
            won = actual['won']
            opp_tn = actual['opp_tn']
            opp_name = actual['opp_name']

            # Luck classification:
            # Lucky win: won matchup but bottom-half all-play (rank 7-12)
            # Unlucky loss: lost matchup but top-half all-play (rank 1-6)
            luck_tag = ''
            if won and ap_rank >= 7:
                luck_tag = 'lucky_win'
                team_luck_counts[tn]['lucky_w'] += 1
            elif not won and ap_rank <= 6:
                luck_tag = 'unlucky_loss'
                team_luck_counts[tn]['unlucky_l'] += 1

            if won:
                team_luck_counts[tn]['total_w'] += 1
            else:
                team_luck_counts[tn]['total_l'] += 1

            weekly_luck_data.append({
                'week': week,
                'tn': tn,
                'name': tn_latest_name.get(tn, tn),
                'ap_w': ap['w'], 'ap_l': ap['l'], 'ap_t': ap['t'],
                'ap_pct': ap_pct,
                'ap_rank': ap_rank,
                'cats_w': cats_w, 'cats_l': cats_l,
                'won': won,
                'opp_name': opp_name,
                'luck_tag': luck_tag
            })

# ============================================================
# 6. Summary: luck coefficient per team
# ============================================================
# Luck coefficient = (lucky wins - unlucky losses) / total games
team_summary = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    lc = team_luck_counts[tn]
    matchup_total = lc['total_w'] + lc['total_l']
    lucky_w = lc['lucky_w']
    unlucky_l = lc['unlucky_l']
    luck_coeff = (lucky_w - unlucky_l) / matchup_total if matchup_total > 0 else 0

    # Season record = total category W-L-T across all weeks (each week has 12 cats)
    cat_w = cat_l = cat_t = 0
    for w in weeks:
        if w in actual_results and tn in actual_results[w]:
            cw = int(actual_results[w][tn]['cats_won'])
            cl = int(actual_results[w][tn]['cats_lost'])
            ct = 12 - cw - cl  # ties = remaining cats
            cat_w += cw
            cat_l += cl
            cat_t += ct

    # All-play season totals
    total_ap_w = sum(weekly_allplay[w][tn]['w'] for w in weeks)
    total_ap_l = sum(weekly_allplay[w][tn]['l'] for w in weeks)
    total_ap_t = sum(weekly_allplay[w][tn]['t'] for w in weeks)
    ap_total = total_ap_w + total_ap_l + total_ap_t
    xwin_pct = (total_ap_w + total_ap_t * 0.5) / ap_total if ap_total > 0 else 0

    # xWins = expected category wins from all-play simulation
    # For each week: avg category wins across all 11 simulated opponents
    expected_cat_w = 0
    expected_cat_l = 0
    expected_cat_t = 0
    weeks_with_results = 0
    for w in weeks:
        if w in actual_results and tn in actual_results[w]:
            ap = weekly_allplay[w][tn]
            n_opps = ap['w'] + ap['l'] + ap['t']  # number of opponents simulated
            if n_opps > 0:
                expected_cat_w += ap['cat_w'] / n_opps
                expected_cat_l += ap['cat_l'] / n_opps
                expected_cat_t += ap['cat_t'] / n_opps
                weeks_with_results += 1

    diff = cat_w - expected_cat_w

    team_summary.append({
        'tn': tn,
        'name': tn_latest_name.get(tn, tn),
        'cat_w': cat_w, 'cat_l': cat_l, 'cat_t': cat_t,
        'lucky_w': lucky_w, 'unlucky_l': unlucky_l,
        'luck_coeff': luck_coeff,
        'xwin_pct': xwin_pct,
        'expected_cat_w': round(expected_cat_w, 1),
        'expected_cat_l': round(expected_cat_l, 1),
        'expected_cat_t': round(expected_cat_t, 1),
        'diff': round(diff, 1),
        'allplay_w': total_ap_w, 'allplay_l': total_ap_l, 'allplay_t': total_ap_t,
    })

team_summary.sort(key=lambda x: x['luck_coeff'], reverse=True)

print("\n=== LUCK COEFFICIENT ===")
for s in team_summary:
    print(f"  {s['name']:35s} Record: {s['cat_w']}-{s['cat_l']}  xWins: {s['expected_cat_w']}  Diff: {s['diff']:+.1f}  Coeff: {s['luck_coeff']:+.3f}  xW%: {s['xwin_pct']:.3f}")

# ============================================================
# 7. Blowouts and closest matchups
# ============================================================
all_matchups = []
for week in weeks:
    for tn, result in actual_results[week].items():
        if result['diff'] > 0:
            all_matchups.append({
                'week': week, 'winner': result['team_name'],
                'loser': result['opp_name'],
                'w_cats': int(result['cats_won']), 'l_cats': int(result['cats_lost']),
                'margin': result['diff']
            })

blowouts = sorted(all_matchups, key=lambda x: x['margin'], reverse=True)
closest = sorted(all_matchups, key=lambda x: x['margin'])

# ============================================================
# 8. Best/Worst Matchups (by combined weekly xWins)
# ============================================================
matchup_quality = []
for week in weeks:
    seen_pairs = set()
    for tn, result in actual_results[week].items():
        opp_tn = result['opp_tn']
        if opp_tn is None:
            continue
        pair = tuple(sorted([tn, opp_tn]))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        ap_a = weekly_allplay[week].get(tn)
        ap_b = weekly_allplay[week].get(opp_tn)
        if not ap_a or not ap_b:
            continue
        n_a = ap_a['w'] + ap_a['l'] + ap_a['t']
        n_b = ap_b['w'] + ap_b['l'] + ap_b['t']
        xwins_a = ap_a['cat_w'] / n_a if n_a > 0 else 0
        xwins_b = ap_b['cat_w'] / n_b if n_b > 0 else 0
        combined = xwins_a + xwins_b

        # Figure out who won
        if result['diff'] > 0:
            w_name, l_name = result['team_name'], result['opp_name']
            w_xw, l_xw = xwins_a, xwins_b
            score = f"{int(result['cats_won'])}-{int(result['cats_lost'])}"
        elif result['diff'] < 0:
            w_name, l_name = result['opp_name'], result['team_name']
            w_xw, l_xw = xwins_b, xwins_a
            score = f"{int(result['cats_lost'])}-{int(result['cats_won'])}"
        else:
            w_name, l_name = result['team_name'], result['opp_name']
            w_xw, l_xw = xwins_a, xwins_b
            score = f"{int(result['cats_won'])}-{int(result['cats_lost'])}"

        matchup_quality.append({
            'week': week,
            'winner': w_name, 'loser': l_name,
            'w_xw': round(w_xw, 1), 'l_xw': round(l_xw, 1),
            'combined': round(combined, 1),
            'score': score,
            'tied': result['diff'] == 0,
        })

best_matchups = sorted(matchup_quality, key=lambda x: x['combined'], reverse=True)
worst_matchups = sorted(matchup_quality, key=lambda x: x['combined'])

# ============================================================
# 9. Schedule Strength
# ============================================================
schedule_strength = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    opp_xw_sum = 0
    opp_count = 0
    opp_cat_w_total = 0
    for w in weeks:
        if w in actual_results and tn in actual_results[w]:
            opp_tn = actual_results[w][tn]['opp_tn']
            if opp_tn and opp_tn in weekly_allplay[w]:
                oap = weekly_allplay[w][opp_tn]
                n = oap['w'] + oap['l'] + oap['t']
                if n > 0:
                    opp_xw_sum += (oap['w'] + oap['t'] * 0.5) / n
                    opp_cat_w_total += oap['cat_w'] / n
                    opp_count += 1
    avg_opp_xw = opp_xw_sum / opp_count if opp_count > 0 else 0
    avg_opp_cats = opp_cat_w_total / opp_count if opp_count > 0 else 0
    ts = next((s for s in team_summary if s['tn'] == tn), None)
    schedule_strength.append({
        'tn': tn,
        'name': tn_latest_name.get(tn, tn),
        'avg_opp_xw': avg_opp_xw,
        'avg_opp_cats': round(avg_opp_cats, 1),
        'cat_w': ts['cat_w'] if ts else 0,
        'cat_l': ts['cat_l'] if ts else 0,
        'cat_t': ts['cat_t'] if ts else 0,
        'expected_cat_w': ts['expected_cat_w'] if ts else 0,
        'diff': ts['diff'] if ts else 0,
    })

schedule_strength.sort(key=lambda x: x['avg_opp_xw'], reverse=True)

# ============================================================
# 10. What-If All-Play Standings vs Actual Standings
# ============================================================
actual_standings = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    mw = ml = mt = 0
    for w in weeks:
        if w in actual_results and tn in actual_results[w]:
            r = actual_results[w][tn]
            if r['won']:
                mw += 1
            elif r['tied']:
                mt += 1
            else:
                ml += 1
    actual_standings.append({
        'tn': tn,
        'name': tn_latest_name.get(tn, tn),
        'matchup_w': mw, 'matchup_l': ml, 'matchup_t': mt,
        'matchup_pct': (mw + mt * 0.5) / (mw + ml + mt) if (mw + ml + mt) > 0 else 0,
    })
actual_standings.sort(key=lambda x: x['matchup_pct'], reverse=True)
actual_rank_map = {s['tn']: i + 1 for i, s in enumerate(actual_standings)}

allplay_standings = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    ts = next((s for s in team_summary if s['tn'] == tn), None)
    if not ts:
        continue
    total = ts['allplay_w'] + ts['allplay_l'] + ts['allplay_t']
    pct = (ts['allplay_w'] + ts['allplay_t'] * 0.5) / total if total > 0 else 0
    actual = next((a for a in actual_standings if a['tn'] == tn), None)
    allplay_standings.append({
        'tn': tn,
        'name': ts['name'],
        'ap_w': ts['allplay_w'], 'ap_l': ts['allplay_l'], 'ap_t': ts['allplay_t'],
        'ap_pct': pct,
        'matchup_w': actual['matchup_w'] if actual else 0,
        'matchup_l': actual['matchup_l'] if actual else 0,
        'matchup_t': actual['matchup_t'] if actual else 0,
    })
allplay_standings.sort(key=lambda x: x['ap_pct'], reverse=True)
for i, s in enumerate(allplay_standings):
    s['ap_rank'] = i + 1
    s['actual_rank'] = actual_rank_map.get(s['tn'], 0)
    s['rank_diff'] = s['actual_rank'] - s['ap_rank']  # positive = actual rank worse than deserved

# ============================================================
# 11. Weekly Dominators & Consistency
# ============================================================
weekly_mvps = defaultdict(lambda: {'first': 0, 'top3': 0, 'last': 0})
weekly_pcts = defaultdict(list)

for week in weeks:
    week_ap = []
    for tn in weekly_allplay[week]:
        ap = weekly_allplay[week][tn]
        total = ap['w'] + ap['l'] + ap['t']
        if total > 0:
            pct = (ap['w'] + ap['t'] * 0.5) / total
            week_ap.append((tn, pct))
            weekly_pcts[tn].append(pct)
    week_ap.sort(key=lambda x: x[1], reverse=True)
    if len(week_ap) >= 1:
        weekly_mvps[week_ap[0][0]]['first'] += 1
    for idx in range(min(3, len(week_ap))):
        weekly_mvps[week_ap[idx][0]]['top3'] += 1
    if len(week_ap) >= 1:
        weekly_mvps[week_ap[-1][0]]['last'] += 1

# Consistency = standard deviation of weekly all-play win%
import statistics
dominator_data = []
for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
    d = weekly_mvps[tn]
    pcts = weekly_pcts.get(tn, [])
    avg_pct = sum(pcts) / len(pcts) if pcts else 0
    stdev = statistics.stdev(pcts) if len(pcts) > 1 else 0
    dominator_data.append({
        'tn': tn,
        'name': tn_latest_name.get(tn, tn),
        'first': d['first'],
        'top3': d['top3'],
        'last': d['last'],
        'avg_pct': avg_pct,
        'stdev': stdev,
    })
dominator_data.sort(key=lambda x: x['first'], reverse=True)

# ============================================================
# 12. Generate HTML
# ============================================================
colors = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]
by_xwin = sorted(team_summary, key=lambda x: x['xwin_pct'], reverse=True)
color_map = {s['tn']: colors[i % len(colors)] for i, s in enumerate(by_xwin)}

# Luck coefficient table
luck_rows = ""
for i, s in enumerate(team_summary):
    color = color_map.get(s['tn'], '#94a3b8')
    coeff_class = "lucky" if s['luck_coeff'] > 0 else "unlucky"
    coeff_val = f"+{s['luck_coeff']:.3f}" if s['luck_coeff'] > 0 else f"{s['luck_coeff']:.3f}"
    ap = f"{s['allplay_w']}-{s['allplay_l']}"
    if s['allplay_t'] > 0: ap += f"-{s['allplay_t']}"
    record = f"{s['cat_w']}-{s['cat_l']}"
    if s['cat_t'] > 0: record += f"-{s['cat_t']}"
    xrecord = f"{s['expected_cat_w']}-{s['expected_cat_l']}"
    if s['expected_cat_t'] > 0.5: xrecord += f"-{s['expected_cat_t']}"
    diff_class = "lucky" if s['diff'] > 0 else "unlucky"
    diff_val = f"+{s['diff']}" if s['diff'] > 0 else str(s['diff'])
    luck_rows += f'<tr><td class="rank">{i+1}</td><td><span style="color:{color}">&#9679;</span> {s["name"]}</td><td>{record}</td><td>{xrecord}</td><td class="{diff_class}">{diff_val}</td><td class="lucky">{s["lucky_w"]}</td><td class="unlucky">{s["unlucky_l"]}</td><td class="{coeff_class}">{coeff_val}</td><td>{ap}</td><td>{s["xwin_pct"]:.3f}</td></tr>'

# Weekly breakdown - grouped by week
weekly_rows = ""
for week in weeks:
    week_entries = [d for d in weekly_luck_data if d['week'] == week]
    week_entries.sort(key=lambda x: x['ap_pct'], reverse=True)

    for rank_i, d in enumerate(week_entries):
        ap_str = f"{d['ap_w']}-{d['ap_l']}"
        if d['ap_t'] > 0: ap_str += f"-{d['ap_t']}"
        actual_str = f"{'W' if d['won'] else 'L'} {d['cats_w']}-{d['cats_l']}"
        actual_class = "lucky" if d['won'] else "unlucky"

        hl = ""
        if d['luck_tag'] == 'lucky_win':
            hl = ' class="lucky-highlight"'
        elif d['luck_tag'] == 'unlucky_loss':
            hl = ' class="unlucky-highlight"'

        luck_icon = ""
        if d['luck_tag'] == 'lucky_win':
            luck_icon = '<span class="lucky">&#x2191;</span>'
        elif d['luck_tag'] == 'unlucky_loss':
            luck_icon = '<span class="unlucky">&#x2193;</span>'

        color = color_map.get(d['tn'], '#94a3b8')
        opp_short = d['opp_name'][:20]
        weekly_rows += f'<tr{hl}><td class="year">Wk {d["week"]}</td><td class="rank">#{d["ap_rank"]}</td><td><span style="color:{color}">&#9679;</span> {d["name"]}</td><td>{ap_str}</td><td class="{actual_class}">{actual_str}</td><td>vs {opp_short}</td><td>{luck_icon}</td></tr>'

# Blowout/closest tables
blowout_rows = ""
for i, m in enumerate(blowouts[:10]):
    blowout_rows += f'<tr><td class="rank">{i+1}</td><td class="year">Wk {m["week"]}</td><td>{m["winner"]}</td><td class="lucky">{m["w_cats"]}-{m["l_cats"]}</td><td>{m["loser"]}</td></tr>'

closest_rows = ""
for i, m in enumerate(closest[:10]):
    closest_rows += f'<tr><td class="rank">{i+1}</td><td class="year">Wk {m["week"]}</td><td>{m["winner"]}</td><td class="close-margin">{m["w_cats"]}-{m["l_cats"]}</td><td>{m["loser"]}</td></tr>'

# Best/worst matchup rows
best_matchup_rows = ""
for i, m in enumerate(best_matchups[:10]):
    color_w = color_map.get(name_to_tn.get(m['winner']), '#94a3b8')
    color_l = color_map.get(name_to_tn.get(m['loser']), '#94a3b8')
    result_str = f"{'T' if m['tied'] else ''} {m['score']}" if m['tied'] else m['score']
    best_matchup_rows += f'<tr><td class="rank">{i+1}</td><td class="year">Wk {m["week"]}</td><td><span style="color:{color_w}">&#9679;</span> {m["winner"]}</td><td><span style="color:{color_l}">&#9679;</span> {m["loser"]}</td><td class="score">{result_str}</td><td>{m["w_xw"]}</td><td>{m["l_xw"]}</td><td class="lucky">{m["combined"]}</td></tr>'

worst_matchup_rows = ""
for i, m in enumerate(worst_matchups[:10]):
    color_w = color_map.get(name_to_tn.get(m['winner']), '#94a3b8')
    color_l = color_map.get(name_to_tn.get(m['loser']), '#94a3b8')
    result_str = f"{'T' if m['tied'] else ''} {m['score']}" if m['tied'] else m['score']
    worst_matchup_rows += f'<tr><td class="rank">{i+1}</td><td class="year">Wk {m["week"]}</td><td><span style="color:{color_w}">&#9679;</span> {m["winner"]}</td><td><span style="color:{color_l}">&#9679;</span> {m["loser"]}</td><td class="score">{result_str}</td><td>{m["w_xw"]}</td><td>{m["l_xw"]}</td><td class="unlucky">{m["combined"]}</td></tr>'

# Schedule strength rows
sched_rows = ""
for i, s in enumerate(schedule_strength):
    color = color_map.get(s['tn'], '#94a3b8')
    record = f"{s['cat_w']}-{s['cat_l']}"
    if s['cat_t'] > 0: record += f"-{s['cat_t']}"
    diff_class = "lucky" if s['diff'] > 0 else "unlucky"
    diff_val = f"+{s['diff']}" if s['diff'] > 0 else str(s['diff'])
    sched_rows += f'<tr><td class="rank">{i+1}</td><td><span style="color:{color}">&#9679;</span> {s["name"]}</td><td>{s["avg_opp_xw"]:.3f}</td><td>{s["avg_opp_cats"]}</td><td>{record}</td><td>{s["expected_cat_w"]}</td><td class="{diff_class}">{diff_val}</td></tr>'

# What-if standings rows
whatif_rows = ""
for i, s in enumerate(allplay_standings):
    color = color_map.get(s['tn'], '#94a3b8')
    ap_str = f"{s['ap_w']}-{s['ap_l']}"
    if s['ap_t'] > 0: ap_str += f"-{s['ap_t']}"
    matchup_str = f"{s['matchup_w']}-{s['matchup_l']}"
    if s['matchup_t'] > 0: matchup_str += f"-{s['matchup_t']}"
    rd = s['rank_diff']
    if rd > 0:
        diff_str = f'<span class="unlucky">&#x2193;{rd}</span>'
    elif rd < 0:
        diff_str = f'<span class="lucky">&#x2191;{abs(rd)}</span>'
    else:
        diff_str = '<span style="color:#64748b">&#8212;</span>'
    whatif_rows += f'<tr><td class="rank">{s["ap_rank"]}</td><td><span style="color:{color}">&#9679;</span> {s["name"]}</td><td>{ap_str}</td><td>{s["ap_pct"]:.3f}</td><td>{s["actual_rank"]}</td><td>{matchup_str}</td><td>{diff_str}</td></tr>'

# Dominator rows
dom_rows = ""
for i, d in enumerate(dominator_data):
    color = color_map.get(d['tn'], '#94a3b8')
    consistency = "Very High" if d['stdev'] < 0.10 else "High" if d['stdev'] < 0.15 else "Medium" if d['stdev'] < 0.22 else "Low"
    con_class = "lucky" if d['stdev'] < 0.10 else "score" if d['stdev'] < 0.15 else "" if d['stdev'] < 0.22 else "unlucky"
    dom_rows += f'<tr><td class="rank">{i+1}</td><td><span style="color:{color}">&#9679;</span> {d["name"]}</td><td class="lucky">{d["first"]}</td><td>{d["top3"]}</td><td class="unlucky">{d["last"]}</td><td>{d["avg_pct"]:.3f}</td><td>{d["stdev"]:.3f}</td><td class="{con_class}">{consistency}</td></tr>'

# Charts
luck_labels = json.dumps([s['name'].replace('\\', '\\\\') for s in team_summary])
luck_values = json.dumps([s['luck_coeff'] for s in team_summary])
luck_bar_colors = json.dumps(['#22c55e' if s['luck_coeff'] > 0 else '#ef4444' for s in team_summary])

# Cumulative all-play win% trend
xwin_datasets = []
for s in by_xwin:
    tn = s['tn']
    label = s['name'].replace('\\', '\\\\').replace("'", "\\'")
    color = color_map[tn]
    cum_w = cum_total = 0
    points = []
    for w in weeks:
        ap = weekly_allplay[w].get(tn, {'w': 0, 'l': 0, 't': 0})
        cum_w += ap['w'] + ap['t'] * 0.5
        cum_total += ap['w'] + ap['l'] + ap['t']
        pct = (cum_w / cum_total * 100) if cum_total > 0 else 50
        points.append(round(pct, 1))
    xwin_datasets.append(f"""{{
      label: '{label}',
      data: {json.dumps(points)},
      borderColor: '{color}',
      backgroundColor: '{color}22',
      borderWidth: 2.5,
      pointRadius: 3,
      pointHoverRadius: 6,
      tension: 0.3,
      fill: false
    }}""")

xwin_ds = ',\n      '.join(xwin_datasets)
week_labels = json.dumps([f'Wk {w}' for w in weeks])

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>&#x26be;</text></svg>">
<title>Summertime Sadness - 2025 Luck & Matchup Analysis</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0f172a; color: #e2e8f0; font-family: 'Segoe UI', system-ui, sans-serif; padding: 24px; }}
  .container {{ max-width: 1400px; margin: 0 auto; }}
  h1 {{ text-align: center; font-size: 2em; margin-bottom: 4px; background: linear-gradient(135deg, #3b82f6, #8b5cf6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
  h2 {{ text-align: center; color: #64748b; font-size: 1.1em; margin-bottom: 24px; }}
  h3 {{ color: #38bdf8; margin: 32px 0 12px; font-size: 1.3em; border-bottom: 1px solid #1e293b; padding-bottom: 8px; }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; }}
  th {{ text-align: left; padding: 10px 12px; border-bottom: 2px solid #334155; color: #94a3b8; font-size: 0.85em; text-transform: uppercase; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #1e293b; font-size: 0.95em; }}
  tr:hover td {{ background: #1e293b; }}
  .rank {{ color: #64748b; font-weight: 600; width: 40px; }}
  .score {{ font-weight: 700; color: #34d399; }}
  .year {{ color: #a78bfa; font-weight: 600; }}
  .lucky {{ color: #22c55e; font-weight: 700; }}
  .unlucky {{ color: #ef4444; font-weight: 700; }}
  .close-margin {{ color: #f59e0b; font-weight: 700; }}
  .lucky-highlight td {{ background: #22c55e15; }}
  .unlucky-highlight td {{ background: #ef444415; }}
  .chart-box {{ background: #1e293b; border-radius: 12px; padding: 24px; margin-bottom: 24px; position: relative; height: 550px; }}
  .chart-box-tall {{ background: #1e293b; border-radius: 12px; padding: 24px; margin-bottom: 24px; position: relative; height: 400px; }}
  .section-desc {{ color: #94a3b8; font-size: 0.9em; margin-bottom: 16px; }}
  .weekly-table {{ max-height: 700px; overflow-y: auto; margin-bottom: 24px; }}
  .weekly-table table {{ margin-bottom: 0; }}
  .weekly-table th {{ position: sticky; top: 0; background: #0f172a; z-index: 1; }}
  th.sortable {{ cursor: pointer; user-select: none; position: relative; padding-right: 18px; }}
  th.sortable:hover {{ color: #e2e8f0; }}
  th.sortable::after {{ content: '\\2195'; position: absolute; right: 4px; opacity: 0.3; font-size: 0.8em; }}
  th.sortable.asc::after {{ content: '\\2191'; opacity: 0.8; }}
  th.sortable.desc::after {{ content: '\\2193'; opacity: 0.8; }}
</style>
</head>
<body>
<div id="nav"></div>
<script src="nav.js"></script>
<div class="container">
<h1>Summertime Sadness Fantasy Baseball</h1>
<h2>2025 Luck & Matchup Analysis</h2>

<h3>Luck Coefficient</h3>
<p class="section-desc">Record = total category W-L-T across all weeks (12 categories per week). xWins = expected category wins based on all-play H2H strength. Diff = actual cat wins - xWins. A <span class="lucky">Lucky Win</span> = won your weekly matchup despite being bottom-half in all-play (ranked 7-12). An <span class="unlucky">Unlucky Loss</span> = lost despite top-half all-play. Luck Coefficient = (Lucky Wins - Unlucky Losses) / Total Matchups.</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="wlt">Record (W-L-T)</th><th class="sortable" data-type="wlt">xRecord</th><th class="sortable" data-type="num">Diff</th><th class="sortable" data-type="num">Lucky W</th><th class="sortable" data-type="num">Unlucky L</th><th class="sortable" data-type="num">Coeff</th><th class="sortable" data-type="wlt">All-Play</th><th class="sortable" data-type="num">xW%</th></tr>
{luck_rows}
</table>

<div class="chart-box-tall">
<canvas id="luckChart"></canvas>
</div>

<h3>Weekly Matchup Breakdown</h3>
<p class="section-desc">Every matchup by week, ranked by all-play performance. <span style="background:#22c55e15; padding:2px 6px; border-radius:4px;">Green</span> = lucky win (bottom-half all-play but won). <span style="background:#ef444415; padding:2px 6px; border-radius:4px;">Red</span> = unlucky loss (top-half all-play but lost). Category scores show the actual H2H result (e.g. W 7-5 = won 7 categories, lost 5).</p>
<div class="weekly-table">
<table>
<tr><th>Week</th><th>AP Rank</th><th>Team</th><th>All-Play</th><th>Result</th><th>Opponent</th><th></th></tr>
{weekly_rows}
</table>
</div>

<h3>All-Play Win% Trend</h3>
<p class="section-desc">Cumulative all-play win percentage over the season.</p>
<div class="chart-box">
<canvas id="xwinChart"></canvas>
</div>

<h3>Biggest Blowouts</h3>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="num">Week</th><th class="sortable" data-type="str">Winner</th><th class="sortable" data-type="wlt">Score</th><th class="sortable" data-type="str">Loser</th></tr>
{blowout_rows}
</table>

<h3>Closest Matchups</h3>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="num">Week</th><th class="sortable" data-type="str">Winner</th><th class="sortable" data-type="wlt">Score</th><th class="sortable" data-type="str">Loser</th></tr>
{closest_rows}
</table>

<h3>Best Matchups of the Year</h3>
<p class="section-desc">Combined xWins = sum of both teams' expected category wins that week (from all-play simulation). Higher = both teams were firing on all cylinders. These were the heavyweight bouts.</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="num">Week</th><th class="sortable" data-type="str">Winner</th><th class="sortable" data-type="str">Loser</th><th class="sortable" data-type="wlt">Score</th><th class="sortable" data-type="num">W xWins</th><th class="sortable" data-type="num">L xWins</th><th class="sortable" data-type="num">Combined</th></tr>
{best_matchup_rows}
</table>

<h3>Worst Matchups of the Year</h3>
<p class="section-desc">The toilet bowl games. Both teams at their worst the same week. Low combined xWins = neither team showed up.</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="num">Week</th><th class="sortable" data-type="str">Winner</th><th class="sortable" data-type="str">Loser</th><th class="sortable" data-type="wlt">Score</th><th class="sortable" data-type="num">W xWins</th><th class="sortable" data-type="num">L xWins</th><th class="sortable" data-type="num">Combined</th></tr>
{worst_matchup_rows}
</table>

<h3>What-If Standings (All-Play vs Actual)</h3>
<p class="section-desc">If every team played every team every week, here's how the standings would look vs how they actually finished. <span class="lucky">&#x2191;</span> = schedule helped you (actual rank better than deserved). <span class="unlucky">&#x2193;</span> = schedule screwed you (actual rank worse than deserved).</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">AP Rank</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="wlt">All-Play</th><th class="sortable" data-type="num">AP W%</th><th class="sortable" data-type="num">Actual Rank</th><th class="sortable" data-type="wlt">Matchup W-L</th><th>Rank Diff</th></tr>
{whatif_rows}
</table>

<h3>Schedule Strength</h3>
<p class="section-desc">How tough were your opponents? Avg Opp xW% = your opponents' average all-play win percentage across the season. Higher = you faced tougher teams. Avg Opp xCats = your opponents' average expected category wins per week.</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="num">Avg Opp xW%</th><th class="sortable" data-type="num">Avg Opp xCats</th><th class="sortable" data-type="wlt">Record</th><th class="sortable" data-type="num">xWins</th><th class="sortable" data-type="num">Diff</th></tr>
{sched_rows}
</table>

<h3>Weekly Dominators</h3>
<p class="section-desc">#1 AP = weeks where a team had the best all-play record (beat the most opponents). Top 3 = weeks finishing top 3. Last = weeks finishing dead last. Consistency = standard deviation of weekly all-play win% (lower = more consistent).</p>
<table class="sortable">
<tr><th class="sortable" data-type="num">#</th><th class="sortable" data-type="str">Team</th><th class="sortable" data-type="num">#1 AP Weeks</th><th class="sortable" data-type="num">Top 3 Weeks</th><th class="sortable" data-type="num">Last Place Weeks</th><th class="sortable" data-type="num">Avg W%</th><th class="sortable" data-type="num">Std Dev</th><th class="sortable" data-type="str">Consistency</th></tr>
{dom_rows}
</table>

</div>
<script>
new Chart(document.getElementById('luckChart').getContext('2d'), {{
  type: 'bar',
  data: {{
    labels: {luck_labels},
    datasets: [{{ label: 'Luck Coefficient', data: {luck_values}, backgroundColor: {luck_bar_colors}, borderRadius: 4 }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false, indexAxis: 'y',
    plugins: {{
      legend: {{ display: false }},
      tooltip: {{ backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8', borderColor: '#475569', borderWidth: 1,
        callbacks: {{ label: ctx => 'Luck Coeff: ' + (ctx.parsed.x > 0 ? '+' : '') + ctx.parsed.x.toFixed(3) }} }}
    }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Luck Coefficient', color: '#64748b' }} }},
      y: {{ ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }}, grid: {{ display: false }} }}
    }}
  }}
}});

new Chart(document.getElementById('xwinChart').getContext('2d'), {{
  type: 'line',
  data: {{
    labels: {week_labels},
    datasets: [{xwin_ds}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{
      legend: {{ position: 'bottom', labels: {{ color: '#e2e8f0', padding: 16, font: {{ size: 12 }}, usePointStyle: true, pointStyle: 'circle' }} }},
      tooltip: {{ backgroundColor: '#1e293b', titleColor: '#e2e8f0', bodyColor: '#94a3b8', borderColor: '#475569', borderWidth: 1, padding: 12,
        callbacks: {{ label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + '%' }} }}
    }},
    scales: {{
      x: {{ ticks: {{ color: '#94a3b8' }}, grid: {{ color: '#1e293b33' }} }},
      y: {{ min: 0, max: 100, ticks: {{ color: '#94a3b8', callback: v => v + '%' }}, grid: {{ color: '#334155' }}, title: {{ display: true, text: 'Cumulative All-Play Win %', color: '#64748b' }} }}
    }}
  }}
}});

// Sortable tables
document.querySelectorAll('th.sortable').forEach(th => {{
  th.addEventListener('click', () => {{
    const table = th.closest('table');
    const idx = Array.from(th.parentElement.children).indexOf(th);
    const rows = Array.from(table.querySelectorAll('tr')).slice(1);
    const type = th.dataset.type || 'str';
    const isAsc = th.classList.contains('asc');

    // Clear other sort indicators in this table
    th.parentElement.querySelectorAll('th').forEach(h => h.classList.remove('asc', 'desc'));
    th.classList.add(isAsc ? 'desc' : 'asc');

    const getValue = (row, i) => {{
      const cell = row.children[i];
      const text = cell.textContent.trim();
      if (type === 'num') {{
        return parseFloat(text.replace(/[+#]/g, '')) || 0;
      }} else if (type === 'wlt') {{
        // Parse W-L or W-L-T format, sort by first number (wins)
        const parts = text.replace(/[WL ]/g, '').split('-').map(Number);
        return parts[0] || 0;
      }}
      return text.toLowerCase();
    }};

    rows.sort((a, b) => {{
      const va = getValue(a, idx);
      const vb = getValue(b, idx);
      const cmp = typeof va === 'string' ? va.localeCompare(vb) : va - vb;
      return isAsc ? -cmp : cmp;
    }});

    const tbody = table.querySelector('tbody') || table;
    rows.forEach(r => tbody.appendChild(r));
  }});
}});
</script>
</body>
</html>"""

with open(r'c:\Users\taylor.ward\Documents\yahoo-fantasy-baseball-dynamo\docs\luck_analysis_2025.html', 'w', encoding='utf-8') as f:
    f.write(html)

print(f'\nGenerated docs/luck_analysis_2025.html')
