"""
Lambda: Compute 2026 luck & matchup analysis and store in DynamoDB.
Reads weekly_stats + weekly_results + power_ranks_live, computes luck coefficients,
all-play records, blowouts, schedule strength, etc. Stores full result as a single DynamoDB item.
Triggered by: CloudWatch Events - run every Monday after pull_weekly_stats (e.g. cron(30 9 ? * MON *))
"""

import json
import logging
import statistics
import boto3
from boto3.dynamodb.conditions import Attr
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
table = dynamodb.Table('FantasyBaseball-SeasonTrends')

HIGH_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'K9', 'QS', 'SVH']
LOW_CATS = ['ERA', 'WHIP', 'TB']
ALL_CATS = HIGH_CATS + LOW_CATS

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]


def scan_by_prefix(prefix, year=2026):
    """Scan all items where DataType#Week starts with prefix and Year matches."""
    items = []
    kwargs = {'FilterExpression': Attr('DataType#Week').begins_with(prefix) & Attr('Year').eq(year)}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp['Items'])
        if 'LastEvaluatedKey' not in resp:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
    return items


def rank_xwins(stats_dict, cats):
    """Rank-based xWins: 0 to 1/N per cat, max total = len(cats)/N (N = num teams).
    Normalized so total across all 12 cats = 0 to 1.0 per week."""
    tns = list(stats_dict.keys())
    n = len(tns)
    result = {tn: 0.0 for tn in tns}
    if n < 2:
        return result
    for cat in cats:
        vals = [(tn, stats_dict[tn][cat]) for tn in tns if cat in stats_dict[tn]]
        if not vals:
            continue
        reverse = cat not in LOW_CATS
        vals.sort(key=lambda x: x[1], reverse=reverse)
        i = 0
        while i < len(vals):
            j = i
            while j < len(vals) - 1 and vals[j][1] == vals[j + 1][1]:
                j += 1
            avg_score = sum((n - 1 - p) / (n - 1) for p in range(i, j + 1)) / (j - i + 1)
            for k in range(i, j + 1):
                result[vals[k][0]] += avg_score
            i = j + 1
    return result


def h2h_result(a, b):
    wa = wb = 0
    for c in HIGH_CATS:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    for c in LOW_CATS:
        if c in a and c in b:
            if a[c] < b[c]: wa += 1
            elif b[c] < a[c]: wb += 1
    return wa, wb


def lambda_handler(event, context):
    try:
        logger.info("compute_luck_analysis: START")

        # 1. Build team mapping - team_names#current is the canonical source (written by pull_live_standings)
        name_to_tn = {}
        tn_latest_name = {}
        all_tns_set = set(str(i) for i in range(1, 13))

        meta = table.get_item(Key={'TeamNumber': '0', 'DataType#Week': 'team_names#current'})
        if meta.get('Item') and 'Teams' in meta['Item']:
            for tn, name in meta['Item']['Teams'].items():
                name_to_tn[name] = tn
                tn_latest_name[tn] = name

        # Supplement with power_ranks_live (handles name changes mid-season)
        for item in scan_by_prefix('power_ranks_live#'):
            tn = item['TeamNumber']
            name_to_tn[item['Team']] = tn
            tn_latest_name[tn] = item['Team']

        # Pull weekly_results once for both name mapping and actual results
        wr_items = scan_by_prefix('weekly_results#')
        wr_by_week = defaultdict(list)
        for item in wr_items:
            wr_by_week[int(item['Week'])].append(item)

        # Auto-map names from weekly_results
        for week_items in wr_by_week.values():
            mapped = {}
            unmapped_names = []
            for item in week_items:
                name = item.get('Team', '')
                if not name:
                    continue
                tn = name_to_tn.get(name)
                if tn:
                    mapped[name] = tn
                else:
                    unmapped_names.append(name)
            if unmapped_names:
                used_tns = set(mapped.values())
                missing_tns = sorted(all_tns_set - used_tns, key=int)
                if len(unmapped_names) == 1 and len(missing_tns) == 1:
                    name_to_tn[unmapped_names[0]] = missing_tns[0]
                    tn_latest_name[missing_tns[0]] = unmapped_names[0]

        # 2. Pull weekly_stats
        weekly_stats = defaultdict(dict)
        ws_by_week = defaultdict(list)
        for item in scan_by_prefix('weekly_stats#'):
            ws_by_week[int(item['Week'])].append(item)
        for week, items in ws_by_week.items():
            if len(items) < 12:
                continue
            for item in items:
                tn = name_to_tn.get(item.get('Team'))
                if not tn:
                    continue
                weekly_stats[week][tn] = {c: float(item[c]) for c in ALL_CATS if c in item}

        weeks = sorted(w for w in weekly_stats.keys() if w <= 20)

        if not weeks:
            logger.warning("No weekly stats data found")
            return {'statusCode': 200, 'body': 'No data to compute'}

        # 3. Build actual_results from weekly_results
        actual_results = defaultdict(dict)
        for week, week_items in wr_by_week.items():
            seen = set()
            for item in week_items:
                team = item['Team']
                tn = name_to_tn.get(team)
                w = int(item['Week'])
                if tn is None or (tn, w) in seen:
                    continue
                seen.add((tn, w))
                score = float(item['Score'])
                opp_score = float(item['Opponent_Score'])
                diff = score - opp_score
                actual_results[w][tn] = {
                    'won': diff > 0, 'tied': diff == 0,
                    'cats_won': score, 'cats_lost': opp_score,
                    'opp_tn': name_to_tn.get(item['Opponent']),
                    'opp_name': item['Opponent'],
                    'team_name': team, 'diff': diff,
                }

        # 4. Compute xWins per team per week (rank-based, 0-1.0 per week)
        weekly_xwins = {}
        for week in weeks:
            weekly_xwins[week] = rank_xwins(weekly_stats[week], ALL_CATS)

        # 5. H2H all-play simulation (11 opponents, for all-play win% chart)
        weekly_allplay = defaultdict(lambda: defaultdict(lambda: {'w': 0, 'l': 0, 't': 0}))

        for week in weeks:
            tns = list(weekly_stats[week].keys())
            for i, tn_a in enumerate(tns):
                for j, tn_b in enumerate(tns):
                    if i == j:
                        continue
                    wa, wb = h2h_result(weekly_stats[week][tn_a], weekly_stats[week][tn_b])
                    if wa > wb:
                        weekly_allplay[week][tn_a]['w'] += 1
                    elif wb > wa:
                        weekly_allplay[week][tn_a]['l'] += 1
                    else:
                        weekly_allplay[week][tn_a]['t'] += 1

        # 6. Weekly luck data: luck = actual_cats_won - xwins (0-12 scale)
        weekly_luck_data = []
        team_luck_totals = defaultdict(lambda: {'actual_cats': 0, 'xwins_cats': 0.0, 'matchup_w': 0, 'matchup_l': 0, 'matchup_t': 0})

        for week in weeks:
            # Compute ap_pct per team for ranking
            ap_pcts = {}
            for tn2, ap2 in weekly_allplay[week].items():
                total2 = ap2['w'] + ap2['l'] + ap2['t']
                ap_pcts[tn2] = (ap2['w'] + ap2['t'] * 0.5) / total2 if total2 > 0 else 0

            for tn in sorted(weekly_allplay[week].keys(), key=lambda x: int(x)):
                ap = weekly_allplay[week][tn]
                ap_total = ap['w'] + ap['l'] + ap['t']
                ap_pct = ap_pcts.get(tn, 0)
                ap_rank = 1 + sum(1 for other_pct in ap_pcts.values() if other_pct > ap_pct)

                xw = weekly_xwins[week].get(tn, 0)
                xw_cats = round(xw, 2)  # expected category wins (0-12 scale)

                actual = actual_results.get(week, {}).get(tn)
                if actual:
                    cats_w = int(actual['cats_won'])
                    cats_l = int(actual['cats_lost'])
                    won = actual['won']
                    luck_score = round(cats_w - xw_cats, 2)
                    # lucky win = won matchup but below-median all-play; unlucky loss = opposite
                    luck_tag = ''
                    n_teams = len(weekly_allplay[week])
                    median_rank = n_teams / 2
                    if won and ap_rank > median_rank:
                        luck_tag = 'lucky_win'
                    elif not won and not actual['tied'] and ap_rank <= median_rank:
                        luck_tag = 'unlucky_loss'

                    team_luck_totals[tn]['actual_cats'] += cats_w
                    team_luck_totals[tn]['xwins_cats'] += xw_cats
                    if won:
                        team_luck_totals[tn]['matchup_w'] += 1
                    elif actual['tied']:
                        team_luck_totals[tn]['matchup_t'] += 1
                    else:
                        team_luck_totals[tn]['matchup_l'] += 1

                    weekly_luck_data.append({
                        'week': week, 'tn': tn,
                        'name': tn_latest_name.get(tn, tn),
                        'ap_w': ap['w'], 'ap_l': ap['l'], 'ap_t': ap['t'],
                        'ap_pct': round(ap_pct, 3), 'ap_rank': ap_rank,
                        'cats_w': cats_w, 'cats_l': cats_l,
                        'xw_cats': xw_cats,
                        'luck_score': luck_score,
                        'luck_tag': luck_tag,
                        'won': won, 'opp_name': actual['opp_name'],
                    })

        # 7. Team summary: cumulative luck = actual_cats - xwins_cats
        team_summary = []
        for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
            lt = team_luck_totals[tn]
            cum_luck = round(lt['actual_cats'] - lt['xwins_cats'], 2)

            total_ap_w = sum(weekly_allplay[w][tn]['w'] for w in weeks)
            total_ap_l = sum(weekly_allplay[w][tn]['l'] for w in weeks)
            total_ap_t = sum(weekly_allplay[w][tn]['t'] for w in weeks)
            ap_total = total_ap_w + total_ap_l + total_ap_t
            ap_pct = (total_ap_w + total_ap_t * 0.5) / ap_total if ap_total > 0 else 0

            cum_xwins = round(sum(weekly_xwins[w].get(tn, 0) for w in weeks), 3)

            team_summary.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'actual_cats': lt['actual_cats'],
                'xwins_cats': round(lt['xwins_cats'], 1),
                'cum_luck': cum_luck,
                'cum_xwins': cum_xwins,
                'matchup_w': lt['matchup_w'], 'matchup_l': lt['matchup_l'], 'matchup_t': lt['matchup_t'],
                'allplay_w': total_ap_w, 'allplay_l': total_ap_l, 'allplay_t': total_ap_t,
                'ap_pct': round(ap_pct, 3),
            })

        team_summary.sort(key=lambda x: x['cum_luck'], reverse=True)

        # Color map sorted by cumulative xwins
        by_xwin = sorted(team_summary, key=lambda x: x['cum_xwins'], reverse=True)
        color_map = {s['tn']: COLORS[i % len(COLORS)] for i, s in enumerate(by_xwin)}
        for s in team_summary:
            s['color'] = color_map.get(s['tn'], '#94a3b8')

        # 7. Blowouts and closest matchups
        all_matchups = []
        for week in weeks:
            for tn, result in actual_results[week].items():
                if result['diff'] > 0:
                    all_matchups.append({
                        'week': week, 'winner': result['team_name'],
                        'loser': result['opp_name'],
                        'w_cats': int(result['cats_won']), 'l_cats': int(result['cats_lost']),
                        'margin': result['diff'],
                    })

        blowouts = sorted(all_matchups, key=lambda x: x['margin'], reverse=True)[:10]
        closest = sorted(all_matchups, key=lambda x: x['margin'])[:10]

        # 8. Best/Worst Matchups (by combined weekly xWins)
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
                xwins_a = weekly_xwins[week].get(tn, 0)
                xwins_b = weekly_xwins[week].get(opp_tn, 0)
                combined = round(xwins_a + xwins_b, 3)

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
                    'week': week, 'winner': w_name, 'loser': l_name,
                    'w_xw': round(w_xw, 3), 'l_xw': round(l_xw, 3),
                    'combined': combined, 'score': score,
                    'tied': result['diff'] == 0,
                })

        best_matchups = sorted(matchup_quality, key=lambda x: x['combined'], reverse=True)[:10]
        worst_matchups = sorted(matchup_quality, key=lambda x: x['combined'])[:10]

        # 9. Schedule Strength (avg opponent xWins)
        schedule_strength = []
        for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
            opp_xw_sum = opp_count = 0
            for w in weeks:
                if w in actual_results and tn in actual_results[w]:
                    opp_tn = actual_results[w][tn]['opp_tn']
                    if opp_tn:
                        opp_xw_sum += weekly_xwins[w].get(opp_tn, 0)
                        opp_count += 1
            avg_opp_xw = opp_xw_sum / opp_count if opp_count > 0 else 0
            ts = next((s for s in team_summary if s['tn'] == tn), None)
            schedule_strength.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'avg_opp_xw': round(avg_opp_xw, 3),
                'actual_cats': ts['actual_cats'] if ts else 0,
                'xwins_cats': ts['xwins_cats'] if ts else 0,
                'cum_luck': ts['cum_luck'] if ts else 0,
                'color': color_map.get(tn, '#94a3b8'),
            })
        schedule_strength.sort(key=lambda x: x['avg_opp_xw'], reverse=True)

        # 10. What-If All-Play Standings vs Actual
        actual_standings = []
        for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
            mw = ml = mt = 0
            for w in weeks:
                if w in actual_results and tn in actual_results[w]:
                    r = actual_results[w][tn]
                    if r['won']: mw += 1
                    elif r['tied']: mt += 1
                    else: ml += 1
            actual_standings.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
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
                'tn': tn, 'name': ts['name'],
                'ap_w': ts['allplay_w'], 'ap_l': ts['allplay_l'], 'ap_t': ts['allplay_t'],
                'ap_pct': round(pct, 3),
                'matchup_w': actual['matchup_w'] if actual else 0,
                'matchup_l': actual['matchup_l'] if actual else 0,
                'matchup_t': actual['matchup_t'] if actual else 0,
                'color': color_map.get(tn, '#94a3b8'),
            })
        allplay_standings.sort(key=lambda x: x['ap_pct'], reverse=True)
        for i, s in enumerate(allplay_standings):
            s['ap_rank'] = i + 1
            s['actual_rank'] = actual_rank_map.get(s['tn'], 0)
            s['rank_diff'] = s['actual_rank'] - s['ap_rank']

        # 11. Weekly Dominators & Consistency
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

        dominator_data = []
        for tn in sorted(tn_latest_name.keys(), key=lambda x: int(x)):
            d = weekly_mvps[tn]
            pcts = weekly_pcts.get(tn, [])
            avg_pct = sum(pcts) / len(pcts) if pcts else 0
            stdev = statistics.stdev(pcts) if len(pcts) > 1 else 0
            dominator_data.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'first': d['first'], 'top3': d['top3'], 'last': d['last'],
                'avg_pct': round(avg_pct, 3), 'stdev': round(stdev, 3),
                'color': color_map.get(tn, '#94a3b8'),
            })
        dominator_data.sort(key=lambda x: x['first'], reverse=True)

        # 12. All-play win% trend (for chart)
        allplay_trend = {}
        for s in by_xwin:
            tn = s['tn']
            cum_w = cum_total = 0
            points = {}
            for w in weeks:
                ap = weekly_allplay[w].get(tn, {'w': 0, 'l': 0, 't': 0})
                cum_w += ap['w'] + ap['t'] * 0.5
                cum_total += ap['w'] + ap['l'] + ap['t']
                pct = (cum_w / cum_total * 100) if cum_total > 0 else 50
                points[str(w)] = round(pct, 1)
            allplay_trend[tn] = points

        result = {
            'weeks': weeks,
            'teamSummary': team_summary,
            'weeklyBreakdown': weekly_luck_data,
            'blowouts': blowouts,
            'closest': closest,
            'bestMatchups': best_matchups,
            'worstMatchups': worst_matchups,
            'scheduleStrength': schedule_strength,
            'allplayStandings': allplay_standings,
            'dominators': dominator_data,
            'allplayTrend': allplay_trend,
            'colorMap': color_map,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        # Store in DynamoDB
        table.put_item(Item={
            'TeamNumber': '0',
            'DataType#Week': 'computed#luck_analysis',
            'DataTypeWeek': 'computed#luck_analysis',
            'YearDataType': '2026#computed',
            'Year': 2026,
            'Data': json.dumps(result, default=str),
            'Timestamp': datetime.utcnow().isoformat(),
        })

        logger.info(f"compute_luck_analysis: SUCCESS - {len(team_summary)} teams, {len(weeks)} weeks")
        return {'statusCode': 200, 'body': f'Computed luck analysis: {len(team_summary)} teams, {len(weeks)} weeks'}

    except Exception as e:
        logger.error(f"compute_luck_analysis FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
