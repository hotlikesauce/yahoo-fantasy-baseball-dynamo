"""
Lambda: Compute 2026 season trends analysis and store in DynamoDB.
Reads weekly_stats + power_ranks_live, computes xWins, batter/pitcher splits,
hot/cold, season's best. Stores full result as a single DynamoDB item.
Triggered by: CloudWatch Events - run every Monday after pull_weekly_stats (e.g. cron(30 9 ? * MON *))
"""

import json
import logging
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
BATTER_CATS = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS']
PITCHER_HIGH = ['K9', 'QS', 'SVH']
PITCHER_LOW = ['ERA', 'WHIP', 'TB']
LONG_WEEKS = {17}

CAT_LABELS = {
    'R': 'Runs', 'H': 'Hits', 'HR': 'Home Runs', 'RBI': 'RBI', 'SB': 'Stolen Bases',
    'OPS': 'OPS', 'TB': 'Total Bases Allowed (Lowest)', 'K9': 'K/9', 'QS': 'Quality Starts',
    'SVH': 'Saves+Holds', 'ERA': 'ERA (Lowest)', 'WHIP': 'WHIP (Lowest)',
}

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
    """
    For each category, rank all teams linearly and assign 0 to 1/N xWins per cat.
    Rank 1 (best) = 1/N, rank N (worst) = 0, ties get the average of their positions.
    Returns {tn: xwins} where max total xwins = len(cats) * (1/N) = 1.0 when N=len(cats).
    Actually max = 1.0 total since we assign 1/N per cat and there are N teams...
    wait: max per cat = 1/N, N cats max total = N*(1/N) = 1.0. But N here is num_teams not num_cats.
    We use num_cats=12 as the divisor so max per cat = 1/12, total max = 12*(1/12) = 1.0.
    """
    tns = list(stats_dict.keys())
    n = len(tns)
    result = {tn: 0.0 for tn in tns}
    if n < 2:
        return result
    for cat in cats:
        vals = [(tn, stats_dict[tn][cat]) for tn in tns if cat in stats_dict[tn]]
        if not vals:
            continue
        # Sort: high cats descending, low cats ascending
        reverse = cat not in LOW_CATS
        vals.sort(key=lambda x: x[1], reverse=reverse)
        # Assign linear scores 1/12 down to 0, handle ties by averaging
        i = 0
        while i < len(vals):
            j = i
            while j < len(vals) - 1 and vals[j][1] == vals[j + 1][1]:
                j += 1
            # positions i..j are tied; average their linear scores
            # position 0 (best) → 1/12, position n-1 (worst) → 0
            avg_score = sum((n - 1 - p) / (n - 1) for p in range(i, j + 1)) / (j - i + 1)
            for k in range(i, j + 1):
                result[vals[k][0]] += avg_score
            i = j + 1
    return result


def lambda_handler(event, context):
    try:
        logger.info("compute_season_trends: START")

        # 1. Build team mapping - team_names#current is the canonical source (written by pull_live_standings)
        name_to_tn = {}
        tn_latest_name = {}

        meta = table.get_item(Key={'TeamNumber': '0', 'DataType#Week': 'team_names#current'})
        if meta.get('Item') and 'Teams' in meta['Item']:
            for tn, name in meta['Item']['Teams'].items():
                name_to_tn[name] = tn
                tn_latest_name[tn] = name

        # Supplement with power_ranks_live for name mapping
        pr_by_week = defaultdict(list)
        for item in scan_by_prefix('power_ranks_live#'):
            tn = item['TeamNumber']
            name_to_tn[item['Team']] = tn
            tn_latest_name[tn] = item['Team']
            pr_by_week[int(item.get('Week', 0))].append(item)

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

        stat_weeks = sorted(w for w in weekly_stats.keys() if w <= 20)

        # WLT: compute category wins from weekly_stats using actual matchup pairings in weekly_results
        latest_wlt = defaultdict(lambda: {'wins': 0, 'losses': 0, 'ties': 0})
        seen_matchups = set()
        wr_items = scan_by_prefix('weekly_results#')
        # Build matchup pairings: {(week, tn) -> opp_tn}
        matchup_pairs = {}
        for item in wr_items:
            w = int(item.get('Week', 0))
            if w not in stat_weeks:
                continue
            tn = name_to_tn.get(item.get('Team'))
            opp_tn = name_to_tn.get(item.get('Opponent'))
            if tn and opp_tn:
                matchup_pairs[(w, tn)] = opp_tn
        # Compute category W-L-T for each matchup pair
        for (w, tn), opp_tn in matchup_pairs.items():
            pair = tuple(sorted([tn, opp_tn]))
            if (w, pair) in seen_matchups:
                continue
            seen_matchups.add((w, pair))
            if w not in weekly_stats or tn not in weekly_stats[w] or opp_tn not in weekly_stats[w]:
                continue
            a = weekly_stats[w][tn]
            b = weekly_stats[w][opp_tn]
            for cat in ALL_CATS:
                if cat not in a or cat not in b:
                    continue
                if cat in LOW_CATS:
                    if a[cat] < b[cat]:
                        latest_wlt[tn]['wins'] += 1; latest_wlt[opp_tn]['losses'] += 1
                    elif a[cat] > b[cat]:
                        latest_wlt[tn]['losses'] += 1; latest_wlt[opp_tn]['wins'] += 1
                    else:
                        latest_wlt[tn]['ties'] += 1; latest_wlt[opp_tn]['ties'] += 1
                else:
                    if a[cat] > b[cat]:
                        latest_wlt[tn]['wins'] += 1; latest_wlt[opp_tn]['losses'] += 1
                    elif a[cat] < b[cat]:
                        latest_wlt[tn]['losses'] += 1; latest_wlt[opp_tn]['wins'] += 1
                    else:
                        latest_wlt[tn]['ties'] += 1; latest_wlt[opp_tn]['ties'] += 1

        if not stat_weeks:
            logger.warning("No weekly stats data found")
            return {'statusCode': 200, 'body': 'No data to compute'}

        # 3. xWins via rank-based linear assignment (0 to 1.0 per week across 12 cats)
        weekly_xwins = defaultdict(dict)
        season_bat = defaultdict(lambda: {'xw': 0.0})
        season_pit = defaultdict(lambda: {'xw': 0.0})

        for week in stat_weeks:
            xw = rank_xwins(weekly_stats[week], ALL_CATS)
            for tn, val in xw.items():
                weekly_xwins[week][tn] = round(val, 4)

            bat_xw = rank_xwins(weekly_stats[week], BATTER_CATS)
            pit_xw = rank_xwins(weekly_stats[week], PITCHER_HIGH + PITCHER_LOW)
            for tn in weekly_stats[week]:
                season_bat[tn]['xw'] += bat_xw.get(tn, 0)
                season_pit[tn]['xw'] += pit_xw.get(tn, 0)

        # 4. Compute power scores (0-1200 per week: 12 cats × 0-100 each via linear scaling)
        weekly_power = defaultdict(dict)
        for week in stat_weeks:
            teams_in_week = list(weekly_stats[week].keys())
            for tn in teams_in_week:
                weekly_power[week][tn] = 0.0
            for cat in ALL_CATS:
                vals = {tn: weekly_stats[week][tn][cat] for tn in teams_in_week if cat in weekly_stats[week][tn]}
                if not vals:
                    continue
                min_val = min(vals.values())
                max_val = max(vals.values())
                for tn, val in vals.items():
                    if max_val == min_val:
                        score = 50.0
                    elif cat in LOW_CATS:
                        score = (max_val - val) / (max_val - min_val) * 100
                    else:
                        score = (val - min_val) / (max_val - min_val) * 100
                    weekly_power[week][tn] += score
            for tn in weekly_power[week]:
                weekly_power[week][tn] = round(weekly_power[week][tn], 1)

        # 5. Sort teams by cumulative power score (latest week's score as tiebreaker)
        cum_power = {tn: sum(weekly_power[w].get(tn, 0) for w in stat_weeks) for tn in tn_latest_name}
        sorted_tns = sorted(tn_latest_name.keys(), key=lambda tn: cum_power.get(tn, 0), reverse=True)

        color_map = {tn: COLORS[i % len(COLORS)] for i, tn in enumerate(sorted_tns)}

        # 6. Build result JSON
        teams = [{'tn': tn, 'name': tn_latest_name.get(tn, tn), 'color': COLORS[i % len(COLORS)]}
                 for i, tn in enumerate(sorted_tns)]

        # Power scores by week and cumulative
        power_score_out = {tn: {str(w): weekly_power[w].get(tn, 0) for w in stat_weeks} for tn in sorted_tns}
        cum_power_out = {}
        for tn in sorted_tns:
            cum = 0
            cum_power_out[tn] = {}
            for w in stat_weeks:
                cum += weekly_power[w].get(tn, 0)
                cum_power_out[tn][str(w)] = round(cum, 1)

        # xWins by week
        xwins_out = {tn: {str(w): round(weekly_xwins[w].get(tn, 0), 2) for w in stat_weeks} for tn in sorted_tns}

        # Cumulative xWins
        cum_out = {}
        for tn in sorted_tns:
            cum = 0
            cum_out[tn] = {}
            for w in stat_weeks:
                cum += weekly_xwins[w].get(tn, 0)
                cum_out[tn][str(w)] = round(cum, 1)

        # Scatter (batter vs pitcher xWins%)
        # Max batter xWins per week = len(BATTER_CATS) (each cat max = 1.0), same for pitcher
        max_bat = float(len(BATTER_CATS)) * len(stat_weeks)
        max_pit = float(len(PITCHER_HIGH) + len(PITCHER_LOW)) * len(stat_weeks)
        scatter = []
        for tn in sorted_tns:
            bat_xw = season_bat[tn]['xw']
            pit_xw = season_pit[tn]['xw']
            scatter.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'bat': round(bat_xw / max_bat * 100, 1) if max_bat > 0 else 50,
                'pit': round(pit_xw / max_pit * 100, 1) if max_pit > 0 else 50,
                'color': color_map.get(tn, '#94a3b8'),
            })

        # Hot/Cold
        last_2 = stat_weeks[-2:] if len(stat_weeks) >= 2 else stat_weeks
        hot_cold = []
        for tn in sorted_tns:
            recent = [weekly_xwins[w].get(tn, 0) for w in last_2]
            recent_avg = sum(recent) / len(recent) if recent else 0
            season_avg = sum(weekly_xwins[w].get(tn, 0) for w in stat_weeks) / len(stat_weeks)
            diff = round(recent_avg - season_avg, 2)
            hot_cold.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'recent': round(recent_avg, 2), 'season': round(season_avg, 2),
                'diff': diff,
                'trend': 'HOT' if diff > 0.5 else 'COLD' if diff < -0.5 else 'STEADY',
                'color': color_map.get(tn, '#94a3b8'),
            })
        hot_cold.sort(key=lambda x: x['recent'], reverse=True)

        # Season's Best
        short_weeks = {w for w in stat_weeks if w not in LONG_WEEKS}
        season_best = {'short': {}, 'long': {}}
        for cat in ALL_CATS:
            is_low = cat in LOW_CATS
            for week_set, key in [(short_weeks, 'short'), (LONG_WEEKS, 'long')]:
                best = None
                for week in week_set:
                    if week not in weekly_stats:
                        continue
                    for tn, stats in weekly_stats[week].items():
                        if cat not in stats:
                            continue
                        val = stats[cat]
                        if best is None or (is_low and val < best['val']) or (not is_low and val > best['val']):
                            best = {'val': val, 'tn': tn, 'week': week}
                if best:
                    if cat in ['ERA', 'WHIP', 'OPS']:
                        fmt = f"{best['val']:.3f}"
                    elif cat == 'K9':
                        fmt = f"{best['val']:.1f}"
                    else:
                        fmt = f"{int(best['val'])}" if best['val'] == int(best['val']) else f"{best['val']:.1f}"
                    season_best[key][cat] = {
                        'val': best['val'], 'formatted': fmt,
                        'team': tn_latest_name.get(best['tn'], best['tn']),
                        'tn': best['tn'], 'week': best['week'],
                        'color': color_map.get(best['tn'], '#94a3b8'),
                        'label': CAT_LABELS.get(cat, cat),
                        'isBatter': cat in BATTER_CATS,
                    }

        # Standings from latest power_ranks_live W-L-T
        standings_out = []
        for tn in sorted_tns:
            wlt = latest_wlt.get(tn, {})
            standings_out.append({
                'tn': tn,
                'name': tn_latest_name.get(tn, tn),
                'wins': wlt.get('wins', 0),
                'losses': wlt.get('losses', 0),
                'ties': wlt.get('ties', 0),
            })
        standings_out.sort(key=lambda x: (x['wins'] + x['ties'] * 0.5) / max(1, x['wins'] + x['losses'] + x['ties']), reverse=True)

        result = {
            'teams': teams,
            'statWeeks': stat_weeks,
            'standings': standings_out,
            'weeklyPowerScores': power_score_out,
            'cumulativePowerScores': cum_power_out,
            'weeklyXwins': xwins_out,
            'cumulativeXwins': cum_out,
            'scatter': scatter,
            'hotCold': hot_cold,
            'last2Label': f"Weeks {last_2[0]}-{last_2[-1]}" if len(last_2) > 1 else f"Week {last_2[0]}",
            'seasonBest': season_best,
            'catOrder': ALL_CATS,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
        }

        # Store in DynamoDB
        table.put_item(Item={
            'TeamNumber': '0',
            'DataType#Week': 'computed#season_trends',
            'DataTypeWeek': 'computed#season_trends',
            'YearDataType': '2026#computed',
            'Year': 2026,
            'Data': json.dumps(result, default=str),
            'Timestamp': datetime.utcnow().isoformat(),
        })

        logger.info(f"compute_season_trends: SUCCESS - {len(teams)} teams, {len(stat_weeks)} weeks")
        return {'statusCode': 200, 'body': f'Computed season trends: {len(teams)} teams, {len(stat_weeks)} weeks'}

    except Exception as e:
        logger.error(f"compute_season_trends FAILED: {e}", exc_info=True)
        return {'statusCode': 500, 'body': str(e)}
