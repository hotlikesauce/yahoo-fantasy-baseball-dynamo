"""
Lambda: Compute 2026 season trends analysis and store in DynamoDB.
Reads weekly_stats + power_ranks_live, computes xWins, batter/pitcher splits,
hot/cold, season's best. Stores full result as a single DynamoDB item.
Triggered by: CloudWatch Events - run every Monday after pull_weekly_stats (e.g. cron(30 9 ? * MON *))
"""

import json
import logging
import boto3
from boto3.dynamodb.conditions import Key
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
LONG_WEEKS = {1, 15}

CAT_LABELS = {
    'R': 'Runs', 'H': 'Hits', 'HR': 'Home Runs', 'RBI': 'RBI', 'SB': 'Stolen Bases',
    'OPS': 'OPS', 'TB': 'Total Bases Allowed (Lowest)', 'K9': 'K/9', 'QS': 'Quality Starts',
    'SVH': 'Saves+Holds', 'ERA': 'ERA (Lowest)', 'WHIP': 'WHIP (Lowest)',
}

COLORS = [
    '#ef4444', '#f97316', '#eab308', '#22c55e', '#14b8a6', '#3b82f6',
    '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b', '#10b981', '#a855f7',
]


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


def h2h_batter(a, b):
    wa = wb = 0
    for c in BATTER_CATS:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    return wa, wb


def h2h_pitcher(a, b):
    wa = wb = 0
    for c in PITCHER_HIGH:
        if c in a and c in b:
            if a[c] > b[c]: wa += 1
            elif b[c] > a[c]: wb += 1
    for c in PITCHER_LOW:
        if c in a and c in b:
            if a[c] < b[c]: wa += 1
            elif b[c] < a[c]: wb += 1
    return wa, wb


def lambda_handler(event, context):
    try:
        logger.info("compute_season_trends: START")

        # 1. Build team mapping from power_ranks_live
        name_to_tn = {}
        tn_latest_name = {}
        power_data = {}

        for week in range(1, 30):
            resp = table.query(IndexName='DataTypeWeekIndex',
                KeyConditionExpression=Key('DataTypeWeek').eq(f'power_ranks_live#{week}'))
            if resp['Count'] == 0:
                break
            for item in resp['Items']:
                tn = item['TeamNumber']
                name_to_tn[item['Team']] = tn
                tn_latest_name[tn] = item['Team']
                if tn not in power_data:
                    power_data[tn] = {}
                w = int(item['Week'])
                power_data[tn][w] = {
                    'score': float(item['Score']),
                    'rank': float(item['Rank']),
                }

        # Also build mapping from weekly_stats
        for week in range(1, 30):
            resp = table.query(IndexName='DataTypeWeekIndex',
                KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
            if resp['Count'] == 0:
                break
            for item in resp['Items']:
                tn = item['TeamNumber']
                if item.get('Team'):
                    name_to_tn[item['Team']] = tn
                    if tn not in tn_latest_name:
                        tn_latest_name[tn] = item['Team']

        # 2. Pull weekly_stats
        weekly_stats = defaultdict(dict)
        for week in range(1, 30):
            resp = table.query(IndexName='DataTypeWeekIndex',
                KeyConditionExpression=Key('DataTypeWeek').eq(f'weekly_stats#{week}'))
            if resp['Count'] == 0:
                break
            if resp['Count'] < 12:
                continue
            for item in resp['Items']:
                tn = name_to_tn.get(item.get('Team')) or item.get('TeamNumber')
                if not tn:
                    continue
                weekly_stats[week][tn] = {c: float(item[c]) for c in ALL_CATS if c in item}

        stat_weeks = sorted(w for w in weekly_stats.keys() if w <= 20)
        power_weeks = sorted(set(w for d in power_data.values() for w in d))

        if not stat_weeks:
            logger.warning("No weekly stats data found")
            return {'statusCode': 200, 'body': 'No data to compute'}

        # 3. H2H Simulation with batter/pitcher splits
        weekly_xwins = defaultdict(dict)
        season_bat = defaultdict(lambda: {'w': 0, 'total': 0})
        season_pit = defaultdict(lambda: {'w': 0, 'total': 0})

        for week in stat_weeks:
            tns = list(weekly_stats[week].keys())
            for i, ta in enumerate(tns):
                cat_w = 0
                for j, tb in enumerate(tns):
                    if i == j:
                        continue
                    wa, wb = h2h_result(weekly_stats[week][ta], weekly_stats[week][tb])
                    cat_w += wa
                    ba, _ = h2h_batter(weekly_stats[week][ta], weekly_stats[week][tb])
                    season_bat[ta]['w'] += ba
                    season_bat[ta]['total'] += len(BATTER_CATS)
                    pa, _ = h2h_pitcher(weekly_stats[week][ta], weekly_stats[week][tb])
                    season_pit[ta]['w'] += pa
                    season_pit[ta]['total'] += len(PITCHER_HIGH) + len(PITCHER_LOW)
                n_opps = len(tns) - 1
                weekly_xwins[week][ta] = cat_w / n_opps if n_opps > 0 else 0

        # 4. Sort teams
        if power_weeks:
            sorted_tns = sorted(power_data.keys(),
                key=lambda tn: power_data[tn].get(max(power_weeks), {}).get('score', 0), reverse=True)
        else:
            cum_xw = {tn: sum(weekly_xwins[w].get(tn, 0) for w in stat_weeks) for tn in tn_latest_name}
            sorted_tns = sorted(cum_xw.keys(), key=lambda tn: cum_xw[tn], reverse=True)

        color_map = {tn: COLORS[i % len(COLORS)] for i, tn in enumerate(sorted_tns)}

        # 5. Build result JSON
        teams = [{'tn': tn, 'name': tn_latest_name.get(tn, tn), 'color': COLORS[i % len(COLORS)]}
                 for i, tn in enumerate(sorted_tns)]

        # Power data
        power_out = {}
        if power_weeks:
            for tn in sorted_tns:
                power_out[tn] = {str(w): power_data[tn][w] for w in power_weeks if w in power_data.get(tn, {})}

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

        # Scatter (batter vs pitcher win%)
        scatter = []
        for tn in sorted_tns:
            bat = season_bat[tn]
            pit = season_pit[tn]
            scatter.append({
                'tn': tn, 'name': tn_latest_name.get(tn, tn),
                'bat': round(bat['w'] / bat['total'] * 100, 1) if bat['total'] > 0 else 50,
                'pit': round(pit['w'] / pit['total'] * 100, 1) if pit['total'] > 0 else 50,
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

        result = {
            'teams': teams,
            'statWeeks': stat_weeks,
            'powerWeeks': power_weeks,
            'powerData': power_out,
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
