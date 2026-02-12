import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

hist_table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')

print('=' * 80)
print('HISTORICAL SEASONS - weekly_results week 21')
print('=' * 80)

for year in [2023, 2024, 2025]:
    print()
    print('--- ' + str(year) + ' ---')
    resp = hist_table.query(
        IndexName='YearDataTypeIndex',
        KeyConditionExpression=Key('YearDataType').eq(str(year) + '#weekly_results') & Key('Week').eq(21)
    )
    items = resp.get('Items', [])
    print('  Teams with week 21 data: ' + str(len(items)))
    for item in sorted(items, key=lambda x: str(x.get('YearTeamNumber', ''))):
        ytn = item.get('YearTeamNumber', '?')
        team = item.get('Team', item.get('team', '?'))
        opp = item.get('Opponent', item.get('opponent', '?'))
        score = item.get('Score', item.get('score', '?'))
        opp_score = item.get('Opponent_Score', item.get('opponent_score', '?'))
        print('    YTN=' + str(ytn) + '  Team=' + str(team) + '  vs ' + str(opp) + '  Score=' + str(score) + '-' + str(opp_score))

trends_table = dynamodb.Table('FantasyBaseball-SeasonTrends')

print()
print('=' * 80)
print('SEASON TRENDS (2025) - weekly_results#21')
print('=' * 80)

resp = trends_table.query(
    IndexName='DataTypeWeekIndex',
    KeyConditionExpression=Key('DataTypeWeek').eq('weekly_results#21')
)
items = resp.get('Items', [])
print('  Teams with week 21 data: ' + str(len(items)))
for item in sorted(items, key=lambda x: str(x.get('TeamNumber', ''))):
    tn = item.get('TeamNumber', '?')
    team = item.get('Team', item.get('team', '?'))
    opp = item.get('Opponent', item.get('opponent', '?'))
    score = item.get('Score', item.get('score', '?'))
    opp_score = item.get('Opponent_Score', item.get('opponent_score', '?'))
    print('    TN=' + str(tn) + '  Team=' + str(team) + '  vs ' + str(opp) + '  Score=' + str(score) + '-' + str(opp_score))

print()
print('=' * 80)
print('SUMMARY')
print('=' * 80)
for year in [2023, 2024, 2025]:
    resp = hist_table.query(
        IndexName='YearDataTypeIndex',
        KeyConditionExpression=Key('YearDataType').eq(str(year) + '#weekly_results') & Key('Week').eq(21)
    )
    count = len(resp.get('Items', []))
    if count == 12:
        status = 'COMPLETE (12 teams)'
    elif count > 0:
        status = 'INCOMPLETE (' + str(count) + '/12 teams)'
    else:
        status = 'MISSING'
    print('  ' + str(year) + ' HistoricalSeasons week 21: ' + status)

resp = trends_table.query(
    IndexName='DataTypeWeekIndex',
    KeyConditionExpression=Key('DataTypeWeek').eq('weekly_results#21')
)
count = len(resp.get('Items', []))
if count == 12:
    status = 'COMPLETE (12 teams)'
elif count > 0:
    status = 'INCOMPLETE (' + str(count) + '/12 teams)'
else:
    status = 'MISSING'
print('  2025 SeasonTrends week 21:      ' + status)
