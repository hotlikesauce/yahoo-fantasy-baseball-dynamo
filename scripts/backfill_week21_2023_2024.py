"""
Backfill/fix 2023 and 2024 week 21 data in HistoricalSeasons.

2023: 5 existing items (TN1-5) are correct. Add TN6-TN12.
2024: TN1,TN4,TN5 correct. TN2/TN3 have swapped opponents (fix). Add TN6-TN12.

Scores are raw W-L (no tie adjustment) matching the existing format for these years.
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
hist_table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')


def put_item(year, tn, team, score, opp_score, opponent):
    total = score + opp_score
    nsd = Decimal(str(round(score / total, 10))) if total > 0 else Decimal('0')
    item = {
        'YearTeamNumber': f'{year}#{tn}',
        'DataTypeWeek': 'weekly_results#21',
        'DataType': 'weekly_results',
        'DataType#Week': f'weekly_results#21#{400 + int(tn)}',
        'YearDataType': f'{year}#weekly_results',
        'Year': Decimal(str(year)),
        'Week': Decimal('21'),
        'TeamNumber': str(tn),
        'Team': team,
        'Opponent': opponent,
        'Score': Decimal(str(score)),
        'Opponent_Score': Decimal(str(opp_score)),
        'Score_Difference': Decimal(str(score - opp_score)),
        'Normalized_Score_Difference': nsd,
    }
    hist_table.put_item(Item=item)
    marker = 'FIX' if tn in FIX_TNS else 'ADD'
    print(f'  [{marker}] TN{tn}: {team} {score}-{opp_score} vs {opponent}')


# ============================================================
# 2023 week 21 - ADD TN6-TN12
# ============================================================
# Matchups from user:
#   Taylor(1) 4-5 Jamie(9)     -- exists
#   Austin(2) 5-6 Kevin(10)    -- exists
#   Kurtis(3) 5-7 Bryant(4)    -- exists (both sides)
#   Greg(5) 4-7 Eric(7)        -- exists
#   Josh(6) 5-7 Mikey(11)      -- MISSING
#   David(8) 5-6 Mike(12)      -- MISSING

print('=== 2023 week 21 ===')
FIX_TNS = set()

# Use team names from existing DB items / power_ranks for cross-referencing
put_item(2023, 6,  'The Dollar General',  5, 7, 'scoopski potatoes')
put_item(2023, 7,  'Ian Cumsler',         7, 4, 'Limit yourself to the present')
put_item(2023, 8,  'How to lose a game in 10 days', 5, 6, '\u00af\\_(\u30c4)_/\u00af')
put_item(2023, 9,  'Camp Lejeune',        5, 4, 'McLainBang')
put_item(2023, 10, 'The Rosterbation Station', 6, 5, 'Moniebol \U0001f433 (DEFCON 2)')
put_item(2023, 11, 'scoopski potatoes',   7, 5, 'The Dollar General')
put_item(2023, 12, '\u00af\\_(\u30c4)_/\u00af', 6, 5, 'How to lose a game in 10 days')

# ============================================================
# 2024 week 21 - FIX TN2/TN3, ADD TN6-TN12
# ============================================================
# Matchups from user:
#   Taylor(1) 6-4 Mark(4)       -- exists, correct
#   Jamie(2) 11-1 Austin(7)     -- exists but WRONG (has 9-3 vs Mikey)
#   Bryant(3) 9-3 Mikey(8)      -- exists but WRONG (has 11-1 vs Austin)
#   Eric(5) 8-3 Kevin(10)       -- exists, correct
#   Greg(6) 2-10 Mike(12)       -- MISSING
#   Josh(9) 7-4 Kurtis(11)      -- MISSING

print('\n=== 2024 week 21 ===')
FIX_TNS = {2, 3}

# Fix TN2: James 11-1 vs Austin (was 9-3 vs Mikey)
put_item(2024, 2, 'It actually goes both ways.', 11, 1, 'Moniebol (100gb goal)\U0001f433')
# Fix TN3: Bryant 9-3 vs Mikey (was 11-1 vs Austin)
put_item(2024, 3, 'BELIEVE', 9, 3, 'BTHO mcneese')

# Add missing teams
put_item(2024, 6,  'OGglass-z13',    2, 10, '\u00af\\_(\u30c4)_/\u00af\U0001f3c6')
put_item(2024, 7,  'Moniebol (100gb goal)\U0001f433', 1, 11, 'It actually goes both ways.')
put_item(2024, 8,  'BTHO mcneese',   3,  9, 'BELIEVE')
put_item(2024, 9,  'Grand Salami Time', 7, 4, 'Ready to Plow')
put_item(2024, 10, 'The Rosterbation Station', 3, 8, 'Ian Cumsler')
put_item(2024, 11, 'Ready to Plow',  4,  7, 'Grand Salami Time')
put_item(2024, 12, '\u00af\\_(\u30c4)_/\u00af\U0001f3c6', 10, 2, 'OGglass-z13')

print('\nDone!')
