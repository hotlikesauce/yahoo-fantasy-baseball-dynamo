"""
Backfill missing 2025 week 21 data for teams 6-12.

Teams 1-5 already exist with correct tie-adjusted scores.
Image shows raw category W-L; DB stores tie-adjusted (0.5 per tied cat).

Matchups from Yahoo week 21 final results:
  Taylor (TN1)  2 vs 10  Mike (TN11)     -- already in DB
  James  (TN2)  6 vs  4  Kevin (TN10)    -- already in DB (7-5 tie-adj)
  Josh   (TN3)  4 vs  8  Austin (TN7)    -- already in DB
  Mark   (TN4)  6 vs  0  Eric (TN5)      -- already in DB (9-3 tie-adj)
  Eric   (TN5)  0 vs  6  Mark (TN4)      -- already in DB (3-9 tie-adj)
  Bryant (TN6)  1 vs 10  Greg (TN8)      -- MISSING
  Austin (TN7)  8 vs  4  Josh (TN3)      -- MISSING
  Greg   (TN8) 10 vs  1  Bryant (TN6)    -- MISSING
  Kurtis (TN9)  3 vs  9  Mikey (TN12)    -- MISSING
  Kevin  (TN10) 4 vs  6  James (TN2)     -- MISSING
  Mike   (TN11)10 vs  2  Taylor (TN1)    -- MISSING
  Mikey  (TN12) 9 vs  3  Kurtis (TN9)   -- MISSING
"""

import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
hist_table = dynamodb.Table('FantasyBaseball-HistoricalSeasons')
trends_table = dynamodb.Table('FantasyBaseball-SeasonTrends')

# Raw scores from image (category wins, NOT tie-adjusted)
# Format: (tn, team_name, raw_score, raw_opp_score, opponent_name)
MISSING = [
    ('6',  'Football Szn',           1, 10, 'OG 9'),
    ('7',  'Moniebol',               8,  4, 'Grand Salami Time'),
    ('8',  'OG 9',                  10,  1, 'Football Szn'),
    ('9',  'Getting Plowed Again.',   3,  9, 'SQUEEZE AGS'),
    ('10', 'The Rosterbation Station', 4, 6, 'Tegridy'),
    ('11', '\u00af\\_(\u30c4)_/\u00af', 10, 2, 'Serafini Hit Squad'),
    ('12', 'SQUEEZE AGS',            9,  3, 'Getting Plowed Again.'),
]


def tie_adjust(raw_w, raw_l):
    """Convert raw category W-L to tie-adjusted scores (0.5 per tie)."""
    ties = 12 - raw_w - raw_l
    return raw_w + ties * 0.5, raw_l + ties * 0.5


for tn, team, raw_s, raw_o, opp in MISSING:
    score, opp_score = tie_adjust(raw_s, raw_o)
    score_diff = score - opp_score
    nsd = Decimal(str(round(score / 12, 10)))

    # HistoricalSeasons item
    hist_item = {
        'YearTeamNumber': f'2025#{tn}',
        'DataTypeWeek': 'weekly_results#21',
        'DataType': 'weekly_results',
        'DataType#Week': f'weekly_results#21#{300 + int(tn)}',
        'YearDataType': '2025#weekly_results',
        'Year': Decimal('2025'),
        'Week': Decimal('21'),
        'TeamNumber': tn,
        'Team': team,
        'Opponent': opp,
        'Score': Decimal(str(score)),
        'Opponent_Score': Decimal(str(opp_score)),
        'Score_Difference': Decimal(str(score_diff)),
        'Normalized_Score_Difference': nsd,
    }

    # SeasonTrends item
    trends_item = {
        'TeamNumber': tn,
        'DataTypeWeek': 'weekly_results#21',
        'DataType': 'weekly_results',
        'DataType#Week': f'weekly_results#21#{300 + int(tn)}',
        'Week': Decimal('21'),
        'Team': team,
        'Opponent': opp,
        'Score': Decimal(str(score)),
        'Opponent_Score': Decimal(str(opp_score)),
        'Score_Difference': Decimal(str(score_diff)),
        'Normalized_Score_Difference': nsd,
    }

    hist_table.put_item(Item=hist_item)
    trends_table.put_item(Item=trends_item)
    print(f'  TN{tn}: {team} {score}-{opp_score} vs {opp}')

print(f'\nInserted {len(MISSING)} items into both tables.')
print('Done!')
