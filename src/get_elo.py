import os
import sys
import logging
import traceback
from datetime import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler

# Local modules
from email_utils import send_failure_email
from manager_dict import manager_dict
from storage_manager import DynamoStorageManager
from datetime_utils import set_this_week
from yahoo_utils import *

# Load environment variables
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

storage = DynamoStorageManager(region='us-west-2')

logging.basicConfig(filename='error.log', level=logging.ERROR)

def get_initial_elo():
    size = league_size()
    return pd.DataFrame({
        'Team_Number': list(range(1, size + 1)),
        'ELO_Sum': [0.9] * size,
        'Week': [1] * size
    })

def expected_outcome(elo_df, schedule_df):
    def convert_nested(value):
        return int(value['$numberInt']) if isinstance(value, dict) and '$numberInt' in value else value

    schedule_df = schedule_df.applymap(convert_nested)

    # Drop and rename columns safely
    elo_df = elo_df.copy()
    for col in ['Expected_Result_Ra', 'Normalized_Score_Difference', 'ELO_Team_Sum']:
        elo_df.drop(columns=[col], errors='ignore', inplace=True)
    print(elo_df)
    elo_df.rename(columns={'New_ELO': 'ELO_Team_Sum', 'ELO_Sum': 'ELO_Team_Sum'}, inplace=True)

    elo_subset = elo_df[['Team_Number', 'ELO_Team_Sum', 'Week']]
    schedule_subset = schedule_df[['Opponent_Team_Number', 'Week']].rename(columns={'Week': 'Next_Week'})

    joined_df = pd.concat([elo_subset.reset_index(drop=True), schedule_subset.reset_index(drop=True)], axis=1).dropna()

    for col in ['Team_Number', 'Opponent_Team_Number']:
        joined_df[col] = joined_df[col].astype('int64')

    # Merge to get opponent ELO
    joined_df = joined_df.merge(
        elo_subset[['Team_Number', 'ELO_Team_Sum']],
        left_on='Opponent_Team_Number',
        right_on='Team_Number',
        how='left',
        suffixes=('', '_opponent')
    )

    joined_df.rename(columns={'ELO_Team_Sum_opponent': 'ELO_Opponent_Sum'}, inplace=True)
    joined_df.drop(columns=['Team_Number_opponent'], inplace=True)

    joined_df['Expected_Result_Ra'] = 1 / (1 + 25 ** ((joined_df['ELO_Opponent_Sum'] - joined_df['ELO_Team_Sum']) / 400))

    return joined_df

def get_new_elo(expected_outcome_df, week_df):
    week_df['Team_Number'] = week_df['Team_Number'].astype('int64')

    merged_df = expected_outcome_df.merge(
        week_df[['Normalized_Score_Difference', 'Team_Number']],
        on='Team_Number',
        how='left'
    )

    K_Factor = 50
    results = []

    for _, row in merged_df.iterrows():
        week = row['Week']
        team_number = row['Team_Number']
        elo = row['ELO_Team_Sum']
        expected = (row['Expected_Result_Ra'] - 0.5) * 2
        actual = (row['Normalized_Score_Difference'] - 0.5) * 2

        new_elo = elo + K_Factor * (actual - expected)

        results.append({
            'Week': week + 1,
            'Team_Number': team_number,
            'ELO_Team_Sum': elo,
            'Expected_Result_Ra': expected,
            'Normalized_Score_Difference': actual,
            'New_ELO': new_elo
        })

    return pd.DataFrame(results)

def main():
    try:
        current_week = set_this_week()
        num_teams = league_size()

        week_1_df = pd.DataFrame({
            'Team_Number': list(range(1, num_teams + 1)),
            'ELO_Team_Sum': [1000] * num_teams,
            'Week': [1] * num_teams
        })

        running_elo_df = pd.DataFrame()
        output_df = week_1_df.copy()

        for week in range(1, current_week):
            schedule_df = storage.get_schedule_data(week=week)
            expected_df = expected_outcome(output_df, schedule_df)

            week_results_df = storage.get_weekly_data('weekly_results', week)
            output_df = get_new_elo(expected_df, week_results_df)
            running_elo_df = pd.concat([running_elo_df, output_df], ignore_index=True)

        week_1_df.rename(columns={'ELO_Team_Sum': 'New_ELO'}, inplace=True)
        running_elo_df = pd.concat([running_elo_df, week_1_df], ignore_index=True)

        running_elo_df['Team_Number'] = running_elo_df['Team_Number'].astype(int).astype(str).str.replace('\.0', '', regex=True)

        for week, week_df in running_elo_df.groupby('Week'):
            storage.append_weekly_data('Running_ELO', int(week), week_df)

    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        logging.error(f'{filename}: {error_message} - Line {line_number}')
        raise

if __name__ == '__main__':
    main()
