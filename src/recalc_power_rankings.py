import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
import os, sys
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules
from email_utils import send_failure_email
from manager_dict import manager_dict
from storage_manager import DynamoStorageManager
from datetime_utils import *
from yahoo_utils import *
from categories_dict import Low_Categories

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

storage = DynamoStorageManager(region='us-west-2')


def get_normalized_ranks(all_time_rank_df):
    #print(all_time_rank_df)
    #parse through columns and figure out which ones are low-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in all_time_rank_df.columns:
        if column == 'Team' or column == 'Opponent' or column == '_id':
            pass
        elif column in Low_Categories:
            low_columns_to_analyze.append(column)
        else:
            high_columns_to_analyze.append(column)
    else:
        pass
    # Calculate Score for each column grouped by team_number
    
    #print(low_columns_to_analyze)
    #print(high_columns_to_analyze)

    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        all_time_rank_df[column + '_Score'] = scaler.fit_transform(all_time_rank_df[column].values.reshape(-1, 1))    
    
    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((all_time_rank_df[column] - min_value) / (max_value - min_value)) * 100
        all_time_rank_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    all_time_rank_df['Score_Sum'] = all_time_rank_df[score_columns].sum(axis=1)
    all_time_rank_df['Score_Rank'] = all_time_rank_df['Score_Sum'].rank(ascending=False)
    all_time_rank_df = build_team_numbers(all_time_rank_df)  

    #print(all_time_rank_df)
    return all_time_rank_df

def main():
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    lastWeek = set_last_week()
    thisweek = set_this_week()
    try:
        running_df = pd.DataFrame()
    
        for week in range(1,thisweek):
            weeks_of_interest = [week]


            #Generate ranks and running ranks in lieu of running power ranks which started at the beginning of the season
            for weeks in weeks_of_interest:
                weekly_stats_df = storage.get_weekly_data('weekly_stats', weeks)

                team_rename_dict = {
                    "Bobby's Big Witt": "Mendoza Line",
                    "Mediocre White Excellence": "Jac Off",
                    "Moniebol (is this thing on?)🐳": "Moniebol 🐳",
                    "Moniebol (when u DEI u DIE)🐳": "Moniebol 🐳",
                    "Ready to Plow": "Getting Plowed.",
                    "Saggy Tatis": "Hoern Hub",
                    "Torpedo Dong": "PCA 3/4/5 & Ohtani"
                }

                # Apply the mapping to the 'Team' column
                weekly_stats_df['Team'] = weekly_stats_df['Team'].replace(team_rename_dict)
                running_df = pd.concat([running_df, weekly_stats_df], ignore_index=True)
            aggregations = {
                'R': 'sum', 'H': 'sum', 'HR': 'sum', 'RBI': 'sum', 'SB': 'sum',
                'TB': 'sum', 'QS': 'sum', 'SVH': 'sum',
                'OPS': 'mean', 'ERA': 'mean', 'WHIP': 'mean', 'K9': 'mean'
            }



            # Group by 'Team' and aggregate
            team_stats = running_df.groupby('Team').agg(aggregations).reset_index()

            normalized_ranks_df = get_normalized_ranks(team_stats)
            normalized_ranks_df['Week'] = week
            #print(normalized_ranks_df[[col for col in normalized_ranks_df.columns if col.endswith('_Score') or col == 'Team' or col == 'Score_Sum']])

            if week == 10:
                print(team_stats[['Team','K9']])
                # Print only Team, Score_Sum, Score_Rank, and each _Score column
                columns_to_print = ['Team', 'Score_Sum', 'Score_Rank'] + \
                                [col for col in normalized_ranks_df.columns if col.endswith('_Score')]
                print(normalized_ranks_df[columns_to_print])

            else:
                pass
            storage.append_weekly_data('running_normalized_ranks', week, normalized_ranks_df)

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
