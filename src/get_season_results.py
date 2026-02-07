import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import time, datetime, os, sys
from dotenv import load_dotenv
from loguru import logger
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules
from email_utils import send_failure_email
from datetime_utils import *
from manager_dict import manager_dict
from yahoo_utils import *
from storage_manager import DynamoStorageManager

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

storage = DynamoStorageManager(region='us-west-2')



def get_seasons_best(df,table):
    if df.empty:
        pass
    else:
        # Initialize a list to store the top 10 results for each stat
        top_1_dfs = []

        # Loop through each column in the DataFrame
        for column in df.columns:
            if column in ['Team', 'Week', 'Opponent']:
                # Skip non-stat columns
                pass
            elif column in Low_Categories:
                # Sort in ascending order for low categories and get top 10
                top_1 = df.sort_values(by=column, ascending=True).head(1)
                top_1['Stat_Category'] = column  # Add a column to indicate the stat category
                top_1_dfs.append(top_1)
            else:
                # Sort in descending order for high categories and get top 10
                top_1 = df.sort_values(by=column, ascending=False).head(1)
                top_1['Stat_Category'] = column  # Add a column to indicate the stat category
                top_1_dfs.append(top_1)

        # Combine all top 10 results into a single DataFrame
        top_10_df = pd.concat(top_1_dfs, ignore_index=True)
        print(top_10_df)



    stat_columns = ['R', 'H', 'HR', 'RBI', 'SB', 'OPS', 'TB', 'ERA', 'WHIP', 'K9', 'QS', 'SVH']

    # Create an empty DataFrame to store the results
    result_df = pd.DataFrame(columns=['Team', 'Week', 'Stat_Category', 'Total','Opponent'])

    # Process each stat column
    for stat in stat_columns:

        # Filter rows where 'Stat_Category' matches the current stat
        stat_df = top_10_df[top_10_df['Stat_Category'] == stat]
        
        # Find the row with the maximum value in the stat column
        top_stat_row = stat_df.loc[stat_df[stat].idxmax()]
        
        # Create a DataFrame with the top result
        top_stat_df = pd.DataFrame({
            'Team': [top_stat_row['Team']],
            'Week': [top_stat_row['Week']],
            'Stat_Category': [stat],
            'Total': [top_stat_row[stat]],
            'Opponent': [top_stat_row['Opponent']],
        })
    
        # Append the top result to the result DataFrame
        result_df = pd.concat([result_df, top_stat_df], ignore_index=True)


    result_df = build_team_numbers(result_df)

    print(result_df)
    storage.write_live_data(str(table), result_df)
    result_df = []

def main():
    thisWeek = set_this_week()
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    logger.add("logs/get_all_play.log", rotation="500 MB")
    try:
        df = storage.get_historical_data('week_stats')
        print(df)
        long_weeks_df = df[df['Week'].isin([1, 15])]
        regular_weeks_df = df[~df['Week'].isin([1, 15])]
        get_seasons_best(long_weeks_df, 'seasons_best_long')
        get_seasons_best(regular_weeks_df, 'seasons_best_regular')
        

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(error_message)
        send_failure_email(error_message, filename)


if __name__ == '__main__':
    main()
