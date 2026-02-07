import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from datetime import datetime
import datetime, os, sys
from dotenv import load_dotenv
import warnings
from sklearn.preprocessing import MinMaxScaler
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules
from email_utils import send_failure_email
from datetime_utils import set_last_week
from manager_dict import manager_dict
from yahoo_utils import *
from storage_manager import DynamoStorageManager

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

storage = DynamoStorageManager(region='us-west-2')

#This uses the weekly_results process to get scores week-by-week
def season_standings():
    weekly_results_df = storage.get_historical_data('weekly_results')
    #weekly_results_df.columns = weekly_results_df.columns[:-1].tolist() + ['Score']
    raw_score_df = pd.DataFrame()
    num_cats = category_size()
    weekly_results_df.sort_values(by=['Team_Number', 'Week'], inplace=True)
    print(weekly_results_df)
    for index, row in weekly_results_df.iterrows():
         # Get the team number, week, score, opponent score, and number of categories
        team_num = row['Team_Number']
        week = row['Week']
        score = row['Score']
        opp_score = row['Opponent_Score']
        
        # Calculate the raw score based on the formula for wins and ties
        raw_score = score + ((num_cats -(score+opp_score)) * 0.5)
        
        # Find the previous raw score for the same team and week
        if week == 1:
            prev_raw_score = 0
        else:
            prev_raw_score = raw_score_df.loc[(raw_score_df['Team_Number'] == team_num) & (raw_score_df['Week'] == week - 1), 'Raw_Score'].sum()
    
        
        # Calculate the running summation of the raw score
        raw_score += prev_raw_score
        
        # Create a new row for the new DataFrame
        new_row = {'Team_Number': team_num,
                'Week': week,
                'Raw_Score': raw_score}
        
        # Add the new row to the new DataFrame
        raw_score_df = pd.concat([raw_score_df, pd.DataFrame([new_row])], ignore_index=True)


    # Sort the new DataFrame by team number and week
    raw_score_df = raw_score_df.sort_values(['Team_Number', 'Week'])

    # Calculate the rank based on raw score for each team and week
    raw_score_df['Rank'] = raw_score_df.groupby(['Week'])['Raw_Score'].rank(ascending=False)



    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    print(raw_score_df)
    return(raw_score_df)


def main():
    try:
        standing_season_df = season_standings()
        for week, week_df in standing_season_df.groupby('Week'):
            storage.append_weekly_data('standings_season_trend', int(week), week_df)
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)


if __name__ == '__main__':
    main()
