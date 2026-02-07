import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import time, datetime, os
from dotenv import load_dotenv
import warnings
# Ignore the FutureWarning
warnings.simplefilter(action='ignore', category=FutureWarning)

# Local Modules
from email_utils import send_failure_email
from storage_manager import DynamoStorageManager
from manager_dict import *
from datetime_utils import *
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def calculate_playoff_status(df_standings):
    """
    Calculate playoff clinching and elimination status based on current standings.
    
    Parameters:
    - df_standings: DataFrame with Raw_Score_Static (current points)
    
    Returns:
    - DataFrame with added playoff_status and seed_status columns
    """
    current_week = 18
    final_week = 21
    weeks_remaining = final_week - current_week
    max_points_remaining = weeks_remaining * 12  # 12 points per week
    
    # Sort by Raw_Score_Static to get current standings
    df_sorted = df_standings.sort_values('Raw_Score_Static', ascending=False).reset_index(drop=True)
    
    # Initialize status columns
    df_sorted['playoff_status'] = 1  # 1 = In Contention, 0 = Out
    df_sorted['playoff_status'] = df_sorted['playoff_status'].astype(int)  # Force int type
    df_sorted['seed_status'] = 'TBD'
    
    for i, row in df_sorted.iterrows():
        current_points = row['Raw_Score_Static']
        max_possible_points = current_points + max_points_remaining
        
        # Check elimination (can't reach 6th place)
        if i >= 6:  # 7th place or lower
            sixth_place_points = df_sorted.iloc[5]['Raw_Score_Static']
            if max_possible_points < sixth_place_points:
                df_sorted.at[i, 'playoff_status'] = 0  # Eliminated
        
        # Check playoff clinching (6th place can't catch up)
        if i < 6:  # Top 6
            seventh_place_points = df_sorted.iloc[6]['Raw_Score_Static'] if len(df_sorted) > 6 else 0
            seventh_max_possible = seventh_place_points + max_points_remaining
            if current_points > seventh_max_possible:
                df_sorted.at[i, 'playoff_status'] = 1  # Still 1 since they're in playoffs
        
        # Check seed clinching
        if i == 0:  # 1st place
            second_place_points = df_sorted.iloc[1]['Raw_Score_Static']
            second_max_possible = second_place_points + max_points_remaining
            if current_points > second_max_possible:
                df_sorted.at[i, 'seed_status'] = 'Clinched #1 Seed'
        
        elif i == 1:  # 2nd place
            third_place_points = df_sorted.iloc[2]['Raw_Score_Static']
            third_max_possible = third_place_points + max_points_remaining
            if current_points > third_max_possible:
                # Check if they can still catch 1st
                first_place_points = df_sorted.iloc[0]['Raw_Score_Static']
                if max_possible_points >= first_place_points:
                    df_sorted.at[i, 'seed_status'] = 'Clinched Top 2 Seed'
                else:
                    df_sorted.at[i, 'seed_status'] = 'Clinched #2 Seed'
    
    return df_sorted

def get_playoff_status():
    """
    Get current standings and calculate playoff status for all teams.
    """
    # Get current standings from Yahoo
    soup = url_requests(YAHOO_LEAGUE_ID)
    table = soup.find_all('table')
    df_seasonRecords = pd.read_html(str(table))[0]
    
    # Clean column names
    df_seasonRecords.columns = df_seasonRecords.columns.str.replace('-', '')
    
    # Parse W-L-T record
    new = df_seasonRecords['WLT'].str.split("-", n=2, expand=True)
    new = new.astype(int)
    df_seasonRecords["WLT_Win"] = new[0]
    df_seasonRecords["WLT_Loss"] = new[1]
    df_seasonRecords["WLT_Draw"] = new[2]
    df_seasonRecords['Raw_Score_Static'] = df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw'] * 0.5)
    
    # Calculate playoff status
    df_playoff_status = calculate_playoff_status(df_seasonRecords)
    
    # Add team numbers using the build_team_numbers function
    df_playoff_status = build_team_numbers(df_playoff_status)
    
    # Add manager names
    df_playoff_status['Manager_Name'] = df_playoff_status['Team_Number'].map(manager_dict)
    
    # Select only the columns we need
    final_df = df_playoff_status[['Team', 'Team_Number', 'Manager_Name', 'playoff_status', 'seed_status']].copy()
    
    print("Playoff Status Results:")
    print(final_df)
    
    return final_df

def main():
    try:
        df_playoff_status = get_playoff_status()
        storage = DynamoStorageManager(region='us-west-2')
        storage.write_live_data('playoff_status', df_playoff_status)
        print("Playoff status data successfully written to DynamoDB")
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        print(error_message)
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()