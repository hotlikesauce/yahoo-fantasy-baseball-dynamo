import pandas as pd
import bs4 as bs
import urllib.request
from urllib.request import urlopen as uReq
import time, datetime, os, sys
import numpy as np
from dotenv import load_dotenv
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

def get_schedule(max_week):
    num_teams = league_size()
    this_week = set_this_week()
    
    # Get team_dict for joining
    team_dict_df = storage.get_live_data('team_dict')
    if team_dict_df.empty:
        print("Error: team_dict collection is empty. Cannot proceed without team mappings.")
        return
    
    # Create team name to number mapping
    team_name_to_number = dict(zip(team_dict_df['Team'], team_dict_df['Team_Number']))
    print(f"Team mappings loaded: {team_name_to_number}")

    # Clear existing schedule data before inserting new data
    storage.clear_schedule()

    for week in range(this_week, 22):
        rows = []
        processed_matchups = set()  # Track processed matchups to avoid duplicates
        
        # Get all team matchups for this week
        for team_id in range(1, num_teams + 1):
            try:
                soup = url_requests(YAHOO_LEAGUE_ID + f'matchup?date=totals&week={week}&mid1={team_id}')
                table = soup.find_all('table')
                df = pd.read_html(str(table))[1]
                df['Week'] = week
                df.columns = df.columns.str.replace('[#,@,&,/,+]', '', regex=True)

                if len(df) >= 2:
                    team1_name = df.loc[0, 'Team']
                    team2_name = df.loc[1, 'Team']
                    
                    # Create a sorted tuple to represent this matchup (order doesn't matter)
                    matchup_key = tuple(sorted([team1_name, team2_name]))
                    
                    # Only process if we haven't seen this matchup yet
                    if matchup_key not in processed_matchups:
                        # Team 1 vs Team 2
                        team1_row = df.loc[0].copy()
                        team1_row['Opponent'] = team2_name
                        
                        # Team 2 vs Team 1  
                        team2_row = df.loc[1].copy()
                        team2_row['Opponent'] = team1_name
                        
                        # Add both teams to rows
                        rows.append(team1_row[['Team', 'Opponent', 'Week']])
                        rows.append(team2_row[['Team', 'Opponent', 'Week']])
                        
                        # Mark this matchup as processed
                        processed_matchups.add(matchup_key)
                        
            except Exception as e:
                # Skip invalid team IDs
                continue

        # Build the full schedule DataFrame
        schedule_df = pd.DataFrame(rows)
        
        if schedule_df.empty:
            print(f"No schedule data found for week {week}")
            continue
            
        print(f"Schedule data for week {week}:")
        print(schedule_df)

        # Check for missing opponent info
        if schedule_df['Opponent'].isnull().any():
            print(f"Missing opponent data for week {week}")
            continue

        # Map team names to team numbers using the team_dict
        schedule_df['Team_Number'] = schedule_df['Team'].map(team_name_to_number)
        schedule_df['Opponent_Team_Number'] = schedule_df['Opponent'].map(team_name_to_number)
        
        # Check if all teams were mapped successfully
        if schedule_df['Team_Number'].isnull().any() or schedule_df['Opponent_Team_Number'].isnull().any():
            print(f"Warning: Some teams could not be mapped for week {week}")
            print("Unmapped teams:", schedule_df[schedule_df['Team_Number'].isnull() | schedule_df['Opponent_Team_Number'].isnull()])
            continue

        # Keep only the required columns: team_number, opponent_number, week
        final_schedule_df = schedule_df[['Team_Number', 'Opponent_Team_Number', 'Week']].copy()
        
        print(f"Final schedule data for week {week}:")
        print(final_schedule_df)

        # Write to DynamoDB
        storage.write_schedule_data(week, final_schedule_df)

def main():
    try:
        df = storage.get_schedule_data()
        if not df.empty:
            max_week = df['Week'].max() + 1
            get_schedule(max_week)
        else:
            get_schedule(1)
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(error_message)
        send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
