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


# Local Modules - email utils for failure emails, storage manager for DB writes
from email_utils import send_failure_email
from storage_manager import DynamoStorageManager
from manager_dict import *
from datetime_utils import *
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

def getCurrentMatchups():
    num_teams = league_size()
    df_teamRecords = pd.DataFrame(columns = ['Team','Team_Wins','Team_Loss','Team_Draw','Record'])
    df_weeklyMatchups = pd.DataFrame(columns = ['Team','Record'])

    # Change this to the number of teams in your league (I have 12)
    for matchup in range(1,(num_teams+1)):
        #To prevent DDOS, Yahoo limits your URL requests over a set amount of time. Sleep timer to hlep space our requests
        time.sleep(4)
        df_currentMatchup = pd.DataFrame(columns = ['Team','Score'])
        
        thisWeek = set_this_week()

        soup = url_requests(YAHOO_LEAGUE_ID+'matchup?date=totals&week='+str(thisWeek)+'&mid1='+str(matchup))

        table = soup.find_all('table')
        df_currentMatchup = pd.read_html(str(table))[1]

        # Assuming df_currentMatchup is your DataFrame
        df_currentMatchup.columns = df_currentMatchup.columns[:-1].tolist() + ['Score']

        print(df_currentMatchup)

        
        df_currentMatchup=df_currentMatchup[['Team','Score']]

        df_teamRecords['Team'] = df_currentMatchup['Team']
        df_teamRecords['Team_Wins'] = df_currentMatchup['Score'].iloc[0]
        df_teamRecords['Team_Loss'] = df_currentMatchup['Score'].iloc[1]
        if df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0] == num_teams:
            df_teamRecords['Team_Draw'] = 0
            df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
        else:
            df_teamRecords['Team_Draw'] = num_teams - (df_teamRecords['Team_Wins'].iloc[0] + df_teamRecords['Team_Loss'].iloc[0])
            df_teamRecords['Record'] = list(zip(df_teamRecords.Team_Wins,df_teamRecords.Team_Draw,df_teamRecords.Team_Loss))
        
        # print(df_teamRecords[['Team','Record']].loc[0])

        df_weeklyMatchups = pd.concat([df_weeklyMatchups, df_teamRecords.loc[[0]]], ignore_index=True)

    #print(df_weeklyMatchups[['Team','Record']])
    
    return df_weeklyMatchups

def getLiveStandings(df_currentMatchup):
    soup = url_requests(YAHOO_LEAGUE_ID)

    table = soup.find_all('table')
    df_seasonRecords = pd.read_html(str(table))[0]
    print(df_seasonRecords.columns)
    
    df_seasonRecords.columns = df_seasonRecords.columns.str.replace('-', '')
    print(df_seasonRecords.columns)
    
    new = df_seasonRecords['WLT'].str.split("-", n=2, expand=True)
    new = new.astype(int)
    df_seasonRecords["WLT_Win"] = new[0]
    df_seasonRecords["WLT_Loss"] = new[1]
    df_seasonRecords["WLT_Draw"] = new[2]
    df_seasonRecords['Raw_Score_Static'] = df_seasonRecords['WLT_Win'] + (df_seasonRecords['WLT_Draw'] * 0.5)

    df_liveStandings = df_seasonRecords.merge(df_currentMatchup, on='Team')
    df_liveStandings['Live_Wins'] = df_liveStandings['WLT_Win'] + df_liveStandings['Team_Wins']
    df_liveStandings['Live_Loss'] = df_liveStandings['WLT_Loss'] + df_liveStandings['Team_Loss']
    df_liveStandings['Live_Draw'] = df_liveStandings['WLT_Draw'] + df_liveStandings['Team_Draw']
    df_liveStandings['Raw_Score'] = df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw'] * 0.5)
    df_liveStandings['Games_Back'] = df_liveStandings['Raw_Score'].max() - df_liveStandings['Raw_Score']
    df_liveStandings['Pct'] = (df_liveStandings['Live_Wins'] + (df_liveStandings['Live_Draw'] * 0.5)) / (df_liveStandings['Live_Wins'] + df_liveStandings['Live_Draw'] + df_liveStandings['Live_Loss'])
    df_liveStandings['Current Matchup'] = df_liveStandings['Team_Wins'].astype(int).astype(str) + '-' + df_liveStandings['Team_Loss'].astype(int).astype(str) + '-' + df_liveStandings['Team_Draw'].astype(int).astype(str)

    # Sort by Raw_Score in descending order
    df_liveStandings = df_liveStandings.sort_values(by=['Raw_Score'], ascending=False, ignore_index=True)

    # Add rank_live field, explicitly handling ties
    df_liveStandings['rank_live'] = df_liveStandings['Raw_Score'].rank(method='first', ascending=False).astype(int)
    
    # Now handle ties
    df_liveStandings['rank_live'] = df_liveStandings.groupby('Raw_Score')['rank_live'].transform('min')

    print(df_liveStandings[['Team', 'Pct', 'Raw_Score', 'rank_live']])

    try:
        df_liveStandings['Rank'] = df_liveStandings['Rank'].str.replace('*', '').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")

    final_return_df = build_team_numbers(df_liveStandings)
    final_return_df['Manager_Name'] = df_liveStandings['Team_Number'].map(manager_dict)

    print(final_return_df)
    return final_return_df

    



   

def main():
    try:
        df_currentMatchup = getCurrentMatchups()
        df_liveStandings = getLiveStandings(df_currentMatchup)

        # Write to DynamoDB
        storage = DynamoStorageManager(region='us-west-2')
        storage.write_live_data('live_standings', df_liveStandings)
        storage.write_live_data('team_dict', df_liveStandings[['Team', 'Team_Number']])  # Team dictionary

        print("✅ Successfully wrote live standings to DynamoDB")
    except Exception as e:
        filename = os.path.basename(__file__)
        error_message = str(e)
        print(error_message)
        send_failure_email(error_message,filename)


if __name__ == '__main__':
    main()
