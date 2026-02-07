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
lastWeek = set_last_week()

storage = DynamoStorageManager(region='us-west-2')

def get_all_play(num_teams,leaguedf,most_recent_week):
    thisWeek = set_this_week()
    lastWeek = set_last_week()
    for week in range ((most_recent_week),thisWeek):
        #Function below sets up the dataframe for the all play function
        if most_recent_week == thisWeek:
            pass
        elif most_recent_week == 0:
            pass
        else:
            allPlaydf = leaguedf
            for matchup in range(1, (num_teams+1)):
                # Setting this sleep timer on a few weeks helps with the rapid requests to the Yahoo servers
                # If you request the site too much in a short amount of time, you will be blocked temporarily          

                soup = url_requests(YAHOO_LEAGUE_ID + 'matchup?week=' + str(week) + '&module=matchup&mid1=' + str(matchup))

                table = soup.find_all('table')
                df = pd.read_html(str(table))[1]
                df['Week'] = week
                df.columns = df.columns.str.replace('[#,@,&,/,+]', '', regex=True)
                #df.columns = df.columns.str.replace('HR.1', 'HRA')

                for column in df.columns:
                    if column in percentage_categories:

                        # Logic below to handle asterisks that happen for % based stats when ties occur
                        df[column] = df[column].astype(str)  # Convert column to string type
                
                        # Remove asterisks from column values
                        df[column] = df[column].map(lambda x: x.rstrip('*'))
                        
                        # Replace '-' with '0.00'
                        df[column] = df[column].replace(['-'], '0.00')
                        
                        df[column] = df[column].astype(float)  # Convert column to float type


                column_list = leaguedf.columns.tolist()
                df = df[column_list]
                df['Opponent'] = df.loc[1, 'Team']


                allPlaydf = pd.concat([allPlaydf, df.loc[[0]]], ignore_index=True)


            #print(week)
            #print(allPlaydf)
            df_week_stats = build_team_numbers(allPlaydf)
            storage.append_weekly_data('week_stats', week, df_week_stats)
            logger.info(f'Week: {week}')

            # Calculate implied win statistics - The person with the most Runs in a week has an implied win of 1.0, because they would defeat every other team in that category.
            # Lowest scoring player has implied wins of 0, which we manually set to avoid dividing by 0
            print(allPlaydf)
            for column in allPlaydf:
                if column in ['Team','Week','Opponent']:
                    pass
                elif column in Low_Categories:
                    # For low categories (ERA, WHIP), lower values are better
                    # Rank ascending: best team (lowest value) gets rank 1, worst gets rank 12
                    # Then invert: best team gets coefficient 1.0, worst gets coefficient 0.0
                    allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=True)
                    allPlaydf.set_index(column+'_Rank')
                    allPlaydf[column+'_Coeff'] = (num_teams - allPlaydf[column+'_Rank']) / (num_teams - 1)
                else:
                    # For high categories (HR, RBI), higher values are better
                    # Rank descending: best team (highest value) gets rank 1, worst gets rank 12  
                    # Then invert: best team gets coefficient 1.0, worst gets coefficient 0.0
                    allPlaydf[column+'_Rank'] = allPlaydf[column].rank(ascending=False)
                    allPlaydf.set_index(column+'_Rank')
                    allPlaydf[column+'_Coeff'] = (num_teams - allPlaydf[column+'_Rank']) / (num_teams - 1)

                coeff_cols = [col for col in allPlaydf.columns if 'Coeff' in col]
                coeff_cols.extend(['Team', 'Week', 'Opponent'])
                rankings_df = allPlaydf[coeff_cols]
            
            cols_to_sum = [col for col in rankings_df.columns if col not in ['Team', 'Week','Opponent']]
            for col in cols_to_sum:
                rankings_df[col] = pd.to_numeric(rankings_df[col], errors='coerce')

            # Now sum the numeric columns along axis=1
            rankings_df['Expected_Wins'] = rankings_df[cols_to_sum].sum(axis=1)

            
            # Remove Individual Stat Columns
            rankings_df = rankings_df[['Team', 'Week', 'Opponent', 'Expected_Wins']]
            print(rankings_df)

            rankings_df['Team'] = rankings_df['Team'].astype(str)
            rankings_df['Opponent'] = rankings_df['Opponent'].astype(str)

            rankings_df_expanded = rankings_df.merge(right=rankings_df, left_on='Team', right_on='Opponent')

            rankings_df_expanded = rankings_df_expanded.rename(columns={"Team_x": "Team", "Week_x": "Week","Opponent_x": "Opponent","Expected_Wins_x": "Team_Expected_Wins","Expected_Wins_y": "Opponent_Expected_Wins"})
            rankings_df_expanded = rankings_df_expanded[['Week', 'Team', 'Team_Expected_Wins', 'Opponent', 'Opponent_Expected_Wins']]
            rankings_df_expanded['Matchup_Difference'] = (rankings_df_expanded['Team_Expected_Wins'] - rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
            rankings_df_expanded['Matchup_Power'] = (rankings_df_expanded['Team_Expected_Wins'] + rankings_df_expanded['Opponent_Expected_Wins']).apply(lambda x: round(x, 2))
            
            print(rankings_df_expanded)
            
            df = build_team_numbers(rankings_df_expanded)
            print(df)
            df = build_opponent_numbers(rankings_df_expanded)

            
            storage.append_weekly_data('coefficient', week, df)

            # Reset dfs for new weeks so data isn't aggregated
            del allPlaydf, rankings_df, df


def main():
    thisWeek = set_this_week()
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    logger.add("logs/get_all_play.log", rotation="500 MB")
    try:
        df = storage.get_historical_data('coefficient')
        if not df.empty:
            max_week = df['Week'].max()
            get_all_play(num_teams, leaguedf, max_week)
        else:
            get_all_play(num_teams, leaguedf, 1)

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(error_message)
        send_failure_email(error_message, filename)


if __name__ == '__main__':
    main()
