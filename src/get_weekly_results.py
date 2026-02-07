import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import time, datetime, os, sys
from loguru import logger
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

def get_weekly_results(num_teams, max_week):
    # Set week number
    weekly_results_df = pd.DataFrame()
    lastWeek = set_last_week()
    thisWeek = set_this_week()
    for week in range((max_week + 1), thisWeek):
        print(f"Processing week {week}...")
        # Sleep timer to avoid rapid requests to the Yahoo servers
        for matchup in range(1, (num_teams + 1)):
            print(f"  Processing matchup {matchup}/{num_teams} for week {week}")
            soup = url_requests(YAHOO_LEAGUE_ID + 'matchup?week=' + str(week) + '&module=matchup&mid1=' + str(matchup))
            
            # Add delay after each Yahoo API request to prevent throttling
            time.sleep(2)  # 2 second delay between requests
            table = soup.find_all('table')
            df = pd.read_html(str(table))[1]
            df.columns = df.columns[:-1].tolist() + ['Score']
            df.columns = df.columns.str.replace('[#,@,&,/,+]', '', regex=True)
            df['Week'] = week
            df = df[['Team', 'Week', 'Score']]
            df['Opponent'] = df.loc[1, 'Team']
            df['Opponent_Score'] = df.loc[1, 'Score']

            # Calculate ties and adjust scores
            # Total categories = 12, so ties = 12 - team_wins - opponent_wins
            team_wins = df.loc[0, 'Score']
            opponent_wins = df.loc[1, 'Score']
            ties = 12 - team_wins - opponent_wins
            
            # Adjust scores to include half points for ties
            df.loc[0, 'Score'] = team_wins + (ties * 0.5)
            df.loc[1, 'Score'] = opponent_wins + (ties * 0.5)
            
            # Update opponent score after adjustment
            df['Opponent_Score'] = df.loc[1, 'Score']

            # Calculate score differences and normalize them (using adjusted scores)
            df['Score_Difference'] = df['Score'] - df['Opponent_Score']
            min_value = -1 * 12  # Max possible difference is now 12 (12-0 with no ties)
            max_value = 12
            df['Normalized_Score_Difference'] = (df['Score_Difference'] - min_value) / (max_value - min_value)
            
            print(f"Week {week}, Matchup {matchup}: Raw scores {team_wins}-{opponent_wins}, Ties: {ties}, Adjusted scores: {df.loc[0, 'Score']}-{df.loc[1, 'Score']}")

            # Instead of append, use pd.concat with df.loc[[0]] to ensure it's a DataFrame
            weekly_results_df = pd.concat([weekly_results_df, df.loc[[0]]], ignore_index=True)
            print(weekly_results_df)

    weekly_results_df = build_team_numbers(weekly_results_df)
    return weekly_results_df 

def get_weekly_stats(num_teams, leaguedf, most_recent_week):
    thisWeek = set_this_week()
    running_df = pd.DataFrame()
    for week in range((most_recent_week + 1), thisWeek):
        # Function below sets up the dataframe for the all-play function
        if most_recent_week + 1 == thisWeek:
            pass
        else:
            print(f"Processing weekly stats for week {week}...")
            allPlaydf = leaguedf.copy()
            for matchup in range(1, (num_teams + 1)):
                print(f"  Processing stats matchup {matchup}/{num_teams} for week {week}")
                # Sleep timer to avoid rapid requests to Yahoo servers
                soup = url_requests(YAHOO_LEAGUE_ID + 'matchup?week=' + str(week) + '&module=matchup&mid1=' + str(matchup))
                
                # Add delay after each Yahoo API request to prevent throttling
                time.sleep(2)  # 2 second delay between requests
                table = soup.find_all('table')
                df = pd.read_html(str(table))[1]
                df['Week'] = week
                print(df)
                df.columns = df.columns.str.replace('[#,@,&,/,+]', '', regex=True)
                df.columns = df.columns.str.replace('HR.1', 'HRA')

                for column in df.columns:
                    if column in percentage_categories:
                        # Handle asterisks for percentage stats when ties occur
                        df[column] = df[column].astype(str)
                        df[column] = df[column].map(lambda x: x.rstrip('*'))
                        df[column] = df[column].replace(['-'], '0.00')
                        df[column] = df[column].astype(float)

                column_list = leaguedf.columns.tolist()
                df = df[column_list]
                df['Opponent'] = df.loc[1, 'Team']

                # Use pd.concat instead of append, wrapping df.loc[[0]] in a list
                allPlaydf = pd.concat([allPlaydf, df.loc[[0]]], ignore_index=True)
            
            logger.info(f'Week: {week}')
            # Concatenate allPlaydf into running_df
            running_df = pd.concat([running_df, allPlaydf], ignore_index=True)
            print(running_df)
    return running_df

def get_running_stats(df):
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    # Exclude 'Team', 'Week', 'Opponent' columns
    cols_to_sum = [col for col in df.columns if col not in ['Team', 'Week', 'Opponent']]

    # Create a new DataFrame to store the running totals or averages
    totals_df = pd.DataFrame(columns=df.columns)

    # Group the DataFrame by 'Team' column
    grouped = df.groupby('Team')

    # Initialize a dictionary to store the running totals or averages for each team
    team_totals = {}

    # Iterate through each group (team) and calculate running totals or averages
    for team, group in grouped:
        team_totals[team] = {}
        running_totals = {col: 0 for col in cols_to_sum}

        for _, row in group.iterrows():
            for col in cols_to_sum:
                if col in percentage_categories:
                    if row['Week'] == 1:
                        running_totals[col] = row[col]
                    else:
                        running_totals[col] = (running_totals[col] * (row['Week'] - 1) + row[col]) / row['Week']
                else:
                    print(row)
                    running_totals[col] += row[col]
            team_totals[team][row['Week']] = running_totals.copy()

    # Iterate through the original DataFrame and populate totals_df with the running totals or averages
    for _, row in df.iterrows():
        team = row['Team']
        week = row['Week']
        running_totals = team_totals[team][week]
        # Create a new DataFrame from the dictionary merge and concatenate it to totals_df
        totals_df = pd.concat([totals_df, pd.DataFrame([{**row, **running_totals}])], ignore_index=True)

    # Sort totals_df by 'Week' and 'Team' columns
    totals_df = totals_df.sort_values(['Week', 'Team'])

    # Iterate through each week and rank teams in each category
    for week in range(1, totals_df['Week'].max() + 1):
        week_mask = totals_df['Week'] <= week
        week_df = totals_df[week_mask]

        for col in cols_to_sum:
            rank_col = col + '_Rank_Stats'
            if col in percentage_categories:
                if col in Low_Categories:
                    ranks = week_df.groupby('Week')[col].rank(ascending=True)
                else:
                    ranks = week_df.groupby('Week')[col].rank(ascending=False)
            else:
                if col in Low_Categories:
                    ranks = week_df.groupby('Week')[col].rank(ascending=True)
                else:
                    ranks = week_df.groupby('Week')[col].rank(ascending=False)
            totals_df.loc[week_mask, rank_col] = ranks

    # Get columns with '_Rank_Stats'
    rank_stats_cols = [col for col in totals_df.columns if '_Rank_Stats' in col]

    # Calculate the average for each week and team
    averages = totals_df.groupby(['Week', 'Team'])[rank_stats_cols].mean().reset_index()
    averages['Stats_Power_Rank'] = averages[rank_stats_cols].mean(axis=1)

    # Merge the averages with totals_df
    totals_df = totals_df.merge(averages[['Week', 'Team', 'Stats_Power_Rank']], on=['Week', 'Team'])
    totals_df = totals_df.sort_values(['Week', 'Team'])
    print(totals_df)
    return totals_df

def main():
    num_teams = league_size()
    leaguedf = league_stats_all_df()
    lastWeek = set_last_week()

    # Set this to True to reprocess all weeks with corrected tie scoring
    REPROCESS_ALL_WEEKS = False

    try:
        # Aggregate W/L throughout season
        df = storage.get_historical_data('weekly_results')
        print(df)

        if REPROCESS_ALL_WEEKS:
            print("REPROCESSING ALL WEEKS with corrected tie scoring...")
            weekly_results_df = get_weekly_results(num_teams, 0)
            if weekly_results_df is not None and not weekly_results_df.empty:
                print("Reprocessed weekly results with tie adjustments:")
                print(weekly_results_df)
                for week, week_df in weekly_results_df.groupby('Week'):
                    storage.append_weekly_data('weekly_results', int(week), week_df)
        elif not df.empty:
            max_week = df['Week'].max()
            weekly_results_df = get_weekly_results(num_teams, max_week)
            if weekly_results_df is not None and not weekly_results_df.empty:
                print(weekly_results_df)
                for week, week_df in weekly_results_df.groupby('Week'):
                    storage.append_weekly_data('weekly_results', int(week), week_df)
        else:
            weekly_results_df = get_weekly_results(num_teams, 0)
            if weekly_results_df is not None and not weekly_results_df.empty:
                for week, week_df in weekly_results_df.groupby('Week'):
                    storage.append_weekly_data('weekly_results', int(week), week_df)

        # Aggregate Stats
        rank_df = storage.get_historical_data('weekly_stats')
        if not rank_df.empty:
            max_week = rank_df['Week'].max()
            weekly_stats_df = get_weekly_stats(num_teams, leaguedf, max_week)
            if weekly_stats_df is not None and not weekly_stats_df.empty:
                print(weekly_stats_df)
                for week, week_df in weekly_stats_df.groupby('Week'):
                    storage.append_weekly_data('weekly_stats', int(week), week_df)
        else:
            weekly_stats_df = get_weekly_stats(num_teams, leaguedf, 0)
            if weekly_stats_df is not None and not weekly_stats_df.empty:
                for week, week_df in weekly_stats_df.groupby('Week'):
                    storage.append_weekly_data('weekly_stats', int(week), week_df)

        # Generate ranks and running ranks in lieu of running power ranks which started at the beginning of the season
        weekly_stats_df = storage.get_historical_data('weekly_stats')
        run_stats_df = get_running_stats(weekly_stats_df)
        storage.write_live_data('power_ranks_lite', run_stats_df)

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(error_message)
        # send_failure_email(error_message, filename)

if __name__ == '__main__':
    main()
