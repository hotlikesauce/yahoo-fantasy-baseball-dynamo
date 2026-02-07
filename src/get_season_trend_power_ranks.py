from calendar import week
from msilib.schema import Error
import pandas as pd
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import bs4 as bs
from functools import reduce
import time, datetime, os, sys
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


def get_records():
    
    # Get Actual Records by looking up standings table on league home page
    soup = url_requests(YAHOO_LEAGUE_ID)
    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec=df_rec.rename(columns = {'Team':'Team Name'})
    
    batting_list = league_stats_batting()
    pitching_list = league_stats_pitching()

    dfb = league_record_batting_df()
    dfp = league_record_pitching_df()

    #Batting
    # split up columns into W-L-D
    for column in dfb:
        if str(column) == 'Team Name':
            pass
        else:
            # new data frame with split value columns
            new = dfb[column].str.split("-", n = 2, expand = True)
            
            # making separate first name column from new data frame
            dfb[str(column)+"_Win"]= new[0]
            dfb[str(column)+"_Loss"]= new[1]
            dfb[str(column)+"_Draw"]= new[2]
    
    #YOU ARE HERE. NEED TO RENAME AND ADJUST, LOOP THROUGH ALL CATS AND CREATE
    for cat in batting_list:
        cat_Win = f'{cat}_Win'
        cat_Draw = f'{cat}_Draw'
        cat_Loss = f'{cat}_Loss'
        dfb[str(cat)] = list(zip(dfb[cat_Win], dfb[cat_Draw], dfb[cat_Loss]))

    # convert tuples to ints
    dfb[str(cat)] = tuple(tuple(map(int, tup)) for tup in  dfb[cat])  

    dfb.columns = dfb.columns.str.replace('[#,@,&,/,+]', '')

    #Pitching
    for column in dfp:
        if str(column) == 'Team Name':
            pass
        else:
            # new data frame with split value columns
            new = dfp[column].str.split("-", n = 2, expand = True)
            
            # making separate first name column from new data frame
            dfp[str(column)+"_Win"]= new[0]
            dfp[str(column)+"_Loss"]= new[1]
            dfp[str(column)+"_Draw"]= new[2]
    
    #YOU ARE HERE. NEED TO RENAME AND ADJUST, LOOP THROUGH ALL CATS AND CREATE
    for cat in pitching_list:
        cat_Win = f'{cat}_Win'
        cat_Draw = f'{cat}_Draw'
        cat_Loss = f'{cat}_Loss'
        dfp[str(cat)] = list(zip(dfp[cat_Win], dfp[cat_Draw], dfp[cat_Loss]))

    # convert tuples to ints
    dfp[str(cat)] = tuple(tuple(map(int, tup)) for tup in  dfp[cat])  

    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')     
    

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp,df_rec])

    print(df)

    # define columns for next df
    df=df[['Team Name'] + batting_list + pitching_list + ['Rank', 'GB', 'Moves']]
    
    # Create a team ranking based on records in all stat categories
    for column in df:
        if column in ['Team Name','Rank','GB','Moves']:
            pass
        else:
            df[column+'_Rank'] = df[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
    
    # change col names to be record independent   
    keep_same = {'Team Name','Rank','GB','Moves'}
    df.columns = ['{}{}'.format(c, '' if c in keep_same else '_Record') for c in df.columns]
    
    df = df.dropna()
    print(df)
    return df

def get_stats(records_df):
    
    num_teams = league_size()
    dfb = league_stats_batting_df()
    dfp = league_stats_pitching_df()
    
    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])



    for column in df:
        if column == 'Team Name':
            pass
        # ERA, WHIP, and HRA need to be ranked descending
        elif column in Low_Categories:
            df[column+'_Rank'] = df[column].rank(ascending = True)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
        # All others ranked ascending
        else:
            df[column+'_Rank'] = df[column].rank(ascending = False)
            # Set the index to newly created column, Rating_Rank
            df.set_index(column+'_Rank')
    

    #Change col names to be stats independent
    keep_same = {'Team Name'}
    df.columns = ['{}{}'.format(c, '' if c in keep_same else '_Stats') for c in df.columns]
    
    df_merge=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [df, records_df])
    
    columns_to_calculate = [col for col in df_merge.columns if '_Rank_Stats' in col]
    df_merge['Stats_Power_Score'] = df_merge[columns_to_calculate].sum(axis=1) / num_teams


    df_merge['Stats_Power_Rank'] = df_merge['Stats_Power_Score'].rank(ascending = True)
    
    
    # Teams will clinch playoffs and you need to remove the asterisks next to their names
    try:        
        df_merge['Rank'] = df_merge['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet, yo")
    
    # Variation is the difference between your stat ranking and you actual ranking
    df_merge['Variation'] = df_merge['Stats_Power_Rank'] - df_merge['Rank'] 

    # Create a new column for the batter rank
    columns_to_calculate = [col for col in df_merge.columns if col in Batting_Rank_Stats]
    df_merge['batter_rank'] = df_merge[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    # Create a new column for the pitcher rank
    columns_to_calculate = [col for col in df_merge.columns if col in Pitching_Rank_Stats]
    df_merge['pitcher_rank'] = df_merge[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    df_merge = df_merge.rename(columns={'Team Name': 'Team'})

    df_merge_teams = build_team_numbers(df_merge)  
    
    return df_merge_teams

def running_normalized_ranks(week_df):
    # Columns to analyze
    high_columns_to_analyze = ['R_Stats', 'H_Stats', 'HR_Stats', 'RBI_Stats', 'SB_Stats', 'OPS_Stats','K9_Stats', 'QS_Stats', 'SVH_Stats' ]

    low_columns_to_analyze = ['ERA_Stats', 'WHIP_Stats', 'HRA_Stats']

    # Calculate Score for each column grouped by team_number
    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = week_df[column].min()
        max_value = week_df[column].max()

        scaler = MinMaxScaler(feature_range=(min_score, 100))

        # Calculate and assign the scaled Score values
        week_df[column + '_Score'] = scaler.fit_transform(week_df[column].values.reshape(-1, 1))    

    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = week_df[column].min()
        max_value = week_df[column].max()

        scaler = MinMaxScaler(feature_range=(min_score, 100))

        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((week_df[column] - min_value) / (max_value - min_value)) * 100
        week_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    week_df['Score_Sum'] = week_df[score_columns].sum(axis=1)
    week_df['Score_Rank'] = week_df['Score_Sum'].rank(ascending=False)
    #week_df['Score_Variation'] = week_df['Score_Rank'] - week_df['Rank']

    return week_df

def main():
    #This is for if the analysis started midseason. If that's the case, we default to the get_weekly_results power rankings
    try:
        df = storage.get_historical_data('power_ranks_season_trend')
        if not df.empty and df is not None:
            lastWeek = set_last_week()
            records_df = get_records()
            power_rank_df = get_stats(records_df)
            storage.append_weekly_data('power_ranks_season_trend', lastWeek, power_rank_df)
        else:
            print('no soup for you')
            pass

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)



if __name__ == '__main__':
    main()