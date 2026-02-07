import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
from datetime import datetime
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

   
    # Drop unnecessary columns
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
    print(df_merge.columns)
    
    
    # Create a new column for the batter rank
    ##HERE IS WHERE YOU ARE - NEED TO FIGURE OUT HOW TO BREAK OUT BATTING AND PITCHING COLS TO CALCULATE
    columns_to_calculate = [col for col in df_merge.columns if col in Batting_Rank_Stats]
    df_merge['batter_rank'] = df_merge[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    # Create a new column for the pitcher rank
    columns_to_calculate = [col for col in df_merge.columns if col in Pitching_Rank_Stats]
    df_merge['pitcher_rank'] = df_merge[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    df_merge = df_merge.rename(columns={'Team Name': 'Team'})

    df_merge_teams = build_team_numbers(df_merge)  
    
    #print(df_merge_teams)

    return df_merge_teams
        
# Normalized Ranks 
def get_normalized_ranks(power_rank_df):
 
    #parse through columns and figure out which ones are low-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in power_rank_df.columns:
        if '_Stats' in column and '_Rank_Stats' not in column:
            if column in Low_Categories_Stats:
                low_columns_to_analyze.append(column)
            else:
                high_columns_to_analyze.append(column)
        else:
            pass
    # Calculate Score for each column grouped by team_number
    
    print(low_columns_to_analyze)
    print(high_columns_to_analyze)

    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = power_rank_df[column].min()
        max_value = power_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        power_rank_df[column + '_Score'] = scaler.fit_transform(power_rank_df[column].values.reshape(-1, 1))    
    
    # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = power_rank_df[column].min()
        max_value = power_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((power_rank_df[column] - min_value) / (max_value - min_value)) * 100
        power_rank_df[column + '_Score'] = scaled_values

    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    power_rank_df['Score_Sum'] = power_rank_df[score_columns].sum(axis=1)
    power_rank_df['Score_Rank'] = power_rank_df['Score_Sum'].rank(ascending=False)
    power_rank_df['Score_Variation'] = power_rank_df['Score_Rank'] - power_rank_df['Rank']

    print(power_rank_df)

    return power_rank_df


def main():
    try:
        records_df = get_records()
        power_rank_df = get_stats(records_df)
        storage.write_live_data('Power_Ranks', power_rank_df)
        storage.write_live_data('power_ranks', power_rank_df)
        print('Wrote out Power Ranks Stats')

        lastWeek = set_last_week()
        normalized_ranks_df = get_normalized_ranks(power_rank_df)
        storage.write_live_data('normalized_ranks', normalized_ranks_df)
        print(f'Write Normalized Ranks')

        normalized_ranks_df['Week'] = lastWeek
        storage.append_weekly_data('running_normalized_ranks', lastWeek, normalized_ranks_df)

        storage.append_weekly_data('power_ranks_season_trend', lastWeek, power_rank_df)
        print(f'Write Season Trend Power Ranks')

        current_year = datetime.now().year
        df_current = storage.get_live_data('normalized_ranks')
        df_current['Manager'] = df_current['Team_Number'].astype(str).map(manager_dict)
        df_current['Year'] = current_year
        print(df_current)
        storage.write_all_time_data(current_year, df_current)

    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)




if __name__ == '__main__':
    main()
