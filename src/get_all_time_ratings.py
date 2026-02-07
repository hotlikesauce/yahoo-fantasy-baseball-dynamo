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

storage = DynamoStorageManager(region='us-west-2')

#league_tuples_all = [('2019','14350'),('2018','885'),('2017','22458'),('2016','10284')]
league_tuples = ('2023','23893'),('2022','11602'),('2021','23999')

#Returns dfs of stats of categories  
def all_time_stats_batting_df(year,id):
    # Get Batting Records by going to stats page
    print(f'https://baseball.fantasysports.yahoo.com/{year}/b1/{id}/headtoheadstats?pt=B&type=stats')
    soup = url_requests(f'https://baseball.fantasysports.yahoo.com/{year}/b1/{id}/headtoheadstats?pt=B&type=stats')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfb = pd.read_html(str(table))[0]

    column_names = dfb.columns

    # Iterate over the column names
    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            # Replace the column name with the corresponding value from the dictionary
            column_names.values[i] = batting_abbreviations[column]

    return dfb

def all_time_stats_pitching_df(year,id):
    # Get Batting Records by going to stats page
    soup = url_requests(f'https://baseball.fantasysports.yahoo.com/{year}/b1/{id}/headtoheadstats?pt=P&type=stats')

    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    dfp = pd.read_html(str(table))[0]

    column_names = dfp.columns

    # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    # Update the column names in the DataFrame
    dfp.columns = column_names


    return dfp


def get_stats(year,id):

    num_teams = league_size()
    dfb = all_time_stats_batting_df(year,id)
    dfp = all_time_stats_pitching_df(year,id)

    df=reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])

    print(df.columns)
    
    df['Year'] = year

    return df
        
# Normalized Ranks 
def get_normalized_ranks(all_time_rank_df):
 
    #parse through columns and figure out which ones are low-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in all_time_rank_df.columns:
        if column == 'Team Name':
            pass
        elif column in Low_Categories:
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

    print(all_time_rank_df)
    return all_time_rank_df

def get_managers(normalized_ranks_df,year,id):
    soup = url_requests(f'https://baseball.fantasysports.yahoo.com/{year}/b1/{id}/teams')
    table = soup.find_all('table')
    # dfb (data frame batting) will be the list of pitching stat categories you have
    managers_df = pd.read_html(str(table))[0]
    merged_df = pd.merge(normalized_ranks_df, managers_df[['Team Name', 'Manager']], on='Team Name', how='left')
    merged_df = merged_df.rename(columns={'Team Name': 'Team'})

    return merged_df

def main():
    try:
        for year, id in league_tuples:
            all_time_rank_df = get_stats(year, id)
            normalized_ranks_df = get_normalized_ranks(all_time_rank_df)
            merged_df = get_managers(normalized_ranks_df, year, id)
            storage.write_all_time_data(int(year), merged_df)
            print(f'Write Normalized Ranks')
                
        
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)




if __name__ == '__main__':
    main()
