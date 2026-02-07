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
from categories_dict import Low_Categories

# Load obfuscated strings from .env file
load_dotenv()

storage = DynamoStorageManager(region='us-west-2')

#league_tuples_all = [('2019','14350'),('2018','885'),('2017','22458'),('2016','10284')]
#league_tuples = ('2023','23893'),('2022','11602'),('2021','23999')


#Returns dfs of stats of categories  
def all_time_stats_batting_df(df):
    # Get Batting Records by going to stats page
    
    column_names = df.columns

    # Iterate over the column names
    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            # Replace the column name with the corresponding value from the dictionary
            column_names.values[i] = batting_abbreviations[column]

    return df

def all_time_stats_pitching_df(df):
    # Get Batting Records by going to stats page
    
    column_names = df.columns

    # Iterate over the column names and modify them according to the logic
    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    # Update the column names in the DataFrame
    df.columns = column_names


    return df


def get_stats(year):

    import pandas as pd

    # Define the path to your Excel file
    file_path = r'S:\\North_Rockies\\Jonah\\GIS\\GIS_V2\\__pycache__\\YahooFantasyBaseball\\YahooFantasyBaseball_2023\\docs\\Legacy_2023.xlsx'

    # Load the first sheet by index (0 for the first sheet)
    df_first = pd.read_excel(file_path, sheet_name=0)

    # Load the second sheet by index (1 for the second sheet)
    df_second = pd.read_excel(file_path, sheet_name=1)

    # Display the first few rows of each DataFrame
    print("First Sheet:")
    print(df_first.head())
    filtered_df_batting = df_first[df_first['Year'] == year]

    print("\nSecond Sheet:")
    print(df_second.head())
    filtered_df_pitching = df_second[df_second['Year'] == year]

    dfb = all_time_stats_batting_df(filtered_df_batting)
    dfp = all_time_stats_pitching_df(filtered_df_pitching)

    print(dfb)
    print(dfp)

    df=reduce(lambda x,y: pd.merge(x,y, on=['Team Name','Manager','Year'], how='outer'), [dfb, dfp])  

    print(df)
    return df
        
# Normalized Ranks 
def get_normalized_ranks(all_time_rank_df):
    print(all_time_rank_df)
    #parse through columns and figure out which ones are low-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in all_time_rank_df.columns:
        if column == 'Team Name' or column == 'Manager' or column == 'Year':
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

        # Calculate Score for each LOW column grouped by team_number
    for column in low_columns_to_analyze:
        print(all_time_rank_df)
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        scaled_values = 100 - ((all_time_rank_df[column] - min_value) / (max_value - min_value)) * 100
        print(scaled_values)
        all_time_rank_df[column + '_Score'] = scaled_values

    for column in high_columns_to_analyze:
        min_score = 0  # Set the desired minimum Score value
        min_value = all_time_rank_df[column].min()
        max_value = all_time_rank_df[column].max()
        
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        
        # Calculate and assign the scaled Score values
        all_time_rank_df[column + '_Score'] = scaler.fit_transform(all_time_rank_df[column].values.reshape(-1, 1))    
    


    # Get the list of Score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum the Score columns
    all_time_rank_df['Score_Sum'] = all_time_rank_df[score_columns].sum(axis=1)
    all_time_rank_df['Score_Rank'] = all_time_rank_df['Score_Sum'].rank(ascending=False)
    #all_time_rank_df['Score_Sum'] = all_time_rank_df['Score_Sum'] * 1.2
    all_time_rank_df = all_time_rank_df.rename(columns={'Team Name': 'Team'})
    print(all_time_rank_df)

    return all_time_rank_df


def main():
    try:
        current_year = datetime.now().year
        for year in range(current_year, current_year+1):
            all_time_rank_df = get_stats(year)
            normalized_ranks_df = get_normalized_ranks(all_time_rank_df)
            storage.write_all_time_data(year, normalized_ranks_df)
            print(f'Write Normalized Ranks')
                    
            
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        send_failure_email(error_message, filename)




if __name__ == '__main__':
    main()
