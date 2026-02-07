import pandas as pd
import bs4 as bs
import http
import urllib
import urllib.request
from urllib.request import urlopen as uReq
import os
import time
import urllib.error
import requests
from categories_dict import *

from dotenv import load_dotenv 

load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

# --- First definition of url_requests (using urllib) ---
def url_requests(url):
    max_retries = 10
    retry_count = 0
    retry = True
    soup = None

    while retry and retry_count < max_retries:
        try:
            # Your code here using urllib
            source = urllib.request.urlopen(url).read()
            soup = bs.BeautifulSoup(source, 'lxml')
            time.sleep(2)
            retry = False
        except urllib.error.HTTPError as e:
            if e.code == 404:
                print("HTTP Error 404: Not Found. Retrying...")
                time.sleep(5)
            else:
                print(f"An HTTP error occurred. Error code: {e.code}. Retrying...")
                time.sleep(10)
        except http.client.IncompleteRead:
            print("Incomplete read error occurred. Retrying...")
            time.sleep(5)
        retry_count += 1

    return soup

# --- Second definition of url_requests (this one overwrites the first) ---
def url_requests(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Error retrieving URL: {url} returned status code {response.status_code}")
    # Correctly call BeautifulSoup from the bs4 module
    return bs.BeautifulSoup(response.text, 'html.parser')


# Get Number of Teams
def league_size():
    soup = url_requests(YAHOO_LEAGUE_ID)
    
    # Find all tables in the page
    table = soup.find_all('table')

    # Parse the first table found into a DataFrame using pandas
    df_seasonRecords = pd.read_html(str(table))[0]
    
    # Return the number of teams (assumed to be the number of rows)
    return len(df_seasonRecords)


def build_team_numbers(df):
    soup = url_requests(YAHOO_LEAGUE_ID)

    table = soup.find('table')  # Use find() to get the first table

    # Extract all href links from the table, if found
    if table is not None:
        links = []
        for link in table.find_all('a'):  # Find all <a> tags within the table
            link_text = link.text.strip()  # Extract the hyperlink text
            link_url = link['href']  # Extract the href link
            if link_text != '':
                links.append((link_text, link_url))  # Append the hyperlink text and link to the list

    # Map team numbers from the dictionary to a new Series
    # Iterate through the rows of the DataFrame
    for index, row in df.iterrows():
        team_name = row['Team']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:]  # Grab the last 2 characters if they are both digits, else grab the last character
                df.at[index, 'Team_Number'] = team_number
                break

    return df
    

def build_opponent_numbers(df):
    soup = url_requests(YAHOO_LEAGUE_ID)

    table = soup.find('table')  # Use find() to get the first table

    # Extract all href links from the table, if found
    if table is not None:
        links = []
        for link in table.find_all('a'):  # Find all <a> tags within the table
            link_text = link.text.strip()  # Extract the hyperlink text
            link_url = link['href']  # Extract the href link
            if link_text != '':
                links.append((link_text, link_url))  # Append the hyperlink text and link to the list
    print(df)
    for index, row in df.iterrows():
        team_name = row['Opponent']
        for link in links:
            if link[0] == team_name:
                team_number = link[1][-2:] if link[1][-2:].isdigit() else link[1][-1:]  # Grab the last 2 characters if they are both digits, else grab the last character
                df.at[index, 'Opponent_Number'] = team_number
                break

    return df
    
  
def category_size():
    # Batting Records
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()
    dfb.pop(0)

    # Pitching Records
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    dfp.pop(0)

    combined_list = dfb + dfp
    
    return len(combined_list)


# Returns List of Stat Categories 
def league_stats_batting():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()
    
    updated_list = [batting_abbreviations.get(item, item) for item in dfb]
    updated_list.pop(0)
    
    return updated_list


def league_stats_pitching():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    
    updated_list = [pitching_abbreviations.get(item, item) for item in dfp]
    updated_list.pop(0)
    
    return updated_list


def league_record_pitching_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    column_names = dfp.columns

    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    dfp.columns = column_names

    return dfp


def league_record_batting_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    column_names = dfb.columns

    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            column_names.values[i] = batting_abbreviations[column]

    return dfb


def league_stats_batting_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=stats')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    column_names = dfb.columns

    for i, column in enumerate(column_names):
        if column in batting_abbreviations:
            column_names.values[i] = batting_abbreviations[column]

    return dfb


def league_stats_pitching_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=stats')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    column_names = dfp.columns

    for i, column in enumerate(column_names):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            column_names.values[i] = new_column_name

    dfp.columns = column_names

    return dfp


def league_stats_all_play_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()

    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    dfp.pop(0)
    dfp = [pitching_abbreviations.get(item, item) for item in dfp]

    # Note: The following loop attempting to modify dfp.values[i] is unnecessary since dfp is a list.
    # You can remove or adjust this section if needed.
    for i, column in enumerate(dfp):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            # This line would not work because dfp is a list, not a DataFrame.
            # dfp.values[i] = new_column_name

    combined_list = dfb + dfp
    combined_list.insert(1, 'Week')

    df = pd.DataFrame(columns=combined_list)
    df = df.rename(columns={'Team Name': 'Team'})
    
    return df


def league_stats_all_df():
    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=B&type=record')
    table = soup.find_all('table')
    dfb = pd.read_html(str(table))[0]
    dfb = dfb.columns.tolist()

    soup = url_requests(YAHOO_LEAGUE_ID + 'headtoheadstats?pt=P&type=record')
    table = soup.find_all('table')
    dfp = pd.read_html(str(table))[0]
    dfp = dfp.columns.tolist()
    dfp.pop(0)
    dfp = [pitching_abbreviations.get(item, item) for item in dfp]

    # As above, this loop is attempting to modify a list in a DataFrame-like way.
    for i, column in enumerate(dfp):
        if column in pitching_abbreviations:
            new_column_name = pitching_abbreviations[column]
            # dfp.values[i] = new_column_name  # Not applicable for lists

    combined_list = dfb + dfp
    combined_list.insert(1, 'Week')

    df = pd.DataFrame(columns=combined_list)
    df = df.rename(columns={'Team Name': 'Team'})
    
    return df
