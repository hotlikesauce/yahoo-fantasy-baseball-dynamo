import pandas as pd
import requests
from bs4 import BeautifulSoup
import os, sys, traceback, logging
from dotenv import load_dotenv
import time
import datetime as dt

# Local Modules
from email_utils import send_failure_email
from storage_manager import DynamoStorageManager
from datetime_utils import *

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')

logging.basicConfig(filename='error.log', level=logging.ERROR)

def check_team_minimum_innings(team_number):
    """
    Check if a specific team has met the 40 innings pitched minimum
    
    Args:
        team_number: Team number (1-12)
    
    Returns:
        dict with team info and minimum IP status
    """
    try:
        # Construct the URL for the specific team
        url = f"https://baseball.fantasysports.yahoo.com/b1/30332/{team_number}"
        
        print(f"Checking team {team_number}: {url}")
        
        # Make request with headers to avoid blocking
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: Failed to fetch team {team_number} page. Status code: {response.status_code}")
            return {
                'Team_Number': team_number,
                'Team_Name': 'Unknown',
                'Current_IP': None,
                'Minimum_IP_Met': None,
                'Error': f"HTTP {response.status_code}"
            }
        
        # Parse the HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the minimum IP span with class F-negative
        # Looking for: <span class="F-negative" id="...">32.2 of 40.0</span>
        min_ip_span = soup.find('span', class_='F-negative')
        
        if not min_ip_span:
            # Also try to find any span containing "of 40.0" as backup
            all_spans = soup.find_all('span')
            for span in all_spans:
                if span.get_text() and 'of 40.0' in span.get_text():
                    min_ip_span = span
                    break
        
        if not min_ip_span:
            print(f"Warning: Could not find minimum IP span for team {team_number}")
            return {
                'Team_Number': team_number,
                'Team_Name': 'Unknown',
                'Current_IP': None,
                'Minimum_IP_Met': None,
                'Error': 'Minimum IP span not found'
            }
        
        # Extract the minimum IP text
        min_ip_text = min_ip_span.get_text(strip=True)
        print(f"Team {team_number} minimum IP text: '{min_ip_text}'")
        
        # Try to extract team name from the page
        team_name = 'Unknown'
        try:
            # Look for team name in various possible locations
            team_name_element = soup.find('h1') or soup.find('title')
            if team_name_element:
                team_name = team_name_element.get_text(strip=True)
        except:
            pass
        
        # Parse the minimum IP information
        # Expected format: "32.2 of 40.0" or similar
        current_ip = None
        minimum_ip_met = False
        
        if 'of 40.0' in min_ip_text:
            # Format: "32.2 of 40.0"
            try:
                import re
                match = re.search(r'([\d.]+)\s+of\s+40\.0', min_ip_text)
                if match:
                    current_ip = float(match.group(1))
                    minimum_ip_met = current_ip >= 40.0
                    print(f"Team {team_number}: Current IP = {current_ip}, Minimum met = {minimum_ip_met}")
            except Exception as e:
                print(f"Error parsing IP text '{min_ip_text}': {e}")
        elif min_ip_text == '' or 'minimum met' in min_ip_text.lower():
            # Empty span or explicit "minimum met" text might mean minimum is met
            minimum_ip_met = True
            current_ip = 40.0
        
        return {
            'Team_Number': team_number,
            'Team_Name': team_name,
            'Current_IP': current_ip,
            'Minimum_IP_Met': minimum_ip_met,
            'Raw_Text': min_ip_text,
            'Error': None
        }
        
    except Exception as e:
        print(f"Error checking team {team_number}: {str(e)}")
        return {
            'Team_Number': team_number,
            'Team_Name': 'Unknown',
            'Current_IP': None,
            'Minimum_IP_Met': None,
            'Error': str(e)
        }

def check_all_teams_minimum_innings():
    """
    Check minimum innings pitched for all 12 teams
    
    Returns:
        DataFrame with results for all teams
    """
    try:
        print(f"Starting minimum innings check at {dt.datetime.now()}")
        
        # Get team names from team_dict if available
        storage = DynamoStorageManager(region='us-west-2')
        team_dict_df = storage.get_live_data('team_dict')
        team_name_mapping = {}
        if not team_dict_df.empty:
            team_dict_df['Team_Number'] = team_dict_df['Team_Number'].astype(str)
            team_name_mapping = dict(zip(team_dict_df['Team_Number'], team_dict_df['Team']))
        
        results = []
        
        for team_number in range(1, 13):  # Teams 1-12
            print(f"\nChecking team {team_number}...")
            
            result = check_team_minimum_innings(team_number)
            
            # Use team name from team_dict if available
            if str(team_number) in team_name_mapping:
                result['Team_Name'] = team_name_mapping[str(team_number)]
            
            results.append(result)
            
            # Add delay between requests to be respectful to Yahoo's servers
            time.sleep(3)  # 3 second delay between team checks
        
        # Convert to DataFrame
        results_df = pd.DataFrame(results)
        
        # Add timestamp
        results_df['Check_Time'] = dt.datetime.now()
        results_df['Week'] = set_this_week()
        
        # Display results
        print(f"\n" + "="*80)
        print("MINIMUM INNINGS PITCHED CHECK RESULTS")
        print("="*80)
        print(f"Check completed at: {dt.datetime.now()}")
        print(f"Week: {set_this_week()}")
        
        # Show teams that haven't met minimum
        teams_below_minimum = results_df[results_df['Minimum_IP_Met'] == False]
        if not teams_below_minimum.empty:
            print(f"\n⚠️  TEAMS BELOW 40 IP MINIMUM ({len(teams_below_minimum)} teams):")
            print("-" * 60)
            for _, row in teams_below_minimum.iterrows():
                current_ip = row['Current_IP'] if row['Current_IP'] is not None else 'Unknown'
                print(f"Team {row['Team_Number']} ({row['Team_Name']}): {current_ip} IP")
                if row['Raw_Text']:
                    print(f"   Status: {row['Raw_Text']}")
        else:
            print(f"\n✅ ALL TEAMS HAVE MET 40 IP MINIMUM!")
        
        # Show teams that have met minimum
        teams_above_minimum = results_df[results_df['Minimum_IP_Met'] == True]
        if not teams_above_minimum.empty:
            print(f"\n✅ TEAMS MEETING 40 IP MINIMUM ({len(teams_above_minimum)} teams):")
            print("-" * 60)
            for _, row in teams_above_minimum.iterrows():
                print(f"Team {row['Team_Number']} ({row['Team_Name']}): ✓ Minimum met")
        
        # Show any errors
        teams_with_errors = results_df[results_df['Error'].notna()]
        if not teams_with_errors.empty:
            print(f"\n❌ TEAMS WITH ERRORS ({len(teams_with_errors)} teams):")
            print("-" * 60)
            for _, row in teams_with_errors.iterrows():
                print(f"Team {row['Team_Number']} ({row['Team_Name']}): {row['Error']}")
        
        return results_df
        
    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        additional_info = f'Error occurred at line {line_number}'
        logging.error(f'{filename}: {error_message} - {additional_info}')
        print(f"Error in minimum innings check: {error_message}")
        raise

def main():
    try:
        # Check if it's Sunday (weekday 6) - uncomment for production
        # current_day = dt.datetime.now().weekday()
        # if current_day != 6:  # 6 = Sunday
        #     print(f"Script only runs on Sundays. Today is {dt.datetime.now().strftime('%A')}")
        #     return
        
        # Check minimum innings for all teams
        results_df = check_all_teams_minimum_innings()
        
        if not results_df.empty:
            # Save to database
            storage = DynamoStorageManager(region='us-west-2')
            storage.write_live_data('minimum_innings_check', results_df)
            print(f"\nMinimum innings check complete - {len(results_df)} teams checked and saved to DynamoDB")
            
            # Send email notification if any teams are below minimum
            teams_below_minimum = results_df[results_df['Minimum_IP_Met'] == False]
            if not teams_below_minimum.empty:
                team_list = ', '.join([f"{row['Team_Name']} ({row['Current_IP']} IP)" 
                                     for _, row in teams_below_minimum.iterrows()])
                email_subject = f"⚠️ Teams Below 40 IP Minimum - Week {set_this_week()}"
                email_message = f"The following teams have not met the 40 innings pitched minimum:\n\n{team_list}"
                try:
                    send_failure_email(email_message, email_subject)
                except:
                    print("Could not send email notification")
        else:
            print("No minimum innings data to save")
        
    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        additional_info = f'Error occurred at line {line_number}'
        logging.error(f'{filename}: {error_message} - {additional_info}')
        print(f"Error in main: {error_message}")
        send_failure_email(f"{error_message} - {additional_info}", filename)

if __name__ == '__main__':
    main()