import pandas as pd
import bs4 as bs
import urllib
import urllib.request
from urllib.request import urlopen as uReq
from functools import reduce
import numpy as np
import os, logging, traceback, sys
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler

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

logging.basicConfig(filename='error.log', level=logging.ERROR)
# Custom function to convert values in the 'Week' column
def convert_to_int(x):
    if isinstance(x, dict):
        return int(x.get('$numberInt', 0))
    return int(x)

def add_team_names(df, team_col='Team_Number'):
    """Add team names to dataframe using team_dict"""
    df['Team_Name'] = df[team_col].astype(str).map(team_dict)
    return df

def calculate_sos_statistics(sos_df):
    """Calculate statistical insights for SOS analysis"""
    stats = {}
    stats['mean_sos'] = sos_df['Avg_Opponent_Power'].mean()
    stats['std_sos'] = sos_df['Avg_Opponent_Power'].std()
    stats['min_sos'] = sos_df['Avg_Opponent_Power'].min()
    stats['max_sos'] = sos_df['Avg_Opponent_Power'].max()
    stats['sos_range'] = stats['max_sos'] - stats['min_sos']
    
    # Calculate percentiles
    sos_df['SOS_Percentile'] = sos_df['Avg_Opponent_Power'].rank(pct=True) * 100
    
    return stats, sos_df

def create_detailed_schedule_breakdown(filtered_schedule_df, power_ranks_df):
    """Create detailed opponent-by-opponent breakdown"""
    # Merge schedule with power rankings to get opponent details
    detailed_df = filtered_schedule_df.merge(
        power_ranks_df, 
        left_on='Opponent_Team_Number', 
        right_on='Team_Number', 
        suffixes=('_schedule', '_power')
    )
    
    # Add opponent names
    detailed_df['Opponent_Name'] = detailed_df['Opponent_Team_Number'].astype(str).map(team_dict)
    
    # Select relevant columns for detailed breakdown
    breakdown_cols = ['Team_Number_schedule', 'Week', 'Opponent_Team_Number', 
                     'Opponent_Name', 'Score_Sum', 'Score_Rank']
    
    if all(col in detailed_df.columns for col in breakdown_cols):
        detailed_breakdown = detailed_df[breakdown_cols].copy()
        detailed_breakdown.rename(columns={
            'Team_Number_schedule': 'Team_Number',
            'Score_Sum': 'Opponent_Power_Score',
            'Score_Rank': 'Opponent_Power_Rank'
        }, inplace=True)
        
        # Sort by team and week for better organization
        detailed_breakdown = detailed_breakdown.sort_values(['Team_Number', 'Week'])
        return detailed_breakdown
    else:
        print("Warning: Some columns missing for detailed breakdown")
        return pd.DataFrame()

def get_remaining_sos(avg_opponent_power=None):
    """
    Calculate remaining strength of schedule for all teams
    
    Args:
        avg_opponent_power: Optional parameter to use specific power metric
    
    Returns:
        DataFrame with 12 records (one per team) containing SOS metrics
    """
    try:
        this_week = set_this_week()
        print(f"Current week: {this_week}")
        
        if this_week >= 22:  # Changed from 21 to 22 to include week 21
            print("No remaining regular season matchups - playoffs or season ended")
            return pd.DataFrame()
        
        # Get schedule data
        schedule_df = storage.get_schedule_data()
        if schedule_df.empty:
            print("Error: No schedule data found")
            return pd.DataFrame()
        
        print(f"Raw schedule data shape: {schedule_df.shape}")
        print("Sample raw schedule data:")
        print(schedule_df.head())
        
        schedule_df['Week'] = schedule_df['Week'].apply(convert_to_int)
        
        # Filter for remaining games INCLUDING current week (>= instead of >)
        filtered_schedule_df = schedule_df[schedule_df['Week'] >= this_week]
        
        print(f"After filtering for weeks >= {this_week}:")
        print(f"Filtered schedule shape: {filtered_schedule_df.shape}")
        print("Week distribution:")
        print(filtered_schedule_df['Week'].value_counts().sort_index())
        
        if filtered_schedule_df.empty:
            print("No remaining games found for any teams")
            return pd.DataFrame()
            
        print(f"Found {len(filtered_schedule_df)} remaining matchups")
        
        # Get power rankings data - try normalized first, fallback to regular
        try:
            power_ranks_df = storage.get_live_data('normalized_ranks')
            if power_ranks_df.empty:
                print("Warning: No normalized ranks found, trying regular power rankings")
                power_ranks_df = storage.get_live_data('power_ranks')
        except Exception as e:
            print(f"Warning: Error getting normalized ranks: {e}")
            power_ranks_df = storage.get_live_data('power_ranks')
            
        if power_ranks_df.empty:
            print("Error: No power ranking data found")
            return pd.DataFrame()
            
        print(f"Using power rankings with {len(power_ranks_df)} teams")
        
        # Debug: Show sample schedule data
        print("Sample schedule data:")
        print(filtered_schedule_df.head(10))
        
        # Debug: Show sample power rankings data
        print("Sample power rankings data:")
        print(power_ranks_df.head())
        
        # Fix data types BEFORE merge - ensure both are integers
        print(f"\nFixing data types before merge...")
        print(f"Schedule Opponent_Team_Number type before: {filtered_schedule_df['Opponent_Team_Number'].dtype}")
        print(f"Power rankings Team_Number type before: {power_ranks_df['Team_Number'].dtype}")
        
        filtered_schedule_df['Opponent_Team_Number'] = filtered_schedule_df['Opponent_Team_Number'].astype(int)
        filtered_schedule_df['Team_Number'] = filtered_schedule_df['Team_Number'].astype(int)
        power_ranks_df['Team_Number'] = power_ranks_df['Team_Number'].astype(int)
        
        print(f"Schedule Opponent_Team_Number type after: {filtered_schedule_df['Opponent_Team_Number'].dtype}")
        print(f"Power rankings Team_Number type after: {power_ranks_df['Team_Number'].dtype}")
        
        # Merge schedule with power rankings to get opponent strength
        # We join the opponent's team number with the power rankings team number
        merged_df = filtered_schedule_df.merge(
            power_ranks_df, 
            left_on='Opponent_Team_Number', 
            right_on='Team_Number', 
            suffixes=('', '_opponent_power')
        )
        
        if merged_df.empty:
            print("Error: No matches found between schedule and power rankings")
            print("Schedule opponent teams:", sorted(filtered_schedule_df['Opponent_Team_Number'].unique()))
            print("Power ranking teams:", sorted(power_ranks_df['Team_Number'].unique()))
            return pd.DataFrame()
        
        # Debug: Print column names and sample data after merge
        print(f"Merged DataFrame columns: {list(merged_df.columns)}")
        print("Sample merged data:")
        print(merged_df[['Team_Number', 'Opponent_Team_Number', 'Week', 'Score_Sum']].head(10))
        
        # Get team_dict for proper team names in debugging
        team_dict_df = storage.get_live_data('team_dict')
        if not team_dict_df.empty:
            print(f"\nTeam_dict data:")
            print(f"Team_dict shape: {team_dict_df.shape}")
            print(f"Team_dict columns: {list(team_dict_df.columns)}")
            print(f"Team_dict Team_Number type: {team_dict_df['Team_Number'].dtype}")
            print(f"Sample team_dict data:")
            print(team_dict_df.head())
            
            # Ensure Team_Number is string in team_dict for mapping
            team_dict_df['Team_Number'] = team_dict_df['Team_Number'].astype(str)
            team_name_mapping = dict(zip(team_dict_df['Team_Number'], team_dict_df['Team']))
            
            print(f"Team name mapping created: {team_name_mapping}")
        else:
            print("Warning: team_dict collection is empty")
            team_name_mapping = {}
        
        # Debug: Show games per team to identify any issues
        games_per_team = merged_df.groupby('Team_Number').size()
        print(f"\nGames remaining per team:")
        for team_num in sorted(games_per_team.index):
            team_name = team_name_mapping.get(str(team_num), f"Team {team_num}")
            print(f"Team {team_num} ({team_name}): {games_per_team[team_num]} games")
        
        # Debug: Show total opponent power per team before aggregation
        print(f"\nTotal opponent power per team (before final aggregation):")
        temp_totals = merged_df.groupby('Team_Number')['Score_Sum'].sum()
        for team_num in sorted(temp_totals.index):
            team_name = team_name_mapping.get(str(team_num), f"Team {team_num}")
            print(f"Team {team_num} ({team_name}): {temp_totals[team_num]:.2f}")
        
        # Debug: Check for any duplicate games or missing weeks
        print(f"\nWeek distribution in merged data:")
        week_counts = merged_df['Week'].value_counts().sort_index()
        for week in sorted(week_counts.index):
            print(f"Week {week}: {week_counts[week]} matchups (should be 12)")
        
        # Debug: Show actual opponent power scores from power rankings
        print(f"\nActual power scores from power rankings table:")
        power_display = power_ranks_df[['Team_Number', 'Score_Sum']].copy()
        power_display['Team_Name'] = power_display['Team_Number'].map(team_name_mapping)
        power_display = power_display.sort_values('Team_Number')
        for _, row in power_display.iterrows():
            team_name = row['Team_Name'] if pd.notna(row['Team_Name']) else f"Team {row['Team_Number']}"
            print(f"Team {row['Team_Number']} ({team_name}): {row['Score_Sum']}")
        
        # Debug: Find Josh specifically and show his detailed schedule
        josh_team_number = None
        for team_num, team_name in team_name_mapping.items():
            if 'josh' in team_name.lower():
                josh_team_number = int(team_num)
                break
        
        if josh_team_number:
            print(f"\n=== DETAILED JOSH ANALYSIS (Team {josh_team_number}) ===")
            josh_schedule = merged_df[merged_df['Team_Number'] == josh_team_number].sort_values('Week')
            
            print(f"Josh has {len(josh_schedule)} games in merged data")
            print("\nJosh's week-by-week schedule:")
            
            running_total = 0
            for _, row in josh_schedule.iterrows():
                opp_name = team_name_mapping.get(int(row['Opponent_Team_Number']), f"Team {row['Opponent_Team_Number']}")
                power_score = row['Score_Sum']
                running_total += power_score
                print(f"Week {row['Week']}: vs {opp_name} (Team {row['Opponent_Team_Number']}) - Power: {power_score} | Running Total: {running_total}")
            
            print(f"\nJosh's calculated total: {running_total}")
            print(f"Expected total: 4497.54")
            print(f"Difference: {4497.54 - running_total}")
            
            print(f"\nActual schedule data:")
            actual_weeks = {row['Week']: (int(row['Opponent_Team_Number']), row['Score_Sum']) 
                          for _, row in josh_schedule.iterrows()}
            
            for week in sorted(actual_weeks.keys()):
                team_num, power_score = actual_weeks[week]
                team_name = team_name_mapping.get(team_num, f"Team {team_num}")
                print(f"Week {week}: Team {team_num} ({team_name}) - Power: {power_score}")
            
            print(f"\nTo fix this, we need Josh's correct opponent team numbers for weeks 16-21")
            print(f"Current opponents by team number: {[actual_weeks[w][0] for w in sorted(actual_weeks.keys())]}")
            print(f"Current power scores: {[actual_weeks[w][1] for w in sorted(actual_weeks.keys())]}")
            
            # Show power rankings for reference
            print(f"\nPower rankings reference (Team_Number: Score_Sum):")
            power_ref = power_ranks_df[['Team_Number', 'Score_Sum']].sort_values('Team_Number')
            for _, row in power_ref.iterrows():
                team_name = team_name_mapping.get(int(row['Team_Number']), f"Team {row['Team_Number']}")
                print(f"Team {int(row['Team_Number'])} ({team_name}): {row['Score_Sum']}")
        else:
            print("Could not find Josh in team_dict")
        
        # Debug: Check team number data types and mismatches
        print(f"\nTeam number data type analysis:")
        print(f"Schedule Opponent_Team_Number type: {filtered_schedule_df['Opponent_Team_Number'].dtype}")
        print(f"Power rankings Team_Number type: {power_ranks_df['Team_Number'].dtype}")
        
        schedule_opponents = set(filtered_schedule_df['Opponent_Team_Number'].unique())
        power_teams = set(power_ranks_df['Team_Number'].unique())
        
        print(f"Schedule opponent team numbers: {sorted(schedule_opponents)}")
        print(f"Power rankings team numbers: {sorted(power_teams)}")
        
        missing_opponents = schedule_opponents - power_teams
        if missing_opponents:
            print(f"\nWARNING: Opponents in schedule but not in power rankings: {missing_opponents}")
        
        extra_power_teams = power_teams - schedule_opponents
        if extra_power_teams:
            print(f"Teams in power rankings but not as opponents: {extra_power_teams}")
        
        # Fix data types - ensure both are integers
        print(f"\nFixing data types...")
        filtered_schedule_df['Opponent_Team_Number'] = filtered_schedule_df['Opponent_Team_Number'].astype(int)
        filtered_schedule_df['Team_Number'] = filtered_schedule_df['Team_Number'].astype(int)
        power_ranks_df['Team_Number'] = power_ranks_df['Team_Number'].astype(int)
        
        # Use provided avg_opponent_power or default to Score_Sum
        power_metric = avg_opponent_power if avg_opponent_power and avg_opponent_power in merged_df.columns else 'Score_Sum'
        print(f"Using power metric: {power_metric}")
        
        # Group by the TEAM (not opponent) to calculate their SOS
        # We want to aggregate the power scores of each team's opponents
        sos_summary = merged_df.groupby('Team_Number').agg({
            power_metric: ['sum', 'mean', 'count']
        }).round(2)
        
        print(f"SOS summary shape: {sos_summary.shape}")
        print("SOS summary preview:")
        print(sos_summary.head())
        
        # Flatten column names
        sos_summary.columns = ['Total_Opponent_Power', 'Avg_Opponent_Power', 'Games_Remaining']
        sos_summary = sos_summary.reset_index()
        
        # Team_Number should already be the index name from groupby, so no renaming needed
        
        # Use the existing team_name_mapping that was already created above
        if team_name_mapping:
            sos_summary['Team_Name'] = sos_summary['Team_Number'].astype(str).map(team_name_mapping)
        else:
            print("Warning: No team name mapping available")
            sos_summary['Team_Name'] = sos_summary['Team_Number'].astype(str)
        
        # Calculate SOS percentiles
        sos_summary['SOS_Percentile'] = sos_summary['Avg_Opponent_Power'].rank(pct=True) * 100
        
        # Add SOS ranking (1 = hardest schedule)
        sos_summary = sos_summary.sort_values('Total_Opponent_Power', ascending=False)
        sos_summary['SOS_Rank'] = range(1, len(sos_summary) + 1)
        
        # Calculate additional metrics
        mean_sos = sos_summary['Avg_Opponent_Power'].mean()
        std_sos = sos_summary['Avg_Opponent_Power'].std()
        
        # Add schedule difficulty classification
        sos_summary['Schedule_Difficulty'] = 'Average'
        sos_summary.loc[sos_summary['Avg_Opponent_Power'] > (mean_sos + std_sos), 'Schedule_Difficulty'] = 'Hard'
        sos_summary.loc[sos_summary['Avg_Opponent_Power'] < (mean_sos - std_sos), 'Schedule_Difficulty'] = 'Easy'
        
        # Ensure we have exactly 12 records (one per team)
        if len(sos_summary) != 12:
            print(f"Warning: Expected 12 teams, found {len(sos_summary)}")
        
        # Select final columns
        final_columns = ['Team_Number', 'Team_Name', 'Total_Opponent_Power', 
                        'Avg_Opponent_Power', 'SOS_Percentile', 'SOS_Rank', 
                        'Games_Remaining', 'Schedule_Difficulty']
        
        sos_final = sos_summary[final_columns].copy()
        
        # Cast Team_Number as string for DynamoDB storage
        sos_final['Team_Number'] = sos_final['Team_Number'].astype(str)
        
        # Display results
        print("\n" + "="*60)
        print("REMAINING STRENGTH OF SCHEDULE ANALYSIS")
        print("="*60)
        print(f"Teams analyzed: {len(sos_final)}")
        print(f"Average SOS: {mean_sos:.2f}")
        print(f"SOS Standard Deviation: {std_sos:.2f}")
        
        print(f"\nRemaining Schedule Rankings (Hardest to Easiest):")
        print("-" * 80)
        display_cols = ['SOS_Rank', 'Team_Name', 'Games_Remaining', 'Total_Opponent_Power', 
                       'Avg_Opponent_Power', 'SOS_Percentile', 'Schedule_Difficulty']
        print(sos_final[display_cols].to_string(index=False))
        
        return sos_final
        
    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        additional_info = f'Error occurred at line {line_number}'
        logging.error(f'{filename}: {error_message} - {additional_info}')
        print(f"Error in SOS calculation: {error_message}")
        raise

def main():
    try:
        # Calculate remaining SOS
        sos_df = get_remaining_sos()
        
        if not sos_df.empty:
            # Save to database
            storage.write_live_data('remaining_sos', sos_df)
            print(f"\nRemaining SOS analysis complete - {len(sos_df)} teams saved to DynamoDB")
        else:
            print("No SOS data to save")
        
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


