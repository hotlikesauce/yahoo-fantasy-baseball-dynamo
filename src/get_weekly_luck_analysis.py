import pandas as pd
import numpy as np
import os, logging, traceback, sys
from dotenv import load_dotenv

# Local Modules
from email_utils import send_failure_email
from storage_manager import DynamoStorageManager
from datetime_utils import *

# Load obfuscated strings from .env file
load_dotenv()

storage = DynamoStorageManager(region='us-west-2')

logging.basicConfig(filename='error.log', level=logging.ERROR)

def get_weekly_luck_analysis(weeks_to_analyze=None):
    """
    Analyze team luck by comparing expected wins (Team_Expected_Wins) to actual wins for multiple weeks
    
    Args:
        weeks_to_analyze: List of weeks to analyze (if None, analyzes weeks 1-15)
    
    Returns:
        DataFrame with luck analysis for all teams across all specified weeks
    """
    try:
        # Determine weeks to analyze
        if weeks_to_analyze is None:
            weeks_to_analyze = list(range(1, 16))  # Weeks 1-15
        
        print(f"Analyzing luck for weeks: {weeks_to_analyze}")
        
        # Get coefficient data (contains Team_Expected_Wins)
        coefficient_df = storage.get_historical_data('coefficient')
        if coefficient_df.empty:
            print("Error: No coefficient data found")
            return pd.DataFrame()
        
        print(f"Coefficient data shape: {coefficient_df.shape}")
        print(f"Coefficient columns: {list(coefficient_df.columns)}")
        print("Sample coefficient data:")
        print(coefficient_df.head())
        
        # Filter coefficient data for specified weeks
        if 'Week' in coefficient_df.columns:
            coefficient_filtered = coefficient_df[coefficient_df['Week'].isin(weeks_to_analyze)].copy()
        else:
            print("Error: 'Week' column not found in coefficient data")
            return pd.DataFrame()
        
        if coefficient_filtered.empty:
            print(f"No coefficient data found for weeks {weeks_to_analyze}")
            return pd.DataFrame()
        
        print(f"Coefficient data for weeks {weeks_to_analyze}: {len(coefficient_filtered)} records")
        
        # Get weekly results data (contains actual wins)
        weekly_results_df = storage.get_historical_data('weekly_results')
        if weekly_results_df.empty:
            print("Error: No weekly_results data found")
            return pd.DataFrame()
        
        print(f"Weekly results data shape: {weekly_results_df.shape}")
        print(f"Weekly results columns: {list(weekly_results_df.columns)}")
        print("Sample weekly results data:")
        print(weekly_results_df.head())
        
        # Filter weekly results for specified weeks
        if 'Week' in weekly_results_df.columns:
            results_filtered = weekly_results_df[weekly_results_df['Week'].isin(weeks_to_analyze)].copy()
        else:
            print("Error: 'Week' column not found in weekly_results data")
            return pd.DataFrame()
        
        if results_filtered.empty:
            print(f"No weekly results data found for weeks {weeks_to_analyze}")
            return pd.DataFrame()
        
        print(f"Weekly results data for weeks {weeks_to_analyze}: {len(results_filtered)} records")
        
        # Get team_dict for proper team names
        team_dict_df = storage.get_live_data('team_dict')
        if not team_dict_df.empty:
            team_dict_df['Team_Number'] = team_dict_df['Team_Number'].astype(str)
            team_name_mapping = dict(zip(team_dict_df['Team_Number'], team_dict_df['Team']))
            print(f"Team name mapping loaded: {len(team_name_mapping)} teams")
        else:
            print("Warning: team_dict collection is empty")
            team_name_mapping = {}
        
        # Ensure Team_Number is string in both datasets for merging
        if 'Team_Number' in coefficient_filtered.columns:
            coefficient_filtered['Team_Number'] = coefficient_filtered['Team_Number'].astype(str)
        
        if 'Team_Number' in results_filtered.columns:
            results_filtered['Team_Number'] = results_filtered['Team_Number'].astype(str)
        
        # Debug: Show what we have for merging
        print(f"\nCoefficient data teams: {sorted(coefficient_filtered['Team_Number'].unique()) if 'Team_Number' in coefficient_filtered.columns else 'No Team_Number column'}")
        print(f"Weekly results teams: {sorted(results_filtered['Team_Number'].unique()) if 'Team_Number' in results_filtered.columns else 'No Team_Number column'}")
        
        # Simple merge: coefficient data with weekly results to get all needed data
        luck_df = coefficient_filtered.merge(
            results_filtered[['Team_Number', 'Week', 'Score']], 
            on=['Team_Number', 'Week'], 
            how='inner'
        )
        
        print(f"After merge: {luck_df.shape}")
        print(f"Columns after merge: {list(luck_df.columns)}")
        print("Sample merged data:")
        print(luck_df.head())
        
        if luck_df.empty:
            print("Error: No matches found between coefficient and weekly results data")
            print("Check that Team_Number and Week fields match between the two collections")
            return pd.DataFrame()
        
        print(f"Merged data shape: {luck_df.shape}")
        print("Sample merged data:")
        print(luck_df.head())
        
        # Calculate luck metrics
        if 'Team_Expected_Wins' not in luck_df.columns:
            print("Error: 'Team_Expected_Wins' column not found in coefficient data")
            print(f"Available columns: {list(luck_df.columns)}")
            return pd.DataFrame()
        
        if 'Score' not in luck_df.columns:
            print("Error: 'Score' column not found in weekly results data")
            print(f"Available columns: {list(luck_df.columns)}")
            return pd.DataFrame()
        
        # Calculate luck difference (positive = unlucky, negative = lucky)
        luck_df['Actual_Wins'] = luck_df['Score']
        luck_df['Expected_Wins'] = luck_df['Team_Expected_Wins']
        luck_df['Luck_Difference'] = luck_df['Expected_Wins'] - luck_df['Actual_Wins']
        
        # Check if coefficient table has opponent expected wins column
        if 'Opponent_Expected_Wins' in luck_df.columns:
            # Calculate expected win differential 
            luck_df['Expected_Win_Diff'] = luck_df['Expected_Wins'] - luck_df['Opponent_Expected_Wins']
            
            # Determine if team won or lost based on Score
            luck_df['Team_Won'] = luck_df['Actual_Wins'] > 6
            luck_df['Team_Lost'] = luck_df['Actual_Wins'] < 6
            
            # Add team names first so we can use them in debug output
            luck_df['Team_Name'] = luck_df['Team_Number'].map(team_name_mapping)
            if 'Opponent_Team_Number' in luck_df.columns:
                luck_df['Opponent_Name'] = luck_df['Opponent_Team_Number'].map(team_name_mapping)
            
            # Debug: Show sample matchup data to understand the calculations
            print(f"\nDEBUG: Sample matchup analysis:")
            sample_data = luck_df[['Team_Name', 'Week', 'Expected_Wins', 'Actual_Wins', 
                                 'Opponent_Expected_Wins', 'Expected_Win_Diff', 'Team_Won', 'Team_Lost']].head(10)
            print(sample_data.to_string(index=False))
            
            # Identify extreme outliers using simple Score logic
            # Case 1: Team had lower expected wins but won (Score > 6)
            luck_df['Underdog_Victory'] = (
                (luck_df['Expected_Win_Diff'] < 0) & 
                (luck_df['Team_Won'])
            )
            
            # Case 2: Team had higher expected wins but lost (Score < 6)
            luck_df['Favorite_Loss'] = (
                (luck_df['Expected_Win_Diff'] > 0) & 
                (luck_df['Team_Lost'])
            )
            
            # Debug: Show counts of different scenarios
            print(f"\nDEBUG: Matchup scenario counts:")
            print(f"Total matchups analyzed: {len(luck_df)}")
            print(f"Teams with lower expected wins (Expected_Win_Diff < 0): {sum(luck_df['Expected_Win_Diff'] < 0)}")
            print(f"Teams that won their matchup (Score > 6): {sum(luck_df['Team_Won'])}")
            print(f"Underdog victories (lower expected but won): {sum(luck_df['Underdog_Victory'])}")
            print(f"Teams with higher expected wins (Expected_Win_Diff > 0): {sum(luck_df['Expected_Win_Diff'] > 0)}")
            print(f"Teams that lost their matchup (Score < 6): {sum(luck_df['Team_Lost'])}")
            print(f"Favorite losses (higher expected but lost): {sum(luck_df['Favorite_Loss'])}")
            
            # Find specific case: Team expected to win but scored < 6 (lost badly)
            bad_losses = luck_df[
                (luck_df['Expected_Wins'] > luck_df['Opponent_Expected_Wins']) & 
                (luck_df['Actual_Wins'] < 6)
            ]
            
            print(f"\nDEBUG: Teams with higher expected wins but scored < 6 (bad losses):")
            print(f"Found {len(bad_losses)} instances")
            
            if not bad_losses.empty:
                bad_loss_cols = ['Team_Name', 'Week', 'Expected_Wins', 'Opponent_Expected_Wins', 
                               'Actual_Wins', 'Expected_Win_Diff']
                print(bad_losses[bad_loss_cols].to_string(index=False))
                
                # Show the worst cases (biggest expected win differential but still lost badly)
                worst_losses = bad_losses.nlargest(5, 'Expected_Win_Diff')
                print(f"\nWorst losses (biggest expected advantage but scored < 6):")
                for _, row in worst_losses.iterrows():
                    team_name = row['Team_Name']
                    week = row['Week']
                    team_expected = row['Expected_Wins']
                    opp_expected = row['Opponent_Expected_Wins']
                    team_actual = row['Actual_Wins']
                    opp_actual = 12 - team_actual  # Calculate opponent score
                    expected_diff = row['Expected_Win_Diff']
                    
                    # Get opponent name by finding the team with matching expected wins for this week
                    opp_name = "Unknown Opponent"
                    if not pd.isna(opp_expected):
                        # Find the opponent team by matching expected wins for this week
                        week_coeff = coefficient_filtered[(coefficient_filtered['Week'] == week) & 
                                                        (coefficient_filtered['Team_Number'] != row['Team_Number'])]
                        
                        matching_opponent = week_coeff[abs(week_coeff['Team_Expected_Wins'] - opp_expected) < 0.01]
                        
                        if not matching_opponent.empty:
                            opponent_team_num = matching_opponent.iloc[0]['Team_Number']
                            opp_name = team_name_mapping.get(opponent_team_num, f"Team {opponent_team_num}")
                    
                    print(f"Week {week}: {team_name} (Expected: {team_expected:.1f}) vs {opp_name} (Expected: {opp_expected:.1f})")
                    print(f"   Expected to win by {expected_diff:.1f}, but lost {team_actual}-{opp_actual}")
                    print()
            
            # Show some specific examples
            underdog_wins = luck_df[luck_df['Underdog_Victory'] == True]
            if not underdog_wins.empty:
                print(f"\nDEBUG: Sample underdog victories:")
                sample_underdogs = underdog_wins[['Team_Name', 'Week', 'Expected_Wins', 'Actual_Wins',
                                                'Opponent_Expected_Wins']].head(5)
                print(sample_underdogs.to_string(index=False))
            else:
                print(f"\nDEBUG: No underdog victories found. Let's check some close cases:")
                # Show cases where team had lower expected wins
                potential_underdogs = luck_df[luck_df['Expected_Win_Diff'] < 0].head(5)
                if not potential_underdogs.empty:
                    debug_cols = ['Team_Name', 'Week', 'Expected_Wins', 'Actual_Wins',
                                'Opponent_Expected_Wins', 'Expected_Win_Diff']
                    print(potential_underdogs[debug_cols].to_string(index=False))
            
            # Calculate magnitude of upset (how big the expected differential was)
            luck_df['Upset_Magnitude'] = abs(luck_df['Expected_Win_Diff'])
            
            # Extreme outliers: significant expected differential but opposite result
            luck_df['Extreme_Outlier'] = (
                ((luck_df['Underdog_Victory']) | (luck_df['Favorite_Loss'])) &
                (luck_df['Upset_Magnitude'] >= 2.0)  # At least 2 win difference in expectations
            )
        else:
            print("Warning: Opponent expected wins not available - outlier analysis limited")
            luck_df['Opponent_Expected_Wins'] = None
            luck_df['Expected_Win_Diff'] = None
            luck_df['Underdog_Victory'] = False
            luck_df['Favorite_Loss'] = False
            luck_df['Upset_Magnitude'] = 0
            luck_df['Extreme_Outlier'] = False
        
        # Add team names using team_dict (already loaded as team_name_mapping)
        luck_df['Team_Name'] = luck_df['Team_Number'].map(team_name_mapping)
        
        # Get opponent team names if we have opponent data in coefficient table
        if 'Opponent_Team_Number' in luck_df.columns:
            luck_df['Opponent_Name'] = luck_df['Opponent_Team_Number'].map(team_name_mapping)
        elif 'Opponent_Expected_Wins' in luck_df.columns:
            # If coefficient table has opponent expected wins, we need to get opponent team numbers
            # We'll need to merge with team_dict to find which team has the matching expected wins
            print("Getting opponent team names from coefficient data...")
            
            # Create a mapping of expected wins to team numbers for each week
            opponent_mapping = {}
            for _, row in luck_df.iterrows():
                week = row['Week']
                opp_expected = row['Opponent_Expected_Wins']
                team_num = row['Team_Number']
                
                # Find the team that has this expected wins value for this week (excluding current team)
                week_coeff = coefficient_filtered[(coefficient_filtered['Week'] == week) & 
                                                (coefficient_filtered['Team_Number'] != team_num)]
                
                matching_team = week_coeff[abs(week_coeff['Team_Expected_Wins'] - opp_expected) < 0.01]
                
                if not matching_team.empty:
                    opponent_team_num = matching_team.iloc[0]['Team_Number']
                    opponent_mapping[f"{team_num}_{week}"] = opponent_team_num
            
            # Apply opponent team numbers and names
            luck_df['Opponent_Team_Number'] = luck_df.apply(
                lambda row: opponent_mapping.get(f"{row['Team_Number']}_{row['Week']}", None), axis=1
            )
            luck_df['Opponent_Name'] = luck_df['Opponent_Team_Number'].map(team_name_mapping)
        
        # Create comprehensive results for all weeks
        all_weeks_results = []
        
        for week in weeks_to_analyze:
            week_data = luck_df[luck_df['Week'] == week].copy()
            if not week_data.empty:
                # Sort by luck difference for this week (most unlucky first)
                week_data = week_data.sort_values('Luck_Difference', ascending=False)
                week_data['Luck_Rank'] = range(1, len(week_data) + 1)
                
                # Add luck categories
                week_data['Luck_Category'] = 'Average'
                week_data.loc[week_data['Luck_Difference'] > 0.5, 'Luck_Category'] = 'Unlucky'
                week_data.loc[week_data['Luck_Difference'] < -0.5, 'Luck_Category'] = 'Lucky'
                
                all_weeks_results.append(week_data)
        
        if not all_weeks_results:
            print("No data found for any of the specified weeks")
            return pd.DataFrame()
        
        # Combine all weeks
        luck_final = pd.concat(all_weeks_results, ignore_index=True)
        
        # Select final columns including opponent and outlier analysis
        final_columns = ['Team_Number', 'Team_Name', 'Week', 'Expected_Wins', 'Actual_Wins', 
                        'Luck_Difference', 'Luck_Rank', 'Luck_Category']
        
        # Add opponent and outlier columns if available
        if 'Opponent_Team_Number' in luck_final.columns:
            final_columns.extend(['Opponent_Team_Number', 'Opponent_Name', 'Opponent_Expected_Wins', 
                                'Expected_Win_Diff', 'Underdog_Victory', 'Favorite_Loss', 
                                'Upset_Magnitude', 'Extreme_Outlier'])
        
        # Only include columns that actually exist in the dataframe
        available_columns = [col for col in final_columns if col in luck_final.columns]
        luck_final = luck_final[available_columns].copy()
        
        # Display results
        print(f"\n" + "="*60)
        print(f"WEEKLY LUCK ANALYSIS - WEEKS {min(weeks_to_analyze)}-{max(weeks_to_analyze)}")
        print("="*60)
        print(f"Total records: {len(luck_final)}")
        print(f"Weeks analyzed: {sorted(luck_final['Week'].unique())}")
        
        # Show summary by week
        print(f"\nWeek-by-week summary:")
        print("-" * 80)
        for week in sorted(weeks_to_analyze):
            week_data = luck_final[luck_final['Week'] == week]
            if not week_data.empty:
                most_unlucky = week_data.loc[week_data['Luck_Rank'] == 1]
                most_lucky = week_data.loc[week_data['Luck_Rank'] == week_data['Luck_Rank'].max()]
                
                if not most_unlucky.empty and not most_lucky.empty:
                    unlucky_team = most_unlucky.iloc[0]
                    lucky_team = most_lucky.iloc[0]
                    print(f"Week {week}: Most Unlucky: {unlucky_team['Team_Name']} ({unlucky_team['Luck_Difference']:+.1f}) | Most Lucky: {lucky_team['Team_Name']} ({lucky_team['Luck_Difference']:+.1f})")
        
        # Show overall luck leaders
        print(f"\nOverall luck summary across all weeks:")
        print("-" * 80)
        team_totals = luck_final.groupby(['Team_Number', 'Team_Name']).agg({
            'Luck_Difference': ['sum', 'mean', 'count'],
            'Expected_Wins': 'sum',
            'Actual_Wins': 'sum'
        }).round(2)
        
        # Flatten column names
        team_totals.columns = ['Total_Luck_Diff', 'Avg_Luck_Diff', 'Weeks_Played', 'Total_Expected', 'Total_Actual']
        team_totals = team_totals.reset_index()
        team_totals = team_totals.sort_values('Total_Luck_Diff', ascending=False)
        
        print("Overall Luck Rankings (Most Unlucky to Most Lucky):")
        display_cols = ['Team_Name', 'Weeks_Played', 'Total_Expected', 'Total_Actual', 'Total_Luck_Diff', 'Avg_Luck_Diff']
        print(team_totals[display_cols].to_string(index=False))
        
        # Show extreme outlier matchups if opponent data is available
        if 'Extreme_Outlier' in luck_final.columns:
            extreme_outliers = luck_final[luck_final['Extreme_Outlier'] == True]
            
            if not extreme_outliers.empty:
                print(f"\n" + "="*80)
                print("EXTREME OUTLIER MATCHUPS")
                print("="*80)
                print(f"Found {len(extreme_outliers)} extreme outlier matchups:")
                print("(Cases where expected outcome was significantly different from actual result)")
                print()
                
                for _, row in extreme_outliers.iterrows():
                    team_name = row['Team_Name']
                    opp_name = row.get('Opponent_Name', f"Team {row.get('Opponent_Team_Number', 'Unknown')}")
                    week = row['Week']
                    
                    team_expected = row['Expected_Wins']
                    team_actual = row['Actual_Wins']
                    opp_expected = row['Opponent_Expected_Wins']
                    opp_actual = 12 - team_actual  # Calculate opponent score
                    
                    expected_diff = row['Expected_Win_Diff']
                    actual_diff = team_actual - opp_actual  # Calculate actual differential
                    upset_magnitude = row['Upset_Magnitude']
                    
                    if row.get('Underdog_Victory', False):
                        print(f"🔥 UNDERDOG VICTORY - Week {week}")
                        print(f"   {team_name} (Expected: {team_expected:.1f}) BEAT {opp_name} (Expected: {opp_expected:.1f})")
                        print(f"   Final Score: {team_name} {team_actual} - {opp_actual} {opp_name}")
                        print(f"   Expected Differential: {expected_diff:+.1f} → Actual Differential: {actual_diff:+.1f}")
                        print(f"   Upset Magnitude: {upset_magnitude:.1f}")
                        print()
                    
                    elif row.get('Favorite_Loss', False):
                        print(f"💥 FAVORITE UPSET - Week {week}")
                        print(f"   {team_name} (Expected: {team_expected:.1f}) LOST TO {opp_name} (Expected: {opp_expected:.1f})")
                        print(f"   Final Score: {team_name} {team_actual} - {opp_actual} {opp_name}")
                        print(f"   Expected Differential: {expected_diff:+.1f} → Actual Differential: {actual_diff:+.1f}")
                        print(f"   Upset Magnitude: {upset_magnitude:.1f}")
                        print()
            
            # Show summary of underdog victories and favorite losses
            underdog_victories = luck_final[luck_final.get('Underdog_Victory', False) == True]
            favorite_losses = luck_final[luck_final.get('Favorite_Loss', False) == True]
            
            print(f"Summary:")
            print(f"- Underdog Victories: {len(underdog_victories)}")
            print(f"- Favorite Upsets: {len(favorite_losses)}")
            print(f"- Extreme Outliers (≥2.0 expected diff): {len(extreme_outliers)}")
        
        return luck_final
        
    except Exception as e:
        filename = os.path.basename(__file__)
        line_number = traceback.extract_tb(sys.exc_info()[2])[-1][1]
        error_message = str(e)
        additional_info = f'Error occurred at line {line_number}'
        logging.error(f'{filename}: {error_message} - {additional_info}')
        print(f"Error in luck analysis: {error_message}")
        raise

def determine_weeks_to_analyze():
    """
    Determine which weeks need to be analyzed based on existing data and current week
    
    Returns:
        List of weeks that need to be analyzed
    """
    try:
        # Get current week
        current_week = set_this_week()
        print(f"Current week: {current_week}")
        
        # The latest week we can analyze is current_week - 1 (previous completed week)
        max_analyzable_week = current_week - 1
        print(f"Maximum analyzable week: {max_analyzable_week}")
        
        if max_analyzable_week < 1:
            print("No completed weeks to analyze yet")
            return []
        
        # Get existing data from weekly_luck_analysis table
        existing_data = storage.get_live_data('weekly_luck_analysis')
        
        if existing_data.empty:
            # No existing data - analyze all weeks from 1 to max_analyzable_week
            weeks_to_analyze = list(range(1, max_analyzable_week + 1))
            print(f"No existing data found. Analyzing all weeks: {weeks_to_analyze}")
            return weeks_to_analyze
        
        # Get weeks that already have data
        existing_weeks = set(existing_data['Week'].unique())
        print(f"Existing weeks in database: {sorted(existing_weeks)}")
        
        # Determine all weeks that should have data (1 to max_analyzable_week)
        all_possible_weeks = set(range(1, max_analyzable_week + 1))
        
        # Find missing weeks
        missing_weeks = all_possible_weeks - existing_weeks
        
        if not missing_weeks:
            print("All possible weeks already analyzed. No new weeks to process.")
            return []
        
        weeks_to_analyze = sorted(list(missing_weeks))
        print(f"Missing weeks to analyze: {weeks_to_analyze}")
        
        return weeks_to_analyze
        
    except Exception as e:
        print(f"Error determining weeks to analyze: {e}")
        # Fallback to analyzing previous week only
        current_week = set_this_week()
        return [current_week - 1] if current_week > 1 else []

def main():
    try:
        # Determine which weeks need to be analyzed
        storage.clear_collection('weekly_luck_analysis')
        weeks_to_analyze = determine_weeks_to_analyze()
        
        if not weeks_to_analyze:
            print("No weeks need to be analyzed at this time.")
            return
        
        print(f"Proceeding to analyze weeks: {weeks_to_analyze}")
        
        luck_df = get_weekly_luck_analysis(weeks_to_analyze)
        
        if not luck_df.empty:
            # Clear existing data and save new data
            storage.write_live_data('weekly_luck_analysis', luck_df)
            print(f"\nWeekly luck analysis complete - {len(luck_df)} records saved to DynamoDB")
            
            # Show what weeks were added
            weeks_added = sorted(luck_df['Week'].unique())
            print(f"Weeks added: {weeks_added}")
        else:
            print("No luck analysis data to save")
        
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