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
from datetime_utils import *
from yahoo_utils import *

# Load obfuscated strings from .env file
load_dotenv()
YAHOO_LEAGUE_ID = os.environ.get('YAHOO_LEAGUE_ID')


def get_comprehensive_stats():
    """Get comprehensive team stats including records, current stats, and all scores"""
    
    # Get Actual Records by looking up standings table on league home page
    soup = url_requests(YAHOO_LEAGUE_ID)
    table = soup.find_all('table')
    df_rec = pd.read_html(str(table))[0]
    df_rec = df_rec.rename(columns={'Team':'Team Name'})
    
    batting_list = league_stats_batting()
    pitching_list = league_stats_pitching()

    dfb = league_record_batting_df()
    dfp = league_record_pitching_df()

    # Process batting records
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
    
    for cat in batting_list:
        cat_Win = f'{cat}_Win'
        cat_Draw = f'{cat}_Draw'
        cat_Loss = f'{cat}_Loss'
        dfb[str(cat)] = list(zip(dfb[cat_Win], dfb[cat_Draw], dfb[cat_Loss]))

    # convert tuples to ints
    dfb[str(cat)] = tuple(tuple(map(int, tup)) for tup in  dfb[cat])  
    dfb.columns = dfb.columns.str.replace('[#,@,&,/,+]', '')

    # Process pitching records
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
    
    for cat in pitching_list:
        cat_Win = f'{cat}_Win'
        cat_Draw = f'{cat}_Draw'
        cat_Loss = f'{cat}_Loss'
        dfp[str(cat)] = list(zip(dfp[cat_Win], dfp[cat_Draw], dfp[cat_Loss]))

    # convert tuples to ints
    dfp[str(cat)] = tuple(tuple(map(int, tup)) for tup in  dfp[cat])  
    dfp.columns = dfp.columns.str.replace('[#,@,&,/,+]', '')     
    
    # Merge records data
    df_records = reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp, df_rec])
    df_records = df_records[['Team Name'] + batting_list + pitching_list + ['Rank', 'GB', 'Moves']]
    
    # Create ranking based on records in all stat categories
    for column in df_records:
        if column in ['Team Name','Rank','GB','Moves']:
            pass
        else:
            df_records[column+'_Rank'] = df_records[column].rank(ascending = False)
    
    # change col names to be record independent   
    keep_same = {'Team Name','Rank','GB','Moves'}
    df_records.columns = ['{}{}'.format(c, '' if c in keep_same else '_Record') for c in df_records.columns]
    df_records = df_records.dropna()
    
    return df_records


def get_current_stats(records_df):
    """Get current season stats and calculate rankings"""
    
    num_teams = league_size()
    dfb = league_stats_batting_df()
    dfp = league_stats_pitching_df()

    df_stats = reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [dfb, dfp])

    # Create rankings for current stats
    for column in df_stats:
        if column == 'Team Name':
            pass
        # ERA, WHIP, and HRA need to be ranked descending
        elif column in Low_Categories:
            df_stats[column+'_Rank'] = df_stats[column].rank(ascending = True)
        # All others ranked ascending
        else:
            df_stats[column+'_Rank'] = df_stats[column].rank(ascending = False)
    
    # Change col names to be stats independent
    keep_same = {'Team Name'}
    df_stats.columns = ['{}{}'.format(c, '' if c in keep_same else '_Stats') for c in df_stats.columns]
    
    # Merge with records
    df_merged = reduce(lambda x,y: pd.merge(x,y, on='Team Name', how='outer'), [df_stats, records_df])
    
    # Calculate power scores
    columns_to_calculate = [col for col in df_merged.columns if '_Rank_Stats' in col]
    df_merged['Stats_Power_Score'] = df_merged[columns_to_calculate].sum(axis=1) / num_teams
    df_merged['Stats_Power_Rank'] = df_merged['Stats_Power_Score'].rank(ascending = True)
    
    # Handle playoff clinching asterisks
    try:        
        df_merged['Rank'] = df_merged['Rank'].str.replace('*','').astype(int)
    except AttributeError:
        print("No one has clinched playoffs yet")
    
    # Calculate variation
    df_merged['Variation'] = df_merged['Stats_Power_Rank'] - df_merged['Rank'] 
    
    # Calculate batting and pitching ranks
    columns_to_calculate = [col for col in df_merged.columns if col in Batting_Rank_Stats]
    df_merged['batter_rank'] = df_merged[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    columns_to_calculate = [col for col in df_merged.columns if col in Pitching_Rank_Stats]
    df_merged['pitcher_rank'] = df_merged[columns_to_calculate].sum(axis=1) / (num_teams / 2)

    df_merged = df_merged.rename(columns={'Team Name': 'Team'})
    df_merged_teams = build_team_numbers(df_merged)  
    
    return df_merged_teams


def calculate_normalized_scores(df):
    """Calculate normalized scores for all stats"""
    
    # Parse through columns and figure out which ones are low-based vs high-based
    low_columns_to_analyze = []
    high_columns_to_analyze = []

    for column in df.columns:
        if '_Stats' in column and '_Rank_Stats' not in column:
            if column in Low_Categories_Stats:
                low_columns_to_analyze.append(column)
            else:
                high_columns_to_analyze.append(column)

    # Calculate scores for high-based categories (higher is better)
    for column in high_columns_to_analyze:
        min_score = 0
        scaler = MinMaxScaler(feature_range=(min_score, 100))
        df[column + '_Score'] = scaler.fit_transform(df[column].values.reshape(-1, 1)).round(2)    
    
    # Calculate scores for low-based categories (lower is better)
    for column in low_columns_to_analyze:
        min_value = df[column].min()
        max_value = df[column].max()
        scaled_values = 100 - ((df[column] - min_value) / (max_value - min_value)) * 100
        df[column + '_Score'] = scaled_values.round(2)

    # Get the list of all score columns
    score_columns = [column + '_Score' for column in high_columns_to_analyze + low_columns_to_analyze]

    # Sum all scores
    df['Total_Score_Sum'] = df[score_columns].sum(axis=1).round(2)
    df['Total_Score_Rank'] = df['Total_Score_Sum'].rank(ascending=False)
    df['Score_Variation'] = (df['Total_Score_Rank'] - df['Rank']).round(2)

    return df


def export_comprehensive_stats_to_csv():
    """Main function to export comprehensive team stats to CSV"""
    
    try:
        print("Gathering comprehensive team statistics...")
        
        # Get records data
        records_df = get_comprehensive_stats()
        
        # Get current stats and merge with records
        stats_df = get_current_stats(records_df)
        
        # Calculate normalized scores
        final_df = calculate_normalized_scores(stats_df)
        
        # Organize columns for export
        # Start with team identification
        base_columns = ['Team', 'Team_Number', 'Rank', 'GB', 'Moves']
        
        # Get all stat categories (both batting and pitching)
        batting_list = league_stats_batting()
        pitching_list = league_stats_pitching()
        all_categories = batting_list + pitching_list
        
        # Build comprehensive column list
        export_columns = base_columns.copy()
        
        # Add cumulative stats (current season totals)
        for cat in all_categories:
            if f'{cat}_Stats' in final_df.columns:
                export_columns.append(f'{cat}_Stats')
        
        # Add all score columns
        for cat in all_categories:
            if f'{cat}_Stats_Score' in final_df.columns:
                export_columns.append(f'{cat}_Stats_Score')
        
        # Add summary columns
        summary_columns = ['Stats_Power_Score', 'Stats_Power_Rank', 'Variation', 
                          'batter_rank', 'pitcher_rank', 'Total_Score_Sum', 
                          'Total_Score_Rank', 'Score_Variation']
        
        for col in summary_columns:
            if col in final_df.columns:
                export_columns.append(col)
        
        # Filter dataframe to only include available columns
        available_columns = [col for col in export_columns if col in final_df.columns]
        export_df = final_df[available_columns]
        
        # Sort by total score rank
        if 'Total_Score_Rank' in export_df.columns:
            export_df = export_df.sort_values('Total_Score_Rank')
        elif 'Stats_Power_Rank' in export_df.columns:
            export_df = export_df.sort_values('Stats_Power_Rank')
        
        # Export to CSV
        csv_filename = 'comprehensive_team_stats.csv'
        export_df.to_csv(csv_filename, index=False)
        
        print(f"✅ Exported comprehensive team statistics to {csv_filename}")
        print(f"📊 Exported {len(export_df)} teams with {len(available_columns)} columns")
        print(f"📈 Columns include: Team info, cumulative stats, individual scores, and total score sum")
        
        # Display summary
        print("\n📋 Export Summary:")
        print(f"   - Teams: {len(export_df)}")
        print(f"   - Stat Categories: {len(all_categories)}")
        print(f"   - Total Columns: {len(available_columns)}")
        
        return export_df
        
    except Exception as e:
        filename = os.path.basename(__file__)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        error_message = f"Error occurred in {filename} at line {line_number}: {str(e)}"
        print(f"❌ Error: {error_message}")
        send_failure_email(error_message, filename)
        return None


def main():
    """Main execution function"""
    export_comprehensive_stats_to_csv()


if __name__ == '__main__':
    main()