
import pandas as pd

def verify_2022_stats():
    try:
        df = pd.read_csv('Dataset/IPL/all_season_details.csv', low_memory=False)
        
        # Filter for 2022 season
        # The season column seems to be float (2023.0), so we should handle that
        df_2022 = df[df['season'] == 2022]
        
        if df_2022.empty:
            print("No data found for season 2022")
            return


        # batsman1_runs is cumulative for the innings.
        # We need the max score for each batsman in each match, then sum those up.
        
        # Group by match_id and batsman1_name, take max of batsman1_runs
        match_scores = df_2022.groupby(['match_id', 'batsman1_name'])['batsman1_runs'].max().reset_index()
        
        # Now sum the scores for each batsman
        season_stats = match_scores.groupby('batsman1_name')['batsman1_runs'].sum().sort_values(ascending=False).head(5)
        
        print("Top Scorers of 2022 (Corrected):")
        print(season_stats)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_2022_stats()
