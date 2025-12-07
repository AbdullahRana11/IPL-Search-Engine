
import pandas as pd

def verify_bowler_stats():
    try:
        df = pd.read_csv('Dataset/IPL/all_season_details.csv', low_memory=False)
        df_2022 = df[df['season'] == 2022]
        
        if df_2022.empty:
            print("No data found for season 2022")
            return

        # Check if bowler1_wkts is cumulative
        # Group by match_id and bowler1_name, take max of bowler1_wkts
        match_wickets = df_2022.groupby(['match_id', 'bowler1_name'])['bowler1_wkts'].max().reset_index()
        
        # Sum the wickets for each bowler
        season_wickets = match_wickets.groupby('bowler1_name')['bowler1_wkts'].sum().sort_values(ascending=False).head(5)
        
        print("Top Wicket Takers of 2022 (Calculated):")
        print(season_wickets)
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    verify_bowler_stats()
