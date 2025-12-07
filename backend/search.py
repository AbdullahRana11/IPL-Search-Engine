"""
Search CLI
Provides command-line interface for searching the IPL dataset.
"""

import sys
sys.path.append('backend')

from src.search_engine import SearchEngine
import time


def main():
    """Main search CLI function."""
    # Get query from command line
    if len(sys.argv) < 2:
        print("Usage: python search.py \"your search query\"")
        sys.exit(1)
    
    query = sys.argv[1]
    
    # Initialize search engine
    engine = SearchEngine()
    
    print(f"\nSearching for: '{query}'")
    print("-" * 50)
    
    start_time = time.time()
    
    # Determine search type
    if ' and ' in query.lower():
        results = engine.search_combined(query)
    elif ' ' in query:
        results = engine.search_multi(query)
    else:
        results = engine.search_single(query)
        
    elapsed_time = time.time() - start_time
    
    print(f"Found {len(results)} results in {elapsed_time:.4f} seconds")
    print("-" * 50)
    
    # Display top 10 results
    for i, res in enumerate(results[:10]):
        if res.get('type') == 'season_stats':
            print(f"{i+1}. {res.get('description', 'Season Stats')}")
        elif res.get('type') == 'football':
            print(f"{i+1}. [Football] {res['player_name']}")
            print(f"   Images: {len(res.get('image_paths', []))} found")
        else:
            print(f"{i+1}. {res.get('match_name', 'N/A')} ({res.get('season', 'N/A')})")
            print(f"   {res.get('home_team', 'N/A')} vs {res.get('away_team', 'N/A')}")
            print(f"   Over: {res.get('over', 'N/A')}, Ball: {res.get('ball', 'N/A')}")
        print()
        
    if len(results) > 10:
        print(f"... and {len(results) - 10} more.")

if __name__ == "__main__":
    main()
