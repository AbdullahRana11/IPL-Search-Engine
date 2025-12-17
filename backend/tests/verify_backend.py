"""
Verification Script for IPL Search Engine Backend
Tests:
1. Player stats generation
2. Ranking (Player stats > Match)
3. Autocomplete
"""

import sys
import os
import time

# Add backend directory to path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if project_root not in sys.path:
    sys.path.append(project_root)

from backend.src.search_engine import SearchEngine
from backend.src.autocomplete import Autocomplete
from backend.src.document_processor import DocumentProcessor

def verify_backend():
    print("=" * 80)
    print("VERIFYING BACKEND IMPLEMENTATION")
    print("=" * 80)
    
    # 1. Initialize Search Engine
    print("\n[1/3] Initializing Search Engine...")
    try:
        engine = SearchEngine(output_dir='.')
        print(f"✓ Search Engine initialized with {len(engine.metadata)} documents")
    except Exception as e:
        print(f"❌ Failed to initialize search engine: {e}")
        return

    # 2. Verify Player Stats Search & Ranking
    print("\n[2/3] Verifying Search Relevance & Ranking...")
    
    test_queries = [
        "Virat Kohli",
        "Kohli 2022",
        "MS Dhoni",
        "Orange Cap 2023"
    ]
    
    for query in test_queries:
        print(f"\nSearching for: '{query}'")
        results = engine.search_single(query) if ' ' not in query else engine.search_multi(query)
        
        if not results:
            print(f"❌ No results found for '{query}'")
            continue
            
        top_doc = results[0]
        print(f"  Top Result Type: {top_doc.get('type')}")
        print(f"  Top Result Title: {top_doc.get('player_name') or top_doc.get('match_name')}")
        print(f"  Top Result Score: {top_doc.get('score', 0):.2f}")
        
        # Check if top result is what we expect
        if "Kohli" in query and "2022" not in query:
            if top_doc.get('type') == 'player_career_stats':
                print("✓ Correctly ranked Career Stats first")
            else:
                print(f"⚠ Expected 'player_career_stats' but got '{top_doc.get('type')}'")
                
        if "Kohli" in query and "2022" in query:
            if top_doc.get('type') == 'player_season_stats' and top_doc.get('season') == 2022:
                print("✓ Correctly ranked Season Stats first")
            else:
                print(f"⚠ Expected 'player_season_stats' for 2022 but got '{top_doc.get('type')}'")

    # 3. Verify Autocomplete
    print("\n[3/3] Verifying Autocomplete...")
    try:
        ac = Autocomplete()
        ac.build_from_lexicon(engine.lexicon)
        
        prefix = "Koh"
        suggestions = ac.search(prefix)
        print(f"Suggestions for '{prefix}': {suggestions}")
        
        if "kohli" in [s.lower() for s in suggestions]:
            print("✓ Autocomplete found 'Kohli'")
        else:
            print("❌ Autocomplete failed to find 'Kohli'")
            
    except Exception as e:
        print(f"❌ Autocomplete verification failed: {e}")

    print("\n" + "=" * 80)
    print("VERIFICATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    verify_backend()
