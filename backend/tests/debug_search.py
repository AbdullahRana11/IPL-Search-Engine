
import os
import sys

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from backend.src.search_engine import SearchEngine

def debug_search():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    print(f"Initializing SearchEngine with base_dir: {base_dir}")
    se = SearchEngine(output_dir=base_dir)
    
    queries = [
        "top run scorer",
        "top wicket taker"
    ]
    
    for query in queries:
        print(f"\nTesting search_multi with query: '{query}'")
        results = se.search_multi(query)
        print(f"  Found {len(results)} results")
        if results:
            print(f"  Top result: {results[0].get('title', 'Unknown')}")

if __name__ == "__main__":
    debug_search()
