import requests
import json

def test_query(q):
    print(f"\nTesting Query: {q}")
    try:
        r = requests.get(f"http://localhost:8000/search?q={q}")
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        print(f"Found {len(results)} results")
        for res in results[:5]:
            print(f"- {res.get('title')} ({res.get('type')}) - Score: {res.get('score'):.2f}")
    except Exception as e:
        print(f"Error testing query '{q}': {e}")

if __name__ == "__main__":
    queries = [
        "virat kohli, dhoni and jadeja runs 201",
        "Ronaldo, Messi, and Neymar",
        "CSK vs MI 1st over 2015"
    ]
    for q in queries:
        test_query(q)
