import sys
import os
# Add current directory to sys.path to find src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.search_engine import SearchEngine

se = SearchEngine(output_dir='.')
query = "virat kohli, dhoni and jadeja runs 201"
print(f"Query: {query}")

# Step 1: Preprocess
q = se._preprocess_query(query)
print(f"Preprocessed: {q}")

# Step 2: Split
normalized_query = q.lower().replace(' and ', ',')
parts = [p.strip() for p in normalized_query.split(',') if p.strip()]
print(f"Parts: {parts}")

all_results_map = {}
for part in parts:
    print(f"\nProcessing part: '{part}'")
    if ' ' in part:
        words = part.split()
        res_ids = None
        for word in words:
            w_id = se._get_word_id(word)
            print(f"  Word: '{word}', ID: {w_id}")
            if w_id is None:
                res_ids = set()
                break
            word_docs = set(se.barrel_manager.get_documents_for_word(w_id))
            print(f"  Docs found: {len(word_docs)}")
            if res_ids is None:
                res_ids = word_docs
            else:
                res_ids.intersection_update(word_docs)
        
        if res_ids:
            print(f"  Intersection size: {len(res_ids)}")
            results = se._get_doc_metadata(list(res_ids))
            for doc in results:
                all_results_map[doc['doc_id']] = doc
    else:
        w_id = se._get_word_id(part)
        print(f"  Word: '{part}', ID: {w_id}")
        if w_id is not None:
            res_ids = se.barrel_manager.get_documents_for_word(w_id)
            print(f"  Docs found: {len(res_ids)}")
            results = se._get_doc_metadata(res_ids)
            for doc in results:
                all_results_map[doc['doc_id']] = doc

print(f"\nTotal unique docs in map: {len(all_results_map)}")
if len(all_results_map) > 0:
    # Check if Ali Karimi is in there
    ali_karimi_ids = [doc_id for doc_id, doc in all_results_map.items() if 'Ali Karimi' in str(doc)]
    print(f"Ali Karimi in map? {len(ali_karimi_ids) > 0}")
    
    # Rank
    term_doc_counts = {}
    all_terms = set()
    for part in parts:
        all_terms.update(part.split())
    for term in all_terms:
        w_id = se._get_word_id(term)
        term_doc_counts[term] = len(se.barrel_manager.get_documents_for_word(w_id)) if w_id else 0
        
    ranking_query = " ".join(parts)
    ranked = se.ranker.rank_results(list(all_results_map.values()), ranking_query, len(se.metadata), term_doc_counts)
    print(f"Ranked results: {len(ranked)}")
    for res in ranked[:5]:
        print(f"- {res['title']} ({res['type']}) - Score: {res['score']}")
