"""
Ranking Module
Handles scoring and ranking of search results.
"""

import math
from collections import Counter

class Ranker:
    """
    Ranker class for scoring search results.
    Uses a combination of TF-IDF, field weights, and document type boosting.
    """
    
    def __init__(self):
        # Weights for different fields
        self.field_weights = {
            'player_name': 3.0,
            'team': 2.0,
            'season': 1.5,
            'description': 1.0,
            'raw_text': 0.8
        }
        
        # Boost factors for document types
        self.type_boosts = {
            'player_career_stats': 5.0,  # Highest priority for "Kohli stats"
            'player_season_stats': 3.0,  # High priority for "Kohli 2022"
            'season_stats': 2.0,         # Medium priority for "Orange Cap 2023"
            'football': 5.0,             # Reduced from 20.0 to avoid overshadowing name matches
            'match': 0.1                 # Even lower priority for raw match data
        }
    
    def calculate_tf_idf(self, term_freq, doc_len, doc_count, total_docs):
        """
        Calculate TF-IDF score.
        
        Args:
            term_freq: Frequency of term in document
            doc_len: Length of document (number of tokens)
            doc_count: Number of documents containing the term
            total_docs: Total number of documents in index
            
        Returns:
            TF-IDF score
        """
        # TF: Log normalization
        tf = 1 + math.log(term_freq) if term_freq > 0 else 0
        
        # IDF: Inverse Document Frequency
        idf = math.log(1 + (total_docs / (doc_count + 1)))
        
        return tf * idf

    def score_document(self, doc_metadata, query_terms, doc_term_freqs, total_docs, term_doc_counts):
        """
        Score a single document against the query using additive scoring.
        """
        score = 0.0
        
        # 1. TF-IDF Score (Base relevance)
        tfidf_sum = 0
        for term in query_terms:
            tf = doc_term_freqs.get(term, 0)
            if tf > 0:
                doc_count = term_doc_counts.get(term, 0)
                tfidf_sum += self.calculate_tf_idf(tf, 100, doc_count, total_docs)
        
        score += tfidf_sum
        
        # 2. Document Type Boost (Additive)
        doc_type = doc_metadata.get('type', 'match')
        type_boost = {
            'player_career_stats': 50.0,
            'player_season_stats': 30.0,
            'season_stats': 20.0,
            'football': 10.0,
            'match': 1.0
        }.get(doc_type, 0.0)
        score += type_boost
        
        # 3. Field Matching Boost (High priority)
        query_str = " ".join(query_terms).lower()
        player_name = str(doc_metadata.get('player_name', '')).lower()
        
        if player_name:
            if query_str == player_name:
                score += 1000.0  # Absolute priority for exact name match
            elif query_str in player_name:
                score += 500.0   # High priority for partial name match
            else:
                # Check if any query term matches player name
                matches = sum(1 for term in query_terms if term in player_name)
                score += 100.0 * matches
                
        # Team match boosts
        home_team = str(doc_metadata.get('home_team', '')).lower()
        away_team = str(doc_metadata.get('away_team', '')).lower()
        teams_in_query = [t for t in query_terms if t in [home_team, away_team]]
        score += 50.0 * len(teams_in_query)
        
        return score

    def rank_results(self, results, query, total_docs, term_doc_counts):
        """
        Rank a list of search results.
        
        Args:
            results: List of document metadata (must include 'doc_id')
            query: Original query string
            total_docs: Total number of documents in index
            term_doc_counts: Dict mapping term -> number of docs containing it
            
        Returns:
            Ranked list of results
        """
        query_terms = query.lower().split()
        
        # Calculate scores without copying documents
        scored_docs = []
        for doc in results:
            # Use matched terms if available (from search_combined)
            matched_terms = doc.get('_matched_terms', query_terms)
            
            # For ranking, we use actual term presence in metadata or raw text
            mock_freqs = {term: 1 for term in matched_terms} 
            score = self.score_document(doc, list(matched_terms), mock_freqs, total_docs, term_doc_counts)
            scored_docs.append((score, doc))
            
        # Sort by score descending
        scored_docs.sort(key=lambda x: x[0], reverse=True)
        
        # Only copy top results with scores
        ranked_results = []
        for score, doc in scored_docs:
            doc_with_score = doc.copy()
            doc_with_score['score'] = score
            ranked_results.append(doc_with_score)
            
        # Aggressive filtering to remove noise
        if ranked_results:
            top_score = ranked_results[0]['score']
            
            # If we have a very strong match, be more aggressive
            if top_score > 30.0:
                # Keep results that are at least 50% of the top score
                threshold = top_score * 0.5
                ranked_results = [doc for doc in ranked_results if doc['score'] >= threshold]
            elif top_score > 10.0:
                # Keep results that are at least 30% of the top score
                threshold = top_score * 0.3
                ranked_results = [doc for doc in ranked_results if doc['score'] >= threshold]
        
        return ranked_results
