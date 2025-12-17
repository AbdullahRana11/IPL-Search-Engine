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
            'football': 1.0,
            'match': 1.0                 # Standard priority
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
        Score a single document against the query.
        
        Args:
            doc_metadata: Metadata dictionary for the document
            query_terms: List of query terms
            doc_term_freqs: Dict mapping term -> frequency in this doc
            total_docs: Total number of documents
            term_doc_counts: Dict mapping term -> number of docs containing it
            
        Returns:
            Float score
        """
        score = 0.0
        
        # 1. TF-IDF Score
        # We approximate doc length as sum of frequencies for simplicity, 
        # or use a standard average length if not available.
        # Here we'll just sum the TF-IDF for each query term present.
        for term in query_terms:
            tf = doc_term_freqs.get(term, 0)
            if tf > 0:
                doc_count = term_doc_counts.get(term, 0)
                score += self.calculate_tf_idf(tf, 100, doc_count, total_docs) # Assumed avg doc len 100
        
        # 2. Document Type Boost
        doc_type = doc_metadata.get('type', 'match')
        type_boost = self.type_boosts.get(doc_type, 1.0)
        score *= type_boost
        
        # 3. Field Matching Boost
        # If query terms appear in specific metadata fields, boost the score
        field_score = 0
        query_str = " ".join(query_terms).lower()
        
        # Check player name match
        player_name = str(doc_metadata.get('player_name', '')).lower()
        if player_name:
            if query_str == player_name:
                field_score += 10.0  # Exact name match
            elif query_str in player_name:
                field_score += 5.0   # Partial name match
            elif any(term in player_name for term in query_terms):
                field_score += 2.0   # Term match
                
        # Check season match
        season = str(doc_metadata.get('season', ''))
        if season and season in query_terms:
            field_score += 3.0
            
        score += field_score
        
        # 4. Recency Boost (for seasons)
        if season and season.isdigit():
            season_year = int(season)
            # Boost recent seasons slightly
            if season_year >= 2023:
                score *= 1.1
        
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
        ranked_results = []
        
        for doc in results:
            # In a real system, we'd have term frequencies from the forward index.
            # For now, we'll estimate or require them to be passed.
            # Since we don't have easy access to forward index here without loading it,
            # we'll rely more on metadata matching for this implementation.
            
            # Mock term freqs based on metadata matching for now
            # (Real implementation would pass this from search engine)
            mock_freqs = {term: 1 for term in query_terms} 
            
            score = self.score_document(doc, query_terms, mock_freqs, total_docs, term_doc_counts)
            
            # Add score to doc for debugging
            doc_with_score = doc.copy()
            doc_with_score['score'] = score
            ranked_results.append(doc_with_score)
            
        # Sort by score descending
        ranked_results.sort(key=lambda x: x['score'], reverse=True)
        
        return ranked_results
