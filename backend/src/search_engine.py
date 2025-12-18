"""
Search Engine Module
Handles searching through the indexed documents.
"""

import os
import pickle
import time
from .lexicon_builder import LexiconBuilder
from .barrel_manager import BarrelManager
from .document_processor import DocumentProcessor

from .ranking import Ranker

class SearchEngine:
    """Main search engine class."""
    
    def __init__(self, output_dir='.'):
        """
        Initialize the search engine.
        
        Args:
            output_dir: Directory containing the index data
        """
        self.output_dir = output_dir
        self.lexicon_dir = os.path.join(output_dir, 'lexicon_data')
        self.inverted_dir = os.path.join(output_dir, 'inverted_index_data')
        self.metadata_dir = os.path.join(output_dir, 'metadata_data')
        
        self.lexicon = None
        self.barrel_manager = None
        self.metadata = None
        self.ranker = Ranker()
        
        self.load_indices()
        
        # Synonym mappings for better search results
        self.synonyms = {
            "top run scorer": "orange cap",
            "highest run scorer": "orange cap",
            "most runs": "orange cap",
            "top wicket taker": "purple cap",
            "highest wicket taker": "purple cap",
            "most wickets": "purple cap",
            "top scorer": "orange cap", # Assuming cricket context usually
        }
        
    def _preprocess_query(self, query):
        """Apply synonym mappings to the query."""
        query_lower = query.lower()
        for phrase, replacement in self.synonyms.items():
            if phrase in query_lower:
                query_lower = query_lower.replace(phrase, replacement)
        return query_lower
        
    def load_indices(self):
        """Load all necessary indices and metadata."""
        print("Loading search engine indices...")
        
        # Load Lexicon
        lex_builder = LexiconBuilder()
        self.lexicon = lex_builder.load_from_file(os.path.join(self.lexicon_dir, 'lexicon.pkl'))
        
        # Initialize Barrel Manager (for reading inverted index)
        self.barrel_manager = BarrelManager(output_dir=self.inverted_dir)
        
        # Load Metadata
        processor = DocumentProcessor('') # Path doesn't matter for loading metadata
        self.metadata = processor.load_metadata(os.path.join(self.metadata_dir, 'metadata.pkl'))
        
        # Convert metadata list to dict for faster lookup by doc_id
        self.metadata_map = {doc['doc_id']: doc for doc in self.metadata}
        
    def _get_word_id(self, word):
        """Get ID for a word (case-insensitive)."""
        # The lexicon might be case-sensitive or not depending on preprocessing.
        # Assuming preprocessing lowercased everything.
        return self.lexicon.get(word.lower())
        
    def _get_doc_metadata(self, doc_ids):
        """Get metadata for a list of document IDs."""
        results = []
        for doc_id in doc_ids:
            if doc_id in self.metadata_map:
                results.append(self.metadata_map[doc_id])
        return results
        
    def search_single(self, query, max_results=100):
        """
        Search for a single word.
        
        Args:
            query: Single word string
            max_results: Maximum number of results to return
            
        Returns:
            List of document metadata
        """
        # Apply synonyms (though less likely to match phrases here)
        query = self._preprocess_query(query)
        
        word_id = self._get_word_id(query)
        if word_id is None:
            return []
            
        doc_ids = self.barrel_manager.get_documents_for_word(word_id)
        
        # Limit results early to avoid processing too many documents
        if len(doc_ids) > max_results * 2:
            doc_ids = doc_ids[:max_results * 2]
        
        results = self._get_doc_metadata(doc_ids)
        
        # Rank results
        term_doc_counts = {query.lower(): len(doc_ids)}
        ranked = self.ranker.rank_results(results, query, len(self.metadata), term_doc_counts)
        return ranked[:max_results]
        
    def search_multi(self, query, max_results=100):
        """
        Search for multiple words (AND logic).
        
        Args:
            query: Space-separated words
            max_results: Maximum number of results to return
            
        Returns:
            List of document metadata
        """
        # Apply synonyms
        query = self._preprocess_query(query)
        
        words = query.split()
        if not words:
            return []
            
        # Get doc_ids for the first word
        first_word_id = self._get_word_id(words[0])
        if first_word_id is None:
            return []
            
        result_doc_ids = set(self.barrel_manager.get_documents_for_word(first_word_id))
        
        # Keep track of doc counts for ranking
        term_doc_counts = {}
        term_doc_counts[words[0].lower()] = len(result_doc_ids)
        
        # Intersect with doc_ids for remaining words
        for word in words[1:]:
            word_id = self._get_word_id(word)
            if word_id is None:
                return [] # If any word is missing, AND result is empty
                
            current_doc_ids = set(self.barrel_manager.get_documents_for_word(word_id))
            term_doc_counts[word.lower()] = len(current_doc_ids)
            
            result_doc_ids.intersection_update(current_doc_ids)
            
            if not result_doc_ids:
                break
                
        # Limit results early
        doc_id_list = list(result_doc_ids)
        if len(doc_id_list) > max_results * 2:
            doc_id_list = doc_id_list[:max_results * 2]
        
        results = self._get_doc_metadata(doc_id_list)
        
        # Rank results
        ranked = self.ranker.rank_results(results, query, len(self.metadata), term_doc_counts)
        return ranked[:max_results]

    def search_combined(self, query, max_results=100):
        """
        Search for multiple queries separated by ',' or ' and '.
        Results are combined (UNION).
        
        Args:
            query: Query string containing ',' or ' and '
            max_results: Maximum number of results to return
            
        Returns:
            List of document metadata
        """
        # Apply synonyms
        query = self._preprocess_query(query)
        
        # Normalize separators: replace ' and ' with ',' for easier splitting
        normalized_query = query.lower().replace(' and ', ',')
        parts = [p.strip() for p in normalized_query.split(',') if p.strip()]
        
        if not parts:
            return []
            
        all_results_map = {} # doc_id -> doc metadata
        doc_matched_terms = {} # doc_id -> set of terms that matched
        all_terms = set()
        
        # Stop words to ignore in intersections
        stop_words = {'vs', 'and', 'the', 'in', 'of', 'at', 'with', 'a', 'an', 'is', 'are'}
        
        # Limit per part to avoid overwhelming the ranker
        limit_per_part = max_results * 2
        
        for part in parts:
            # Search for this part
            part_matched_ids = set()
            part_terms = set()
            
            if ' ' in part:
                # Multi-word search (AND logic within part)
                words = [w for w in part.split() if w not in stop_words]
                if not words:
                    # If all words were stop words, use original words
                    words = part.split()
                    
                part_terms.update(words)
                all_terms.update(words)
                
                # Get intersection of doc IDs
                res_ids = None
                for word in words:
                    w_id = self._get_word_id(word)
                    if w_id is None:
                        # If a non-stop word is missing, this part has no results
                        res_ids = set()
                        break
                    
                    word_docs = set(self.barrel_manager.get_documents_for_word(w_id))
                    if res_ids is None:
                        res_ids = word_docs
                    else:
                        res_ids.intersection_update(word_docs)
                    
                    if not res_ids:
                        break
                
                if res_ids:
                    part_matched_ids = res_ids
            else:
                # Single word search
                part_terms.add(part)
                all_terms.add(part)
                w_id = self._get_word_id(part)
                if w_id is not None:
                    part_matched_ids = set(self.barrel_manager.get_documents_for_word(w_id))
            
            # Limit IDs from this part before fetching metadata
            id_list = list(part_matched_ids)
            if len(id_list) > limit_per_part:
                id_list = id_list[:limit_per_part]
                
            results = self._get_doc_metadata(id_list)
            for doc in results:
                doc_id = doc['doc_id']
                if doc_id not in all_results_map:
                    all_results_map[doc_id] = doc
                    doc_matched_terms[doc_id] = set()
                
                # Track which terms from THIS part matched this doc
                doc_matched_terms[doc_id].update(part_terms)
        
        # Calculate term counts for all terms found
        term_doc_counts = {}
        for term in all_terms:
            w_id = self._get_word_id(term)
            if w_id is not None:
                term_doc_counts[term] = len(self.barrel_manager.get_documents_for_word(w_id))
            else:
                term_doc_counts[term] = 0
                    
        # Rank combined results
        docs_to_rank = []
        for doc_id, doc in all_results_map.items():
            doc_copy = doc.copy()
            doc_copy['_matched_terms'] = doc_matched_terms[doc_id]
            docs_to_rank.append(doc_copy)
            
        ranking_query = " ".join(parts)
        ranked = self.ranker.rank_results(docs_to_rank, ranking_query, len(self.metadata), term_doc_counts)
        
        # Clean up temporary field
        for res in ranked:
            if '_matched_terms' in res:
                del res['_matched_terms']
                
        return ranked[:max_results]
