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
        
    def search_single(self, query):
        """
        Search for a single word.
        
        Args:
            query: Single word string
            
        Returns:
            List of document metadata
        """
        word_id = self._get_word_id(query)
        if word_id is None:
            return []
            
        doc_ids = self.barrel_manager.get_documents_for_word(word_id)
        results = self._get_doc_metadata(doc_ids)
        
        # Rank results
        term_doc_counts = {query.lower(): len(doc_ids)}
        return self.ranker.rank_results(results, query, len(self.metadata), term_doc_counts)
        
    def search_multi(self, query):
        """
        Search for multiple words (AND logic).
        
        Args:
            query: Space-separated words
            
        Returns:
            List of document metadata
        """
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
                
        results = self._get_doc_metadata(list(result_doc_ids))
        
        # Rank results
        return self.ranker.rank_results(results, query, len(self.metadata), term_doc_counts)

    def search_combined(self, query):
        """
        Search for multiple queries separated by ' and '.
        Results are combined (UNION).
        
        Args:
            query: Query string containing ' and '
            
        Returns:
            List of document metadata
        """
        # Split by ' and ' (case-insensitive)
        parts = query.lower().split(' and ')
        
        all_results = []
        seen_doc_ids = set()
        
        # Collect all terms for ranking context
        all_terms = set()
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            
            all_terms.update(part.split())
                
            # Search for this part
            # If it has multiple words, use search_multi, else search_single
            # We use the internal methods but without ranking yet to avoid double ranking
            if ' ' in part:
                # Inline search_multi logic to get raw docs
                words = part.split()
                if not words: continue
                
                w_id = self._get_word_id(words[0])
                if w_id is None: continue
                res_ids = set(self.barrel_manager.get_documents_for_word(w_id))
                
                for w in words[1:]:
                    w_id = self._get_word_id(w)
                    if w_id is None: 
                        res_ids = set()
                        break
                    res_ids.intersection_update(set(self.barrel_manager.get_documents_for_word(w_id)))
                
                results = self._get_doc_metadata(list(res_ids))
            else:
                # Inline search_single logic
                w_id = self._get_word_id(part)
                if w_id is None: continue
                res_ids = self.barrel_manager.get_documents_for_word(w_id)
                results = self._get_doc_metadata(res_ids)
                
            # Add unique results
            for doc in results:
                if doc['doc_id'] not in seen_doc_ids:
                    all_results.append(doc)
                    seen_doc_ids.add(doc['doc_id'])
        
        # Calculate term counts for all terms found
        term_doc_counts = {}
        for term in all_terms:
            w_id = self._get_word_id(term)
            if w_id is not None:
                term_doc_counts[term] = len(self.barrel_manager.get_documents_for_word(w_id))
            else:
                term_doc_counts[term] = 0
                    
        # Rank combined results
        return self.ranker.rank_results(all_results, query.replace(' and ', ' '), len(self.metadata), term_doc_counts)
