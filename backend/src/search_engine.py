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
        return self._get_doc_metadata(doc_ids)
        
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
        
        # Intersect with doc_ids for remaining words
        for word in words[1:]:
            word_id = self._get_word_id(word)
            if word_id is None:
                return [] # If any word is missing, AND result is empty
                
            current_doc_ids = set(self.barrel_manager.get_documents_for_word(word_id))
            result_doc_ids.intersection_update(current_doc_ids)
            
            if not result_doc_ids:
                break
                
        return self._get_doc_metadata(list(result_doc_ids))

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
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
                
            # Search for this part
            # If it has multiple words, use search_multi, else search_single
            if ' ' in part:
                results = self.search_multi(part)
            else:
                results = self.search_single(part)
                
            # Add unique results
            for doc in results:
                if doc['doc_id'] not in seen_doc_ids:
                    all_results.append(doc)
                    seen_doc_ids.add(doc['doc_id'])
                    
        return all_results
