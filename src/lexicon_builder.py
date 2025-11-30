"""
Lexicon Builder Module
Creates a word-to-ID mapping (lexicon) from processed documents.
"""

import pickle
from collections import OrderedDict


class LexiconBuilder:
    """Builds and manages the lexicon (word vocabulary)."""
    
    def __init__(self):
        """Initialize the lexicon builder."""
        self.lexicon = {}  # word -> word_id mapping
        self.next_word_id = 0
    
    def build_from_documents(self, documents):
        """
        Build lexicon from a list of processed documents.
        
        Args:
            documents: List of document dictionaries with 'tokens' field
            
        Returns:
            Dictionary mapping words to word IDs
        """
        print("Building lexicon...")
        
        unique_words = set()
        
        # Collect all unique words
        for doc in documents:
            tokens = doc.get('tokens', [])
            unique_words.update(tokens)
        
        # Sort words for consistent ordering
        sorted_words = sorted(unique_words)
        
        # Assign IDs to words
        for word in sorted_words:
            self.lexicon[word] = self.next_word_id
            self.next_word_id += 1
        
        print(f"Lexicon built with {len(self.lexicon)} unique words")
        return self.lexicon
    
    def save_to_file(self, filepath='lexicon.pkl'):
        """
        Save lexicon to a file.
        
        Args:
            filepath: Path to save the lexicon
        """
        print(f"Saving lexicon to {filepath}...")
        with open(filepath, 'wb') as f:
            pickle.dump(self.lexicon, f)
        print(f"Lexicon saved successfully ({len(self.lexicon)} words)")
    
    def load_from_file(self, filepath='lexicon.pkl'):
        """
        Load lexicon from a file.
        
        Args:
            filepath: Path to load the lexicon from
            
        Returns:
            Loaded lexicon dictionary
        """
        print(f"Loading lexicon from {filepath}...")
        with open(filepath, 'rb') as f:
            self.lexicon = pickle.load(f)
        self.next_word_id = len(self.lexicon)
        print(f"Lexicon loaded successfully ({len(self.lexicon)} words)")
        return self.lexicon
    
    def get_word_id(self, word):
        """
        Get the ID for a word.
        
        Args:
            word: The word to look up
            
        Returns:
            Word ID or None if word not in lexicon
        """
        return self.lexicon.get(word)
    
    def get_lexicon(self):
        """Get the complete lexicon."""
        return self.lexicon


# Test the lexicon builder
if __name__ == "__main__":
    from document_processor import DocumentProcessor
    
    # Process sample documents
    processor = DocumentProcessor('Dataset/IPL/all_season_details.csv')
    processor.process_documents(max_docs=100)
    documents = processor.get_documents()
    
    # Build lexicon
    builder = LexiconBuilder()
    lexicon = builder.build_from_documents(documents)
    
    # Show sample
    print("\nSample words from lexicon:")
    print("-" * 50)
    sample_words = list(lexicon.items())[:10]
    for word, word_id in sample_words:
        print(f"{word_id:5d} -> {word}")
    
    # Save to file
    builder.save_to_file('test_lexicon.pkl')
