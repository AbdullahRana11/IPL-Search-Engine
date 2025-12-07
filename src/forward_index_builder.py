"""
Forward Index Builder Module
Creates a document-to-words mapping (forward index).
"""

import pickle


class ForwardIndexBuilder:
    """Builds and manages the forward index (doc_id -> word_ids)."""
    
    def __init__(self, lexicon):
        """
        Initialize the forward index builder.
        
        Args:
            lexicon: Dictionary mapping words to word IDs
        """
        self.lexicon = lexicon
        self.forward_index = {}
    
    def build_from_documents(self, documents):
        """
        Build forward index from processed documents.
        
        Args:
            documents: List of document dictionaries with 'doc_id' and 'tokens'
            
        Returns:
            Dictionary mapping doc_id to list of word_ids
        """
        print("Building forward index...")
        
        for doc in documents:
            doc_id = doc['doc_id']
            tokens = doc.get('tokens', [])
            
            # Convert words to word IDs
            word_ids = []
            for token in tokens:
                word_id = self.lexicon.get(token)
                if word_id is not None:
                    word_ids.append(word_id)
            
            # Store in forward index
            self.forward_index[doc_id] = word_ids
            
            # Progress indicator
            if (doc_id + 1) % 10000 == 0:
                print(f"Processed {doc_id + 1} documents...")
        
        print(f"Forward index built for {len(self.forward_index)} documents")
        return self.forward_index
    
    def save_to_file(self, filepath='forward_index.pkl'):
        """
        Save forward index to a file.
        
        Args:
            filepath: Path to save the forward index
        """
        print(f"Saving forward index to {filepath}...")
        with open(filepath, 'wb') as f:
            pickle.dump(self.forward_index, f)
        print(f"Forward index saved successfully ({len(self.forward_index)} documents)")
    
    def load_from_file(self, filepath='forward_index.pkl'):
        """
        Load forward index from a file.
        
        Args:
            filepath: Path to load the forward index from
            
        Returns:
            Loaded forward index dictionary
        """
        print(f"Loading forward index from {filepath}...")
        with open(filepath, 'rb') as f:
            self.forward_index = pickle.load(f)
        print(f"Forward index loaded successfully ({len(self.forward_index)} documents)")
        return self.forward_index
    
    def get_document_words(self, doc_id):
        """
        Get word IDs for a document.
        
        Args:
            doc_id: Document ID
            
        Returns:
            List of word IDs in the document
        """
        return self.forward_index.get(doc_id, [])
    
    def get_forward_index(self):
        """Get the complete forward index."""
        return self.forward_index


# Test the forward index builder
if __name__ == "__main__":
    from document_processor import DocumentProcessor
    from lexicon_builder import LexiconBuilder
    
    # Process documents
    processor = DocumentProcessor('Dataset/IPL/all_season_details.csv')
    processor.process_documents(max_docs=100)
    documents = processor.get_documents()
    
    # Build lexicon
    lex_builder = LexiconBuilder()
    lexicon = lex_builder.build_from_documents(documents)
    
    # Build forward index
    fwd_builder = ForwardIndexBuilder(lexicon)
    forward_index = fwd_builder.build_from_documents(documents)
    
    # Show sample
    print("\nSample forward index entries:")
    print("-" * 50)
    for doc_id in list(forward_index.keys())[:3]:
        word_ids = forward_index[doc_id]
        print(f"Doc {doc_id}: {len(word_ids)} words -> {word_ids[:10]}...")
    
    # Save to file
    fwd_builder.save_to_file('test_forward_index.pkl')
