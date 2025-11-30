"""
Inverted Index Builder Module
Creates a word-to-documents mapping (inverted index).
"""

import pickle
import os
from collections import defaultdict


class InvertedIndexBuilder:
    """Builds and manages the inverted index using barrels."""
    
    def __init__(self, barrel_manager=None):
        """
        Initialize the inverted index builder.
        
        Args:
            barrel_manager: Instance of BarrelManager (optional)
        """
        if barrel_manager:
            self.barrel_manager = barrel_manager
        else:
            from .barrel_manager import BarrelManager
            self.barrel_manager = BarrelManager()
    
    def build_from_forward_index(self, forward_index):
        """
        Build inverted index from forward index and store in barrels.
        
        Args:
            forward_index: Dictionary mapping doc_id to list of word_ids
        """
        print("Building inverted index (using barrels)...")
        
        count = 0
        # Invert the forward index
        for doc_id, word_ids in forward_index.items():
            for word_id in word_ids:
                # Add to barrel buffer
                self.barrel_manager.add_to_barrel(word_id, doc_id)
            
            count += 1
            # Progress indicator & periodic flush
            if count % 10000 == 0:
                print(f"Processed {count} documents...")
                # Optional: flush periodically to save memory if dataset is huge
                # self.barrel_manager.flush_barrels()
        
        # Final flush to save all data
        self.barrel_manager.flush_barrels()
        print("Inverted index built and saved to barrels.")


# Test the inverted index builder with barrels
if __name__ == "__main__":
    from document_processor import DocumentProcessor
    from lexicon_builder import LexiconBuilder
    from forward_index_builder import ForwardIndexBuilder
    from barrel_manager import BarrelManager
    import shutil
    
    # Clean up old test barrels
    if os.path.exists('test_barrels'):
        shutil.rmtree('test_barrels')
    
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
    
    # Build inverted index with barrels
    barrel_mgr = BarrelManager(output_dir='test_barrels', barrel_size=100)
    inv_builder = InvertedIndexBuilder(barrel_mgr)
    inv_builder.build_from_forward_index(forward_index)
    
    # Verify
    print("\nVerifying barrels:")
    test_word_id = 0
    docs = barrel_mgr.get_documents_for_word(test_word_id)
    print(f"Word ID {test_word_id} found in {len(docs)} documents")
