"""
Test Suite for IPL Search Engine Indexing
Validates the correctness of lexicon, forward index, and inverted index.
"""

import pickle
import pickle
import os
import sys

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.document_processor import DocumentProcessor
from src.lexicon_builder import LexiconBuilder
from src.forward_index_builder import ForwardIndexBuilder
from src.inverted_index_builder import InvertedIndexBuilder


def test_indexing_small_dataset():
    """Test the indexing pipeline with a small dataset."""
    
    print("=" * 80)
    print("TESTING INDEXING PIPELINE")
    print("=" * 80)
    
    # Configuration
    dataset_path = 'Dataset/IPL/all_season_details.csv'
    test_docs = 1000  # Test with 1000 documents
    
    # Step 1: Process documents
    print("\n[1/4] Processing test documents...")
    processor = DocumentProcessor(dataset_path)
    processor.process_documents(max_docs=test_docs)
    documents = processor.get_documents()
    
    assert len(documents) > 0, "No documents processed!"
    assert len(documents) <= test_docs, "Too many documents processed!"
    print(f"✓ Processed {len(documents)} documents")
    
    # Step 2: Build lexicon
    print("\n[2/4] Building lexicon...")
    lex_builder = LexiconBuilder()
    lexicon = lex_builder.build_from_documents(documents)
    
    assert len(lexicon) > 0, "Lexicon is empty!"
    print(f"✓ Lexicon contains {len(lexicon)} unique words")
    
    # Show sample words
    print("\nSample words from lexicon:")
    for word, word_id in list(lexicon.items())[:10]:
        print(f"  {word_id:5d} -> {word}")
    
    # Step 3: Build forward index
    print("\n[3/4] Building forward index...")
    fwd_builder = ForwardIndexBuilder(lexicon)
    forward_index = fwd_builder.build_from_documents(documents)
    
    assert len(forward_index) == len(documents), "Forward index size mismatch!"
    print(f"✓ Forward index built for {len(forward_index)} documents")
    
    # Show sample
    print("\nSample forward index entries:")
    for doc_id in list(forward_index.keys())[:3]:
        word_ids = forward_index[doc_id]
        print(f"  Doc {doc_id}: {len(word_ids)} words")
    
    # Step 4: Build inverted index
    print("\n[4/4] Building inverted index...")
    from src.barrel_manager import BarrelManager
    barrel_mgr = BarrelManager(output_dir='test_indexing_barrels')
    inv_builder = InvertedIndexBuilder(barrel_mgr)
    inv_builder.build_from_forward_index(forward_index)
    
    print(f"✓ Inverted index built and saved to barrels")
    
    # Test bidirectional consistency
    print("\n[5/5] Testing consistency...")
    test_word = list(lexicon.keys())[0]
    test_word_id = lexicon[test_word]
    
    # Get docs from barrel
    docs_with_word = barrel_mgr.get_documents_for_word(test_word_id)
    
    if docs_with_word:
        test_doc_id = docs_with_word[0]
        words_in_doc = forward_index[test_doc_id]
        assert test_word_id in words_in_doc, "Consistency check failed!"
        print(f"✓ Consistency verified: word '{test_word}' found in doc {test_doc_id}")
    
    # Save test indices
    print("\nSaving test indices...")
    lex_builder.save_to_file('test_lexicon.pkl')
    fwd_builder.save_to_file('test_forward_index.pkl')
    # inv_builder.save_to_file('test_inverted_index.pkl') # No longer needed with barrels
    
    print("\n" + "=" * 80)
    print("ALL TESTS PASSED! ✓")
    print("=" * 80)
    print("\nTest index files saved:")
    print("  - test_lexicon.pkl")
    print("  - test_forward_index.pkl")
    print("  - test_indexing_barrels/")


if __name__ == "__main__":
    # Run tests
    test_indexing_small_dataset()
