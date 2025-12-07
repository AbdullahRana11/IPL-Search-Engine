"""
Test Suite for Barrels Implementation
Validates that the inverted index is correctly partitioned into barrels.
"""

import os
import shutil
import pickle
import sys

# Add parent directory to path to allow importing from src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.document_processor import DocumentProcessor
from src.lexicon_builder import LexiconBuilder
from src.forward_index_builder import ForwardIndexBuilder
from src.inverted_index_builder import InvertedIndexBuilder
from src.barrel_manager import BarrelManager


def test_barrels_functionality():
    """Test the barrel creation and loading logic."""
    
    print("=" * 80)
    print("TESTING BARRELS IMPLEMENTATION")
    print("=" * 80)
    
    # Configuration
    dataset_path = 'Dataset/IPL/all_season_details.csv'
    test_docs = 500
    test_barrel_dir = 'test_barrels_output'
    
    # Clean up previous test run
    if os.path.exists(test_barrel_dir):
        shutil.rmtree(test_barrel_dir)
    
    # Step 1: Prepare data (Docs -> Lexicon -> Forward Index)
    print("\n[1/3] Preparing data...")
    processor = DocumentProcessor(dataset_path)
    processor.process_documents(max_docs=test_docs)
    documents = processor.get_documents()
    
    lex_builder = LexiconBuilder()
    lexicon = lex_builder.build_from_documents(documents)
    
    fwd_builder = ForwardIndexBuilder(lexicon)
    forward_index = fwd_builder.build_from_documents(documents)
    
    print(f"✓ Prepared {len(documents)} docs, {len(lexicon)} words")
    
    # Step 2: Build Inverted Index with Barrels
    print("\n[2/3] Building inverted index with barrels...")
    
    # Use small barrel size to force multiple barrels
    barrel_size = 500 
    barrel_mgr = BarrelManager(output_dir=test_barrel_dir, barrel_size=barrel_size)
    inv_builder = InvertedIndexBuilder(barrel_mgr)
    
    inv_builder.build_from_forward_index(forward_index)
    
    # Check if barrel files were created
    barrel_files = os.listdir(test_barrel_dir)
    print(f"✓ Created {len(barrel_files)} barrel files: {barrel_files}")
    assert len(barrel_files) > 0, "No barrel files created!"
    
    # Step 3: Verify Data Integrity
    print("\n[3/3] Verifying data integrity...")
    
    # Pick a random word and check if we can find its documents
    test_word = list(lexicon.keys())[10] # Pick 10th word
    test_word_id = lexicon[test_word]
    
    print(f"Testing word: '{test_word}' (ID: {test_word_id})")
    
    # Get expected docs from forward index manually
    expected_docs = []
    for doc_id, word_ids in forward_index.items():
        if test_word_id in word_ids:
            expected_docs.append(doc_id)
    
    # Get actual docs from barrel manager
    actual_docs = barrel_mgr.get_documents_for_word(test_word_id)
    
    print(f"  Expected docs: {len(expected_docs)}")
    print(f"  Actual docs:   {len(actual_docs)}")
    
    assert set(expected_docs) == set(actual_docs), "Mismatch in document lists!"
    print("✓ Data verification passed!")
    
    # Verify barrel assignment logic
    expected_barrel_id = test_word_id // barrel_size
    print(f"Word ID {test_word_id} should be in barrel {expected_barrel_id}")
    
    # Load the specific barrel file and check
    barrel_data = barrel_mgr.load_barrel(expected_barrel_id)
    assert test_word_id in barrel_data, f"Word ID {test_word_id} not found in barrel {expected_barrel_id}"
    print(f"✓ Word found in correct barrel file")

    print("\n" + "=" * 80)
    print("BARREL TESTS PASSED! ✓")
    print("=" * 80)


if __name__ == "__main__":
    test_barrels_functionality()
