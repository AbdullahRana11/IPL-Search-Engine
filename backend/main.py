"""
Main IPL Search Engine Indexing Pipeline
Orchestrates the entire indexing process: preprocessing -> lexicon -> forward index -> inverted index
"""

import time
import os
from src.document_processor import DocumentProcessor
from src.lexicon_builder import LexiconBuilder
from src.forward_index_builder import ForwardIndexBuilder
from src.inverted_index_builder import InvertedIndexBuilder


def build_indices(dataset_path, max_docs=None, output_dir='.'):
    """
    Build all indices from the dataset.
    
    Args:
        dataset_path: Path to the CSV dataset
        max_docs: Maximum number of documents to process (None for all)
        output_dir: Directory to save index files
        
    Returns:
        Tuple of (lexicon, forward_index, inverted_index)
    """
    print("=" * 80)
    print("IPL SEARCH ENGINE - INDEXING PIPELINE")
    print("=" * 80)
    
    start_time = time.time()
    
    # Step 1: Process documents
    print("\n[1/4] Processing Documents")
    print("-" * 80)
    processor = DocumentProcessor(dataset_path)
    processor.process_documents(max_docs=max_docs)
    documents = processor.get_documents()

    # Step 1.5: Process Football Documents
    print("\n[1.5/4] Processing Football Documents")
    print("-" * 80)
    # Assuming dataset_path is 'Dataset/IPL/all_season_details.csv'
    # We need to find 'Dataset/Football 2'
    # Let's derive the dataset root
    dataset_root = os.path.dirname(os.path.dirname(dataset_path)) # Dataset/IPL -> Dataset
    football_path = os.path.join(dataset_root, 'Football 2')
    
    if os.path.exists(football_path):
        processor.process_football_documents(football_path)
        documents = processor.get_documents() # Update documents list
    else:
        print(f"Warning: Football dataset not found at {football_path}")

    # Generate season stats (Orange Cap, Purple Cap)
    processor.generate_season_stats()
    
    # Generate player stats (Career and Season-wise)
    processor.generate_player_career_stats()
    processor.generate_player_season_stats()
    
    documents = processor.get_documents() # Update documents list again

    
    # Create separate directories for each component (simulating distributed nodes)
    lexicon_dir = os.path.join(output_dir, 'lexicon_data')
    forward_dir = os.path.join(output_dir, 'forward_index_data')
    inverted_dir = os.path.join(output_dir, 'inverted_index_data')
    metadata_dir = os.path.join(output_dir, 'metadata_data')
    
    for d in [lexicon_dir, forward_dir, inverted_dir, metadata_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    # Save metadata
    processor.save_metadata(os.path.join(metadata_dir, 'metadata.pkl'))
    
    # Step 2: Build lexicon
    print("\n[2/4] Building Lexicon")
    print("-" * 80)
    lex_builder = LexiconBuilder()
    lexicon = lex_builder.build_from_documents(documents)
    lex_builder.save_to_file(os.path.join(lexicon_dir, 'lexicon.pkl'))
    
    # Step 3: Build forward index
    print("\n[3/4] Building Forward Index")
    print("-" * 80)
    fwd_builder = ForwardIndexBuilder(lexicon)
    forward_index = fwd_builder.build_from_documents(documents)
    fwd_builder.save_to_file(os.path.join(forward_dir, 'forward_index.pkl'))
    
    # Step 4: Build inverted index
    print("\n[4/4] Building Inverted Index (with Barrels)")
    print("-" * 80)
    
    # Initialize barrel manager
    from src.barrel_manager import BarrelManager
    # Barrels go inside the inverted index directory
    barrel_mgr = BarrelManager(output_dir=inverted_dir)
    
    # Build inverted index
    inv_builder = InvertedIndexBuilder(barrel_mgr)
    inv_builder.build_from_forward_index(forward_index)
    
    # Summary
    elapsed_time = time.time() - start_time
    print("\n" + "=" * 80)
    print("INDEXING COMPLETE!")
    print("=" * 80)
    print(f"Total documents processed: {len(documents):,}")
    print(f"Unique words in lexicon: {len(lexicon):,}")
    print(f"Time taken: {elapsed_time:.2f} seconds")
    print(f"\nIndex data saved to separate folders:")
    print(f"  - {lexicon_dir}/lexicon.pkl")
    print(f"  - {forward_dir}/forward_index.pkl")
    print(f"  - {inverted_dir}/barrel_*.pkl")
    print(f"  - {metadata_dir}/metadata.pkl")
    print("=" * 80)
    
    return lexicon, forward_index, barrel_mgr


if __name__ == "__main__":
    # Configuration
    DATASET_PATH = 'Dataset/IPL/all_season_details.csv'
    MAX_DOCUMENTS = None  # Set to None to process all documents, or a number for testing
    OUTPUT_DIR = '.'
    
    # Build indices
    build_indices(DATASET_PATH, max_docs=MAX_DOCUMENTS, output_dir=OUTPUT_DIR)
