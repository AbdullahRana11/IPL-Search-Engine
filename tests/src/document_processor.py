"""
Document Processor Module
Handles loading and processing IPL dataset into searchable documents.
"""

import pandas as pd
from .preprocessor import TextPreprocessor


class DocumentProcessor:
    """Processes CSV data into searchable documents."""
    
    def __init__(self, dataset_path, preprocessor=None):
        """
        Initialize document processor.
        
        Args:
            dataset_path: Path to the CSV dataset file
            preprocessor: TextPreprocessor instance (creates default if None)
        """
        self.dataset_path = dataset_path
        self.preprocessor = preprocessor or TextPreprocessor(remove_stopwords=False)
        self.documents = []
        self.doc_metadata = []
    
    def load_dataset(self, max_rows=None):
        """
        Load the CSV dataset.
        
        Args:
            max_rows: Maximum number of rows to load (None for all)
            
        Returns:
            DataFrame with loaded data
        """
        print(f"Loading dataset from {self.dataset_path}...")
        
        # Load CSV with low_memory=False to handle mixed types
        df = pd.read_csv(self.dataset_path, low_memory=False, nrows=max_rows)
        
        print(f"Loaded {len(df)} records")
        return df
    
    def create_document_text(self, row):
        """
        Create searchable text from a dataset row.
        Combines relevant fields into a single text string.
        
        Args:
            row: DataFrame row
            
        Returns:
            Combined text string
        """
        # Fields to include in the document
        fields = [
            'match_name',
            'home_team', 
            'away_team',
            'batsman1_name',
            'batsman2_name',
            'bowler1_name',
            'bowler2_name',
            'shortText',
            'text',
            'wkt_batsman_name',
            'wkt_bowler_name',
            'wkt_text'
        ]
        
        # Combine non-null field values
        text_parts = []
        for field in fields:
            if field in row and pd.notna(row[field]):
                text_parts.append(str(row[field]))
        
        return ' '.join(text_parts)
    
    def process_documents(self, max_docs=None):
        """
        Process all documents from the dataset.
        
        Args:
            max_docs: Maximum number of documents to process (None for all)
            
        Returns:
            Number of documents processed
        """
        # Load dataset
        df = self.load_dataset(max_rows=max_docs)
        
        print("Processing documents...")
        
        # Process each row
        for idx, row in df.iterrows():
            # Create document text
            doc_text = self.create_document_text(row)
            
            # Tokenize the text
            tokens = self.preprocessor.preprocess(doc_text)
            
            # Store document
            self.documents.append({
                'doc_id': idx,
                'tokens': tokens,
                'raw_text': doc_text[:200]  # Store first 200 chars for reference
            })
            
            # Store metadata for search results
            self.doc_metadata.append({
                'doc_id': idx,
                'match_name': row.get('match_name', 'N/A'),
                'season': row.get('season', 'N/A'),
                'home_team': row.get('home_team', 'N/A'),
                'away_team': row.get('away_team', 'N/A'),
                'over': row.get('over', 'N/A'),
                'ball': row.get('ball', 'N/A')
            })
            
            # Progress indicator
            if (idx + 1) % 10000 == 0:
                print(f"Processed {idx + 1} documents...")
        
        print(f"Total documents processed: {len(self.documents)}")
        return len(self.documents)
    
    def get_documents(self):
        """Get all processed documents."""
        return self.documents
    
    def get_metadata(self):
        """Get document metadata."""
        return self.doc_metadata


# Test the document processor
if __name__ == "__main__":
    processor = DocumentProcessor('Dataset/IPL/all_season_details.csv')
    
    # Process first 100 documents
    processor.process_documents(max_docs=100)
    
    # Show sample documents
    print("\nSample Documents:")
    print("-" * 80)
    for i, doc in enumerate(processor.get_documents()[:3]):
        print(f"Doc ID: {doc['doc_id']}")
        print(f"Tokens ({len(doc['tokens'])}): {doc['tokens'][:10]}...")
        print(f"Raw Text: {doc['raw_text']}")
        print()
