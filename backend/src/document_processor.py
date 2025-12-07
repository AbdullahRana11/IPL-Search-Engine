"""
Document Processor Module
Handles loading and processing IPL dataset into searchable documents.
"""

import pandas as pd
import pickle
import os
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

    def process_football_documents(self, football_path):
        """
        Process football documents from the dataset.
        
        Args:
            football_path: Path to the football dataset directory
            
        Returns:
            Number of documents processed
        """
        print(f"Processing football documents from {football_path}...")
        
        players_csv = os.path.join(football_path, 'List Of All Players Names.csv')
        images_dir = os.path.join(football_path, 'Images', 'Images')
        
        if not os.path.exists(players_csv):
            print(f"Football players CSV not found: {players_csv}")
            return 0
            
        # Load players data
        df = pd.read_csv(players_csv)
        
        # Get list of image folders (groups)
        image_groups = []
        if os.path.exists(images_dir):
            image_groups = [d for d in os.listdir(images_dir) if os.path.isdir(os.path.join(images_dir, d))]
        
        # Pre-scan all player folders to avoid nested loops for every row
        # Map: normalized_player_name -> list of image paths
        player_image_map = {}
        
        print("Scanning image directories...")
        for group in image_groups:
            group_path = os.path.join(images_dir, group)
            # List country folders
            country_folders = [d for d in os.listdir(group_path) if os.path.isdir(os.path.join(group_path, d))]
            
            for country in country_folders:
                country_path = os.path.join(group_path, country)
                # List player folders
                player_folders = [d for d in os.listdir(country_path) if os.path.isdir(os.path.join(country_path, d))]
                
                for player_folder in player_folders:
                    player_folder_path = os.path.join(country_path, player_folder)
                    
                    # Folder name format seems to be "Images_<Player Name>"
                    # We'll normalize it for matching
                    # Remove "Images_" prefix if present
                    folder_name_clean = player_folder
                    if folder_name_clean.startswith("Images_"):
                        folder_name_clean = folder_name_clean[7:]
                    
                    # Store images for this player
                    images = [os.path.join(player_folder_path, img) for img in os.listdir(player_folder_path) 
                              if img.lower().endswith(('.png', '.jpg', '.jpeg'))]
                    
                    if images:
                        # Use the clean name as key
                        player_image_map[folder_name_clean.lower()] = images
                        # Also try matching with/without special chars if needed, but let's stick to simple lower case first
        
        print(f"Found images for {len(player_image_map)} players")

        count = 0
        for idx, row in df.iterrows():
            player_name = str(row['Name_Player']).strip()
            
            # Find images for this player
            player_images = []
            
            # Try exact match first
            if player_name.lower() in player_image_map:
                player_images = player_image_map[player_name.lower()]
            else:
                # Try partial match or fuzzy match if needed
                # For now, let's try to see if the player name is contained in any folder key
                for key, imgs in player_image_map.items():
                    if player_name.lower() in key or key in player_name.lower():
                        player_images = imgs
                        break
            
            # Create document text
            doc_text = f"{player_name} football player"
            
            # Tokenize
            tokens = self.preprocessor.preprocess(doc_text)
            
            # Generate a unique doc_id for football (offsetting by a large number or using string ID?)
            # The current system uses int doc_ids. Let's continue that but maybe start from a high number
            # or just append to the existing list.
            # Since process_documents might have already run, we should check the last doc_id
            
            current_doc_id = len(self.documents) # Simple auto-increment
            
            self.documents.append({
                'doc_id': current_doc_id,
                'tokens': tokens,
                'raw_text': doc_text
            })
            
            self.doc_metadata.append({
                'doc_id': current_doc_id,
                'type': 'football',
                'player_name': player_name,
                'image_paths': player_images
            })
            
            count += 1
            
        print(f"Processed {count} football documents")
        return count

    def generate_season_stats(self):
        """
        Generates aggregated statistics for each season (Orange Cap, Purple Cap)
        and adds them as searchable documents.
        """
        print("Generating season stats...")
        
        try:
            df = self.load_dataset()
        except Exception as e:
            print(f"Error loading dataset for stats: {e}")
            return

        if df is None:
            return

        try:
            # Group by season
            seasons = df['season'].unique()
            
            for season in seasons:
                season_df = df[df['season'] == season]
                
                # --- Orange Cap (Top Scorer) ---
                # Max runs per match for each batsman
                match_runs = season_df.groupby(['match_id', 'batsman1_name'])['batsman1_runs'].max().reset_index()
                # Sum runs for the season
                season_runs = match_runs.groupby('batsman1_name')['batsman1_runs'].sum().sort_values(ascending=False)
                
                if not season_runs.empty:
                    top_scorer = season_runs.index[0]
                    runs = season_runs.iloc[0]
                    
                    # Create document for Orange Cap
                    doc_text = f"top scorer of {int(season)} orange cap {top_scorer}"
                    
                    # Tokenize the document
                    tokens = self.preprocessor.preprocess(doc_text)
                    
                    current_doc_id = len(self.documents)
                    
                    self.documents.append({
                        'doc_id': current_doc_id,
                        'tokens': tokens,
                        'raw_text': doc_text
                    })

                    
                    self.doc_metadata.append({
                        'doc_id': current_doc_id,
                        'type': 'season_stats',
                        'season': int(season),
                        'stat': 'orange_cap',
                        'player_name': top_scorer,
                        'value': int(runs),
                        'description': f"Top Scorer (Orange Cap) of {int(season)}: {top_scorer} with {runs} runs"
                    })

                # --- Purple Cap (Top Wicket Taker) ---
                # Max wickets per match for each bowler
                match_wickets = season_df.groupby(['match_id', 'bowler1_name'])['bowler1_wkts'].max().reset_index()
                # Sum wickets for the season
                season_wickets = match_wickets.groupby('bowler1_name')['bowler1_wkts'].sum().sort_values(ascending=False)
                
                if not season_wickets.empty:
                    top_bowler = season_wickets.index[0]
                    wickets = season_wickets.iloc[0]
                    
                    # Create document for Purple Cap
                    doc_text = f"top wicket taker of {int(season)} purple cap {top_bowler}"
                    
                    # Tokenize the document
                    tokens = self.preprocessor.preprocess(doc_text)
                    
                    current_doc_id = len(self.documents)
                    
                    self.documents.append({
                        'doc_id': current_doc_id,
                        'tokens': tokens,
                        'raw_text': doc_text
                    })

                    
                    self.doc_metadata.append({
                        'doc_id': current_doc_id,
                        'type': 'season_stats',
                        'season': int(season),
                        'stat': 'purple_cap',
                        'player_name': top_bowler,
                        'value': int(wickets),
                        'description': f"Top Wicket Taker (Purple Cap) of {int(season)}: {top_bowler} with {wickets} wickets"
                    })
                    
            print(f"Generated season stats for {len(seasons)} seasons")
            
        except Exception as e:
            print(f"Error generating season stats: {e}")

    
    def get_documents(self):
        """Get all processed documents."""
        return self.documents
    
    def get_metadata(self):
        """Get document metadata."""
        return self.doc_metadata

    def save_metadata(self, filepath='metadata.pkl'):
        """
        Save document metadata to a file.
        
        Args:
            filepath: Path to save the metadata
        """
        print(f"Saving metadata to {filepath}...")
        with open(filepath, 'wb') as f:
            pickle.dump(self.doc_metadata, f)
        print(f"Metadata saved successfully ({len(self.doc_metadata)} documents)")

    def load_metadata(self, filepath='metadata.pkl'):
        """
        Load document metadata from a file.
        
        Args:
            filepath: Path to load the metadata from
            
        Returns:
            List of metadata dictionaries
        """
        print(f"Loading metadata from {filepath}...")
        if not os.path.exists(filepath):
            print(f"Metadata file not found: {filepath}")
            return []
            
        with open(filepath, 'rb') as f:
            self.doc_metadata = pickle.load(f)
        print(f"Metadata loaded successfully ({len(self.doc_metadata)} documents)")
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
