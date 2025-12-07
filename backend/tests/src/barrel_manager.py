"""
Barrel Manager Module
Handles partitioning of the inverted index into smaller chunks (barrels).
"""

import os
import pickle
from collections import defaultdict


class BarrelManager:
    """Manages creation, storage, and loading of index barrels."""
    
    def __init__(self, output_dir='barrels', barrel_size=2500):
        """
        Initialize the barrel manager.
        
        Args:
            output_dir: Directory to store barrel files
            barrel_size: Number of words per barrel (approximate)
        """
        self.output_dir = output_dir
        self.barrel_size = barrel_size
        self.barrels_buffer = defaultdict(lambda: defaultdict(list))
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
    def get_barrel_id(self, word_id):
        """
        Determine which barrel a word belongs to.
        
        Args:
            word_id: The unique ID of the word
            
        Returns:
            Barrel ID (integer)
        """
        # Simple range-based partitioning
        # Barrel 0: 0 - 2499
        # Barrel 1: 2500 - 4999
        # etc.
        return word_id // self.barrel_size
    
    def add_to_barrel(self, word_id, doc_id):
        """
        Add a word occurrence to the appropriate barrel buffer.
        
        Args:
            word_id: Word ID
            doc_id: Document ID
        """
        barrel_id = self.get_barrel_id(word_id)
        self.barrels_buffer[barrel_id][word_id].append(doc_id)
        
    def flush_barrels(self):
        """
        Write all buffered barrel data to disk.
        Existing barrel files are updated (merged).
        """
        print(f"Flushing {len(self.barrels_buffer)} barrels to disk...")
        
        for barrel_id, new_data in self.barrels_buffer.items():
            barrel_path = os.path.join(self.output_dir, f"barrel_{barrel_id}.pkl")
            
            # Load existing data if file exists
            current_data = {}
            if os.path.exists(barrel_path):
                try:
                    with open(barrel_path, 'rb') as f:
                        current_data = pickle.load(f)
                except Exception as e:
                    print(f"Warning: Could not load barrel {barrel_id}: {e}")
            
            # Merge new data
            for word_id, doc_ids in new_data.items():
                if word_id in current_data:
                    # Append new doc_ids and remove duplicates
                    current_data[word_id] = list(set(current_data[word_id] + doc_ids))
                else:
                    current_data[word_id] = list(set(doc_ids))
            
            # Save back to file
            with open(barrel_path, 'wb') as f:
                pickle.dump(current_data, f)
        
        # Clear buffer
        self.barrels_buffer.clear()
        print("Barrels flushed successfully.")

    def load_barrel(self, barrel_id):
        """
        Load a specific barrel into memory.
        
        Args:
            barrel_id: ID of the barrel to load
            
        Returns:
            Dictionary mapping word_id -> list of doc_ids, or empty dict if not found
        """
        barrel_path = os.path.join(self.output_dir, f"barrel_{barrel_id}.pkl")
        
        if not os.path.exists(barrel_path):
            return {}
            
        try:
            with open(barrel_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"Error loading barrel {barrel_id}: {e}")
            return {}

    def get_documents_for_word(self, word_id):
        """
        Get documents for a specific word by loading the appropriate barrel.
        
        Args:
            word_id: Word ID to look up
            
        Returns:
            List of document IDs
        """
        barrel_id = self.get_barrel_id(word_id)
        barrel_data = self.load_barrel(barrel_id)
        return barrel_data.get(word_id, [])
