"""
Autocomplete Module
Handles real-time query suggestions using a Trie data structure.
"""

import pickle
import os

class TrieNode:
    """Node in the Trie."""
    def __init__(self):
        self.children = {}
        self.is_end = False
        self.frequency = 0  # To rank suggestions

class Autocomplete:
    """
    Autocomplete engine using a Trie.
    """
    
    def __init__(self):
        self.root = TrieNode()
        
    def insert(self, word, frequency=1):
        """
        Insert a word into the Trie.
        
        Args:
            word: Word to insert
            frequency: Frequency of the word (for ranking)
        """
        node = self.root
        for char in word.lower():
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.is_end = True
        node.frequency = frequency
        
    def build_from_lexicon(self, lexicon):
        """
        Build Trie from a lexicon dictionary.
        
        Args:
            lexicon: Dictionary mapping word -> word_id (or frequency)
        """
        print(f"Building autocomplete trie from {len(lexicon)} words...")
        for word in lexicon:
            # If lexicon is just word->id, we assume freq=1 or need another source
            # For now, we'll just insert. 
            # Ideally we'd have frequency data.
            self.insert(word)
            
    def search(self, prefix, limit=5):
        """
        Get suggestions for a prefix.
        
        Args:
            prefix: Prefix string
            limit: Max number of suggestions
            
        Returns:
            List of suggested words
        """
        node = self.root
        prefix = prefix.lower()
        
        # Traverse to the end of prefix
        for char in prefix:
            if char not in node.children:
                return []
            node = node.children[char]
            
        # DFS to find all words with this prefix
        suggestions = []
        self._dfs(node, prefix, suggestions, limit)
        
        # Sort by length (shorter first) as a simple heuristic if no frequency
        # Or if we had frequency, sort by that.
        suggestions.sort(key=len)
        
        return suggestions[:limit]
    
    def _dfs(self, node, current_word, suggestions, limit):
        """Depth-first search to find words."""
        if len(suggestions) >= limit * 2: # Optimization: stop if we have enough
            return
            
        if node.is_end:
            suggestions.append(current_word)
            
        for char, child_node in node.children.items():
            self._dfs(child_node, current_word + char, suggestions, limit)

    def save(self, filepath):
        """Save Trie to file."""
        with open(filepath, 'wb') as f:
            pickle.dump(self.root, f)
            
    def load(self, filepath):
        """Load Trie from file."""
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                self.root = pickle.load(f)
            return True
        return False
