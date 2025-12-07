"""
Text Preprocessing Module
Handles text cleaning, tokenization, and normalization for the IPL Search Engine.
"""

import re
import string

class TextPreprocessor:
    """Preprocesses text for indexing."""
    
    def __init__(self, remove_stopwords=False):
        """
        Initialize the preprocessor.
        
        Args:
            remove_stopwords: Whether to remove common stop words
        """
        self.remove_stopwords = remove_stopwords
        
        # Common English stop words (small set for now)
        self.stopwords = {
            'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 
            'from', 'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 
            'that', 'the', 'to', 'was', 'will', 'with'
        }
    
    def clean_text(self, text):
        """
        Clean and normalize text.
        
        Args:
            text: Input text string
            
        Returns:
            Cleaned text string
        """
        if not text or not isinstance(text, str):
            return ""
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters but keep spaces
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text
    
    def tokenize(self, text):
        """
        Split text into individual words (tokens).
        
        Args:
            text: Input text string
            
        Returns:
            List of word tokens
        """
        # Clean the text first
        cleaned_text = self.clean_text(text)
        
        if not cleaned_text:
            return []
        
        # Split by whitespace
        tokens = cleaned_text.split()
        
        # Remove stop words if enabled
        if self.remove_stopwords:
            tokens = [word for word in tokens if word not in self.stopwords]
        
        # Filter out very short tokens (single characters)
        tokens = [word for word in tokens if len(word) > 1]
        
        return tokens
    
    def preprocess(self, text):
        """
        Full preprocessing pipeline: clean and tokenize.
        
        Args:
            text: Input text string
            
        Returns:
            List of processed word tokens
        """
        return self.tokenize(text)


# Test the preprocessor
if __name__ == "__main__":
    preprocessor = TextPreprocessor(remove_stopwords=False)
    
    test_texts = [
        "Mumbai Indians vs Chennai Super Kings",
        "Virat Kohli scored 100 runs!",
        "MS Dhoni's winning six in IPL 2023"
    ]
    
    print("Testing Text Preprocessor:")
    print("-" * 50)
    for text in test_texts:
        tokens = preprocessor.preprocess(text)
        print(f"Original: {text}")
        print(f"Tokens: {tokens}")
        print()
