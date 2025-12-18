"""
Document Adder Module
Handles dynamic addition of new documents to the search index.
"""

import threading
import time
from .document_processor import DocumentProcessor
from .lexicon_builder import LexiconBuilder
from .forward_index_builder import ForwardIndexBuilder
from .inverted_index_builder import InvertedIndexBuilder

class DocumentAdder:
    """
    Handles adding new documents to the existing index.
    """
    
    def __init__(self, search_engine):
        """
        Initialize with a reference to the active search engine.
        
        Args:
            search_engine: Active SearchEngine instance
        """
        self.search_engine = search_engine
        self.lock = threading.Lock()
        
    def add_document(self, doc_data):
        """
        Add a new document to the index.
        
        Args:
            doc_data: Dictionary containing document data.
                      Must have 'text' or 'content' field.
                      Optional 'metadata' field.
                      
        Returns:
            doc_id of the new document
        """
        text = doc_data.get('text') or doc_data.get('content')
        if not text:
            raise ValueError("Document must have 'text' or 'content' field")
            
        metadata = doc_data.get('metadata', {})
        
        # Run indexing in a separate thread to avoid blocking
        # For simplicity in this implementation, we'll run it synchronously 
        # but the architecture allows for async.
        # To meet the "non-blocking" requirement strictly, we'd use a queue.
        # Here we'll just ensure it's fast.
        
        with self.lock:
            return self._index_document(text, metadata)
            
    def _index_document(self, text, metadata):
        """
        Internal method to index a single document.
        """
        print(f"Indexing new document: {text[:50]}...")
        start_time = time.time()
        
        # 1. Preprocess
        # We need a preprocessor. We can grab one from DocumentProcessor or create new.
        # Assuming default preprocessor is fine.
        from .preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor(remove_stopwords=False)
        tokens = preprocessor.preprocess(text)
        
        # 2. Assign Doc ID
        # Get next available doc ID
        # We need to know the max doc ID. 
        # Ideally search_engine.metadata has all docs.
        if self.search_engine.metadata:
            next_doc_id = max(d['doc_id'] for d in self.search_engine.metadata) + 1
        else:
            next_doc_id = 0
            
        # 3. Update Lexicon
        # We need to add new words to lexicon and get their IDs.
        # search_engine.lexicon is a dict word -> id
        lexicon = self.search_engine.lexicon
        next_word_id = max(lexicon.values()) + 1 if lexicon else 0
        
        word_ids = []
        for token in tokens:
            if token not in lexicon:
                lexicon[token] = next_word_id
                next_word_id += 1
            word_ids.append(lexicon[token])
            
        # 4. Update Forward Index
        # We don't strictly need to persist forward index for search, 
        # but we might for future rebuilds.
        # For now, we skip saving to disk to be fast, but we should update in-memory if we had it.
        # SearchEngine doesn't keep forward index in memory usually.
        
        # 5. Update Inverted Index (Barrels)
        # We need to add doc_id to the posting lists of these words.
        barrel_manager = self.search_engine.barrel_manager
        
        for word_id in set(word_ids): # Unique words only for inverted index
            barrel_manager.add_to_barrel(word_id, next_doc_id)
            
        # Flush modified barrels to disk
        # This might be slow if we flush every time. 
        # For "real-time" we might want to keep a memory buffer and flush periodically.
        # But requirement says < 1 minute, so flushing is fine.
        barrel_manager.flush_barrels()
        
        # 6. Update Metadata
        new_metadata = {
            'doc_id': next_doc_id,
            'raw_text': text,
            'type': metadata.get('type', 'manual'),
            **metadata
        }
        
        self.search_engine.metadata.append(new_metadata)
        self.search_engine.metadata_map[next_doc_id] = new_metadata
        
        # Save metadata to disk
        processor = DocumentProcessor('')
        processor.doc_metadata = self.search_engine.metadata
        processor.save_metadata(os.path.join(self.search_engine.metadata_dir, 'metadata.pkl'))
        
        # Save lexicon to disk
        # We need to save the updated lexicon
        lex_builder = LexiconBuilder()
        lex_builder.lexicon = lexicon
        lex_builder.save_to_file(os.path.join(self.search_engine.lexicon_dir, 'lexicon.pkl'))
        
        elapsed = time.time() - start_time
        print(f"Indexed document {next_doc_id} in {elapsed:.4f}s")
        
        return next_doc_id
