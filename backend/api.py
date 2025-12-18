"""
IPL Search Engine API
FastAPI application to serve the search engine and autocomplete.
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import os
import time
import random
from src.search_engine import SearchEngine
from src.autocomplete import Autocomplete

app = FastAPI(title="IPL Search Engine API")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Dataset directory for serving images
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(BASE_DIR, 'Dataset')

if os.path.exists(DATASET_DIR):
    app.mount("/dataset", StaticFiles(directory=DATASET_DIR), name="dataset")
else:
    print(f"Warning: Dataset directory not found at {DATASET_DIR}")

from src.document_adder import DocumentAdder

# Global instances
search_engine = None
autocomplete = None
document_adder = None

@app.on_event("startup")
async def startup_event():
    """Initialize search engine and autocomplete on startup."""
    global search_engine, autocomplete, document_adder
    
    print("Initializing API...")
    
    # Initialize Search Engine
    # Index data is now inside the backend directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    search_engine = SearchEngine(output_dir=base_dir)
    
    # Initialize Autocomplete
    autocomplete = Autocomplete()
    if search_engine.lexicon:
        print(f"Building autocomplete from {len(search_engine.lexicon)} words...")
        autocomplete.build_from_lexicon(search_engine.lexicon)
        
    # Initialize Document Adder
    document_adder = DocumentAdder(search_engine)
    
    print("API Initialized successfully.")

@app.get("/")
def read_root():
    return {"status": "online", "name": "IPL Search Engine API"}

def get_image_url(path):
    """Convert absolute file path to dataset URL."""
    if not path:
        return None
    try:
        # Normalize paths for comparison
        abs_path = os.path.abspath(path)
        abs_dataset_dir = os.path.abspath(DATASET_DIR)
        
        # Check if path is within DATASET_DIR (case-insensitive for Windows)
        if abs_path.lower().startswith(abs_dataset_dir.lower()):
            rel_path = os.path.relpath(abs_path, abs_dataset_dir)
            return f"/dataset/{rel_path.replace(os.sep, '/')}"
        return None
    except Exception as e:
        print(f"Error converting image path: {e}")
        return None

@app.get("/search")
async def search(q: str = Query(..., min_length=1), limit: int = 50):
    """
    Search endpoint.
    Supports single word, multi-word, and combined queries.
    """
    start_time = time.time()
    
    if not search_engine:
        return {"error": "Search engine not initialized"}
        
    # Determine search type and pass limit
    if ' and ' in q.lower():
        results = search_engine.search_combined(q, max_results=limit)
    elif ' ' in q.strip():
        results = search_engine.search_multi(q, max_results=limit)
    else:
        results = search_engine.search_single(q, max_results=limit)
        
    elapsed_time = time.time() - start_time
    
    # Format results
    formatted_results = []
    for doc in results[:limit]:
        # Process images
        image_paths = doc.get('image_paths', [])
        image_urls = []
        if image_paths:
            # Convert paths to URLs
            for p in image_paths:
                url = get_image_url(p)
                if url:
                    image_urls.append(url)
            
            # Shuffle images for "random order every time"
            random.shuffle(image_urls)
            
        formatted_results.append({
            "doc_id": doc.get('doc_id'),
            "title": doc.get('player_name') or doc.get('match_name') or "Document",
            "type": doc.get('type', 'unknown'),
            "description": doc.get('description') or doc.get('raw_text', '')[:200],
            "score": doc.get('score', 0),
            "images": image_urls, # List of all image URLs (shuffled)
            "metadata": doc
        })
        
    return {
        "query": q,
        "count": len(results),
        "time_taken": f"{elapsed_time:.4f}s",
        "results": formatted_results
    }

@app.get("/autocomplete")
async def get_autocomplete(q: str = Query(..., min_length=1)):
    """
    Autocomplete endpoint.
    Returns suggestions for the given prefix.
    """
    if not autocomplete:
        return []
        
    suggestions = autocomplete.search(q, limit=5)
    return {"suggestions": suggestions}

from pydantic import BaseModel
from typing import Optional, Dict, Any

class DocumentInput(BaseModel):
    text: str
    metadata: Optional[Dict[str, Any]] = {}

@app.post("/add-document")
async def add_document(doc: DocumentInput):
    """
    Add a new document to the index.
    """
    if not document_adder:
        return {"error": "Document adder not initialized"}
        
    try:
        doc_id = document_adder.add_document(doc.dict())
        
        # Update autocomplete with new words
        # Simple approach: just re-add words from text
        # Ideally we'd only add new words
        for word in doc.text.split():
            autocomplete.insert(word)
            
        return {"status": "success", "doc_id": doc_id, "message": "Document indexed successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
