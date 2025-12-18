import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [isSearching, setIsSearching] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [activeTab, setActiveTab] = useState('all'); // 'all' or 'images'
  const [selectedImage, setSelectedImage] = useState(null);
  const [searchMode, setSearchMode] = useState('quick'); // 'quick', 'deep', 'research'
  const [searchTime, setSearchTime] = useState('');
  
  // Debounce autocomplete
  useEffect(() => {
    const delayDebounceFn = setTimeout(() => {
      if (query.length >= 2) {
        fetchSuggestions(query);
      } else {
        setSuggestions([]);
      }
    }, 300);

    return () => clearTimeout(delayDebounceFn);
  }, [query]);

  const fetchSuggestions = async (text) => {
    try {
      const res = await axios.get(`${API_URL}/autocomplete?q=${text}`);
      setSuggestions(res.data.suggestions || []);
    } catch (err) {
      console.error("Autocomplete error:", err);
    }
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!query.trim()) return;

    setIsSearching(true);
    setSuggestions([]); // Hide suggestions
    setActiveTab('all'); // Reset tab
    try {
      const res = await axios.get(`${API_URL}/search?q=${query}`);
      setResults(res.data.results || []);
      setSearchTime(res.data.time_taken);
    } catch (err) {
      console.error("Search error:", err);
      alert("Backend not connected. Please ensure the backend server is running on port 8000.");
    }
  };

  const handleSuggestionClick = (suggestion) => {
    setQuery(suggestion);
    setSuggestions([]);
    setActiveTab('all');
    // Trigger search immediately
    axios.get(`${API_URL}/search?q=${suggestion}`)
      .then(res => setResults(res.data.results || []))
      .catch(err => console.error(err));
    setIsSearching(true);
  };

  const getPlayerMessage = (type) => {
    if (type === 'football') return "A Professional Football Player";
    if (type.includes('player') || type === 'season_stats') return "A Professional Cricketer";
    return "Search Result";
  };

  return (
    <div className="app">
      <div className="app-background"></div>
      
      <div className="container">
        {/* Header / Hero */}
        <header className={`hero ${isSearching ? 'shrunk' : ''}`}>
          <div className="hero-content">
            <h1 className="title">SPORTS SEARCH ENGINE</h1>
            
            <div className="search-wrapper">
              <div className="search-container">
                <form onSubmit={handleSearch}>
                  <div className="search-input-wrapper">
                    <span className="search-icon">🔍</span>
                    <input 
                      type="text" 
                      className="search-input" 
                      placeholder="Ask anything..." 
                      value={query}
                      onChange={(e) => setQuery(e.target.value)}
                    />
                    {query && (
                      <button type="button" className="clear-btn" onClick={() => setQuery('')}>✕</button>
                    )}
                  </div>
                </form>
                
                {suggestions.length > 0 && (
                  <ul className="suggestions-list glass">
                    {suggestions.map((s, idx) => (
                      <li key={idx} className="suggestion-item" onClick={() => handleSuggestionClick(s)}>
                        {s}
                      </li>
                    ))}
                  </ul>
                )}
              </div>

              <div className="search-modes">
                <button 
                  className={`mode-btn ${searchMode === 'quick' ? 'active' : ''}`}
                  onClick={() => setSearchMode('quick')}
                >
                  ⚡ Quick Search
                </button>
                <button 
                  className={`mode-btn ${searchMode === 'deep' ? 'active' : ''}`}
                  onClick={() => setSearchMode('deep')}
                >
                  🧠 Deep Thinking
                </button>
                <button 
                  className={`mode-btn ${searchMode === 'research' ? 'active' : ''}`}
                  onClick={() => setSearchMode('research')}
                >
                  📚 Research
                </button>
              </div>
            </div>
            
            {!isSearching && (
               <div className="hero-actions">
                 <button className="btn btn-secondary" onClick={() => setShowModal(true)}>
                   + Add Document
                 </button>
               </div>
            )}
          </div>
        </header>

        {/* Results Section */}
        {isSearching && (
          <main className="results-section">
            <div style={{display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem'}}>
              <h2 style={{color: 'var(--text-secondary)'}}>
                Found {results.length} results {searchTime && <span style={{fontSize: '0.9rem', opacity: 0.7}}>({searchTime})</span>}
              </h2>
              
              {/* Tabs */}
              <div className="tabs">
                <button 
                  className={`btn ${activeTab === 'all' ? 'btn-primary' : 'btn-close'}`}
                  onClick={() => setActiveTab('all')}
                  style={{marginRight: '1rem'}}
                >
                  All
                </button>
                <button 
                  className={`btn ${activeTab === 'images' ? 'btn-primary' : 'btn-close'}`}
                  onClick={() => setActiveTab('images')}
                >
                  Images
                </button>
              </div>

              <button className="btn btn-close" onClick={() => setShowModal(true)}>
                + Add Document
              </button>
            </div>

            {activeTab === 'images' ? (
              <div className="unified-image-grid-container">
                <div className="unified-image-grid">
                  {(() => {
                    // Flatten all images from all results
                    let allImages = [];
                    results.forEach(doc => {
                      if (doc.images && doc.images.length > 0) {
                        doc.images.forEach(img => {
                          allImages.push({
                            url: img,
                            title: doc.title,
                            type: doc.type
                          });
                        });
                      }
                    });

                    // Shuffle images (Fisher-Yates shuffle)
                    for (let i = allImages.length - 1; i > 0; i--) {
                      const j = Math.floor(Math.random() * (i + 1));
                      [allImages[i], allImages[j]] = [allImages[j], allImages[i]];
                    }

                    if (allImages.length === 0) {
                      return <div style={{color: '#ccc', textAlign: 'center', width: '100%'}}>No images found.</div>;
                    }

                    return allImages.map((img, idx) => (
                      <div key={idx} className="image-grid-item" onClick={() => setSelectedImage(img)}>
                        <img 
                          src={`${API_URL}${img.url}`} 
                          alt={img.title} 
                          className="grid-image"
                          loading="lazy"
                        />
                        <div className="image-overlay">
                          <span>{img.title}</span>
                        </div>
                      </div>
                    ));
                  })()}
                </div>
              </div>
            ) : (

              <div className="results-grid">
                {results.map((doc, idx) => {
                  // ... existing card logic for 'all' tab ...
                  return (
                    <div key={idx} className="glass glass-card card-content">
                      <div style={{marginBottom: '0.5rem', fontWeight: 'bold', color: '#4facfe'}}>
                        {getPlayerMessage(doc.type)}
                      </div>
                      
                      <h3 className="card-title">
                        {doc.title}
                      </h3>
                      
                      {/* Show limited images in All tab */}
                      {doc.images && doc.images.length > 0 && (
                        <div className="image-gallery">
                          {doc.images.slice(0, 2).map((imgUrl, imgIdx) => (
                            <img 
                              key={imgIdx} 
                              src={`${API_URL}${imgUrl}`} 
                              alt={doc.title} 
                              className="player-image"
                              style={{
                                width: '150px',
                                height: '150px',
                                objectFit: 'cover',
                                borderRadius: '8px',
                                margin: '0.5rem'
                              }}
                            />
                          ))}
                        </div>
                      )}

                      {/* Content only visible in All tab */}
                      {/* Dynamic Content based on type */}
                      {doc.type === 'player_career_stats' && (
                        <div className="stats-grid">
                          <div className="stat-row">
                            <span className="stat-label">Runs</span>
                            <span className="stat-value">{doc.metadata?.stats?.runs}</span>
                          </div>
                          <div className="stat-row">
                            <span className="stat-label">Wickets</span>
                            <span className="stat-value">{doc.metadata?.stats?.wickets}</span>
                          </div>
                          <div className="stat-row">
                            <span className="stat-label">Centuries</span>
                            <span className="stat-value">{doc.metadata?.stats?.centuries}</span>
                          </div>
                        </div>
                      )}

                      {doc.type === 'player_season_stats' && (
                        <div className="stats-grid">
                          <div className="stat-row">
                            <span className="stat-label">Season</span>
                            <span className="stat-value">{doc.metadata?.season}</span>
                          </div>
                          {doc.metadata?.runs > 0 && (
                            <div className="stat-row">
                              <span className="stat-label">Runs</span>
                              <span className="stat-value">{doc.metadata?.runs}</span>
                            </div>
                          )}
                          {doc.metadata?.wickets > 0 && (
                            <div className="stat-row">
                              <span className="stat-label">Wickets</span>
                              <span className="stat-value">{doc.metadata?.wickets}</span>
                            </div>
                          )}
                        </div>
                      )}

                      {doc.type === 'football' && (
                        <div className="football-gallery-all">
                          {doc.images && doc.images.length > 0 && (
                            <div className="football-gallery-grid">
                              {doc.images.slice(0, 6).map((imgUrl, i) => (
                                <div key={i} className="football-gallery-item">
                                  <img 
                                    src={`${API_URL}${imgUrl}`} 
                                    alt={`${doc.title} ${i}`}
                                    className="football-gallery-img"
                                    onError={(e) => e.target.style.display = 'none'}
                                  />
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                      
                      <p style={{marginTop: '1rem', color: '#ccc', fontSize: '0.9rem'}}>
                        {doc.description || doc.metadata?.raw_text?.substring(0, 100) + '...'}
                      </p>
                    </div>
                  );
                })}
              </div>
            )}
          </main>
        )}
      </div>

      {/* Add Document Modal */}


      {showModal && (
        <div className="modal-overlay">
          <div className="glass modal-content">
            <h2 className="card-title">Add New Document</h2>
            <form onSubmit={(e) => {
              e.preventDefault();
              // Implement add logic here
              alert("Document addition implemented in backend, connect UI here.");
              setShowModal(false);
            }}>
              <div className="form-group">
                <label className="form-label">JSON Data</label>
                <textarea className="form-textarea" rows="10" placeholder='{"text": "..."}'></textarea>
              </div>
              <div style={{display: 'flex', justifyContent: 'flex-end'}}>
                <button type="button" className="btn btn-close" onClick={() => setShowModal(false)}>Cancel</button>
                <button type="submit" className="btn btn-primary">Add</button>
              </div>
            </form>
          </div>
        </div>
      )}
      {selectedImage && (
        <div className="lightbox-overlay" onClick={() => setSelectedImage(null)}>
          <button className="lightbox-close" onClick={() => setSelectedImage(null)}>&times;</button>
          <img 
            src={`${API_URL}${selectedImage.url}`} 
            alt={selectedImage.title} 
            className="lightbox-image"
            onClick={(e) => e.stopPropagation()} // Prevent closing when clicking image
          />
          <div style={{position: 'absolute', bottom: '20px', color: 'white', fontSize: '1.2rem', textShadow: '0 2px 4px black'}}>
            {selectedImage.title}
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
