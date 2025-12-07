<<<<<<< HEAD
# IPL Search Engine

A comprehensive search engine for Indian Premier League (IPL) cricket data.

## Project Overview

This project implements a search engine for IPL match data using custom indexing techniques including lexicon, forward index, and inverted index.

**Dataset:** IPL ball-by-ball data (242,550+ records)

## Current Features

### Phase 1: Core Indexing (Completed)
- ✅ Text preprocessing and tokenization
- ✅ Document processing from CSV data
- ✅ Lexicon generation (word-to-ID mapping)
- ✅ Forward index (document-to-words mapping)
- ✅ Inverted index (word-to-documents mapping)
- ✅ Barrel-based storage for scalability
=======
# IPL-Search-Engine

A search engine for IPL (Indian Premier League) cricket data with support for FIFA World Cup 2022 football dataset integration.

## Description

This project implements a custom search engine using inverted index, lexicon, and barrel-based architecture to efficiently search through IPL match data and football player information. The system includes advanced features like season statistics aggregation and combined multi-dataset search capabilities.

## Features

- Full-text search across IPL match data (2008-2023)
- Football player search with image support (FIFA WC 2022)
- Season statistics search (Orange Cap, Purple Cap)
- Barrel-based inverted index for scalability
- Combined search across multiple datasets
- Text preprocessing and tokenization
- TF-IDF based ranking

## Installation

### Prerequisites

- Python 3.8 or higher
- pip package manager

### Setup

1. Clone the repository
```bash
git clone https://github.com/AbdullahRana11/IPL-Search-Engine.git
cd IPL-Search-Engine
```

2. Install required dependencies
```bash
pip install pandas
```

3. Prepare datasets
- Place IPL dataset at: `Dataset/IPL/all_season_details.csv`
- Place Football dataset at: `Dataset/Football 2/`

## Usage

### Building the Index

Run the main script to build all indices:

```bash
python backend/main.py
```

This will generate:
- Lexicon
- Forward Index
- Inverted Index (split into barrels)
- Metadata

### Searching

Use the search script with your query:

```bash
python backend/search.py "your search query"
```

**Example queries:**

```bash
# Search for a player
python backend/search.py "Virat Kohli"

# Search for season stats
python backend/search.py "top scorer of 2022"

# Combined search
python backend/search.py "top scorer of 2022 and Ronaldo"
```
>>>>>>> 5a3f425f1f8bce65ad0065cf707bb96f31d05e3b

## Project Structure

```
IPL-Search-Engine/
<<<<<<< HEAD
├── Dataset/
│   └── IPL/
│       └── all_season_details.csv    # 242k+ ball-by-ball records
├── src/                               # Core Engine Code
│   ├── preprocessor.py                # Text cleaning and tokenization
│   ├── document_processor.py          # CSV parsing and document creation
│   ├── lexicon_builder.py             # Lexicon generation
│   ├── forward_index_builder.py       # Forward index generation
│   ├── inverted_index_builder.py      # Inverted index generation
│   └── barrel_manager.py              # Barrel management
├── tests/                             # Test Suite
│   ├── test_indexing.py               # Pipeline tests
│   └── test_barrels.py                # Barrel tests
├── main.py                            # Main indexing pipeline
└── README.md                          # Project documentation
```

## Installation

```bash
# Install required dependencies
pip install pandas
```

## Usage

### Build Indices

Run the complete indexing pipeline:

```bash
python main.py
```

This will create three separate directories for index data (simulating distributed storage):
- `lexicon_data/lexicon.pkl`
- `forward_index_data/forward_index.pkl`
- `inverted_index_data/barrel_*.pkl`

### Run Tests

Test the indexing system:

```bash
python tests/test_indexing.py
```

## Development Team

- Abdullah Rana
- Huzaifa Sohail

---

*Course Project: CS-250 Data Structures and Algorithms*  
*SEECS, NUST*
=======
├── backend/
│   ├── src/
│   │   ├── document_processor.py    # Document processing and indexing
│   │   ├── lexicon_builder.py       # Lexicon construction
│   │   ├── forward_index_builder.py # Forward index creation
│   │   ├── inverted_index_builder.py# Inverted index with barrels
│   │   ├── barrel_manager.py        # Barrel management
│   │   ├── search_engine.py         # Search logic
│   │   └── preprocessor.py          # Text preprocessing
│   ├── tests/                       # Test files
│   ├── main.py                      # Index building pipeline
│   └── search.py                    # Search CLI
├── Dataset/                         # Datasets (IPL and Football)
└── README.md
```

## Architecture

The search engine uses a barrel-based inverted index architecture:

1. **Document Processing**: Processes IPL match data and football player data
2. **Lexicon**: Maps words to unique word IDs
3. **Forward Index**: Maps documents to their word lists
4. **Inverted Index**: Maps words to documents (split into barrels)
5. **Barrels**: Chunks of inverted index for scalability
6. **Search Engine**: Retrieves and ranks results using TF-IDF

## Testing

Run tests using:

```bash
python backend/tests/test_barrels.py
python backend/tests/test_indexing.py
```

## Verifying Barrels Implementation

To verify the barrel implementation:

1. Check for multiple barrel files:
```bash
dir inverted_index_data
```

You should see files like `barrel_0.pkl`, `barrel_1.pkl`, etc.

2. Look for barrel messages during indexing:
```
Flushing 9 barrels to disk...
Barrels flushed successfully.
```

## Contributors

- **Huzaifa Sohail** - Backend implementation, document processing, season stats
- **Abdullah Rana** - Search engine logic, combined search, documentation

## Requirements

- pandas

## License

This project is licensed under the MIT License.

## Acknowledgments

- IPL dataset source: ESPN Cricinfo
- Football dataset: FIFA World Cup 2022
>>>>>>> 5a3f425f1f8bce65ad0065cf707bb96f31d05e3b
