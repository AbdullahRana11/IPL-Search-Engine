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

## Project Structure

```
IPL-Search-Engine/
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