import pickle

def inspect_metadata():
    try:
        with open('metadata_data/metadata.pkl', 'rb') as f:
            metadata = pickle.load(f)
            
        print(f"Total documents: {len(metadata)}")
        print("\nLast 10 documents:")
        for doc in metadata[-10:]:
            print(doc)
            print()
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect_metadata()
