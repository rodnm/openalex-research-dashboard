import json
import os
import pandas as pd
import glob

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

def process_bronze_to_silver():
    """
    Reads JSON files from Bronze, flattens them, and saves to Silver as Parquet.
    """
    print("Processing Bronze to Silver...")
    
    # Find all bronze files
    files = glob.glob(os.path.join(BRONZE_DIR, "*.json"))
    if not files:
        print("No bronze files found.")
        return
    
    print(f"Found {len(files)} files to process.")
    
    all_works = []
    
    for file_path in files:
        print(f"Processing file: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                works = json.load(f)
                if isinstance(works, list):
                    all_works.extend(works)
                else:
                    print(f"Skipping {file_path}: Content is not a list.")
        except json.JSONDecodeError as e:
            print(f"Skipping {file_path}: JSON Decode Error - {e}")
        except Exception as e:
            print(f"Skipping {file_path}: Error - {e}")
            
    processed_works = []
    
    for work in all_works:
        # Extract basic info
        work_id = work.get('id')
        title = work.get('title')
        pub_year = work.get('publication_year')
        cited_by = work.get('cited_by_count')
        
        # Extract primary topic
        primary_topic = work.get('primary_topic') or {}
        topic_name = primary_topic.get('display_name') if primary_topic else None
        
        # Extract Domain and Field (Hierarchy)
        domain_info = primary_topic.get('domain', {})
        domain_name = domain_info.get('display_name') if domain_info else None
        
        field_info = primary_topic.get('field', {})
        field_name = field_info.get('display_name') if field_info else None
        
        # Extract authors (flattened)
        authorships = work.get('authorships', [])
        for auth in authorships:
            author = auth.get('author', {})
            insts = auth.get('institutions', [])
            inst_name = insts[0].get('display_name') if insts else None
            
            processed_works.append({
                'work_id': work_id,
                'title': title,
                'publication_year': pub_year,
                'cited_by_count': cited_by,
                'topic': topic_name,
                'domain': domain_name,
                'field': field_name,
                'author_name': author.get('display_name'),
                'author_id': author.get('id'),
                'institution': inst_name
            })
            
    if not processed_works:
        print("No data extracted.")
        return

    df = pd.DataFrame(processed_works)
    
    # Data Cleaning
    df['publication_year'] = pd.to_numeric(df['publication_year'], errors='coerce').fillna(0).astype(int)
    df['cited_by_count'] = pd.to_numeric(df['cited_by_count'], errors='coerce').fillna(0).astype(int)
    df.dropna(subset=['title'], inplace=True)
    
    # Save to Silver
    os.makedirs(SILVER_DIR, exist_ok=True)
    output_path = os.path.join(SILVER_DIR, "works_flat.parquet")
    df.to_parquet(output_path, index=False)
    print(f"Saved {len(df)} rows to {output_path}")

if __name__ == "__main__":
    process_bronze_to_silver()
