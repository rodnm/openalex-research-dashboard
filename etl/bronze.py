import requests
import json
import os
from datetime import datetime
import time

# Configuration
BASE_URL = "https://api.openalex.org/works"
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bronze")

def fetch_works(concept_name="Artificial intelligence", limit=500):
    """
    Fetches works from OpenAlex related to a specific concept.
    """
    print(f"Fetching works for concept: {concept_name}...")
    
    # 1. Get Concept ID (Simple search)
    concept_url = "https://api.openalex.org/concepts"
    concept_res = requests.get(concept_url, params={"search": concept_name})
    if concept_res.status_code != 200 or not concept_res.json()['results']:
        print("Concept not found.")
        return
    
    concept_id = concept_res.json()['results'][0]['id']
    print(f"Found Concept ID: {concept_id}")

    # 2. Fetch Works
    params = {
        "filter": f"concepts.id:{concept_id},from_publication_date:2023-01-01",
        "per-page": 200,
        "sort": "cited_by_count:desc"
    }
    
    works = []
    cursor = "*"
    
    while len(works) < limit:
        params['cursor'] = cursor
        res = requests.get(BASE_URL, params=params)
        if res.status_code != 200:
            print(f"Error fetching works: {res.status_code}")
            break
            
        data = res.json()
        results = data.get('results', [])
        if not results:
            break
            
        works.extend(results)
        cursor = data['meta']['next_cursor']
        print(f"Fetched {len(works)} works...")
        time.sleep(0.5) # Be nice to the API

    import uuid
    # 3. Save to Bronze
    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    filename = f"works_{concept_name.replace(' ', '_')}_{timestamp}_{unique_id}.json"
    filepath = os.path.join(DATA_DIR, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(works[:limit], f, ensure_ascii=False, indent=2)
        
    print(f"Saved {len(works[:limit])} works to {filepath}")

if __name__ == "__main__":
    fetch_works()
