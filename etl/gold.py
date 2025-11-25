import os
import pandas as pd

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")

def process_silver_to_gold():
    """
    Reads Parquet from Silver, aggregates metrics, and saves to Gold.
    """
    print("Processing Silver to Gold...")
    
    input_path = os.path.join(SILVER_DIR, "works_flat.parquet")
    if not os.path.exists(input_path):
        print("Silver data not found.")
        return
        
    df = pd.read_parquet(input_path)
    
    os.makedirs(GOLD_DIR, exist_ok=True)
    
    # 1. Yearly Trends
    yearly_counts = df.groupby(['domain', 'field', 'topic', 'publication_year']).agg(
        total_works=('work_id', 'nunique'),
        total_citations=('cited_by_count', 'sum')
    ).reset_index().sort_values(['domain', 'field', 'topic', 'publication_year'])
    
    yearly_counts.to_parquet(os.path.join(GOLD_DIR, "yearly_trends.parquet"))
    
    # 2. Top Authors
    top_authors = df.groupby(['domain', 'field', 'topic', 'author_name']).agg(
        total_works=('work_id', 'nunique'),
        total_citations=('cited_by_count', 'sum')
    ).reset_index().sort_values('total_citations', ascending=False)
    
    top_authors.to_parquet(os.path.join(GOLD_DIR, "top_authors.parquet"))
    
    # 3. Top Institutions
    top_institutions = df.groupby(['domain', 'field', 'topic', 'institution']).agg(
        total_works=('work_id', 'nunique'),
        total_citations=('cited_by_count', 'sum')
    ).reset_index().sort_values('total_works', ascending=False)
    
    top_institutions.to_parquet(os.path.join(GOLD_DIR, "top_institutions.parquet"))

    # 4. Top Works (Articles)
    # Aggregate institutions per work before deduplicating
    unique_works = df.groupby('work_id').agg({
        'title': 'first',
        'cited_by_count': 'first',
        'publication_year': 'first',
        'topic': 'first',
        'domain': 'first',
        'field': 'first',
        'institution': lambda x: list(set(x.dropna()))
    }).reset_index()
    
    # Get top 10 per topic
    top_works = unique_works.sort_values('cited_by_count', ascending=False).groupby('topic').head(10)
    top_works = top_works[['domain', 'field', 'topic', 'title', 'cited_by_count', 'publication_year', 'institution']]
    
    top_works.to_parquet(os.path.join(GOLD_DIR, "top_works.parquet"))
    
    # 5. Master Table (for Dynamic Dashboard)
    # Save the full flattened dataframe to allow dynamic filtering in Streamlit
    df.to_parquet(os.path.join(GOLD_DIR, "all_works.parquet"))
    
    print("Gold layer aggregations completed.")

if __name__ == "__main__":
    process_silver_to_gold()
