import streamlit as st
import pandas as pd
import plotly.express as px
import os
import requests

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/openalex_medallion_etl/dagRuns"
AIRFLOW_AUTH = ('airflow', 'airflow')

st.set_page_config(page_title="OpenAlex Research Dashboard", layout="wide")

st.title("📚 OpenAlex Research Dashboard")
st.markdown("Analyzing research trends, top authors, and institutions across **multiple fields of study**.")

# Load Data
@st.cache_data
def load_data():
    path = os.path.join(GOLD_DIR, "all_works.parquet")
    if not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    
    # Remap 'Arts and Humanities' field to 'Humanities' domain for better visibility
    if 'field' in df.columns and 'domain' in df.columns:
        mask = df['field'] == 'Arts and Humanities'
        # Use .loc to avoid SettingWithCopyWarning
        df.loc[mask, 'domain'] = 'Humanities'
        
    return df

df = load_data()

# Sidebar
with st.sidebar:
    st.header("⚙️ Filters & Options")
    
    if df is not None:
        # 1. Domain Filter (General Discipline)
        # Filter out None values
        unique_domains = [d for d in df['domain'].unique().tolist() if d is not None]
        available_domains = sorted(unique_domains)
        available_domains.insert(0, "All")
        selected_domain = st.selectbox("General Discipline", available_domains)

        # 2. Topic Filter (Field of Study) - Filtered by Domain
        if selected_domain != "All":
            filtered_topics_df = df[df['domain'] == selected_domain]
            unique_topics = [t for t in filtered_topics_df['topic'].unique().tolist() if t is not None]
        else:
            unique_topics = [t for t in df['topic'].unique().tolist() if t is not None]
            
        available_topics = sorted(unique_topics)
        available_topics.insert(0, "All")
        selected_topic = st.selectbox("Field of Study", available_topics)
        
        # 3. Year Filter
        min_year = int(df['publication_year'].min())
        max_year = int(df['publication_year'].max())
        selected_years = st.slider("Publication Year", min_year, max_year, (min_year, max_year))
        
        # 4. Institution Filter
        all_insts = sorted(df['institution'].dropna().unique().tolist())
        selected_insts = st.multiselect(
            "Filter by Institution", 
            all_insts,
            format_func=lambda x: x[:50] + "..." if len(x) > 50 else x
        )
    else:
        selected_domain = "All"
        selected_topic = "All"
        selected_years = (2000, 2024)
        selected_insts = []

    st.divider()
    
    # Update Button
    st.subheader("🔄 Data Pipeline")
    if st.button("Trigger ETL Update"):
        try:
            response = requests.post(
                AIRFLOW_API_URL,
                auth=AIRFLOW_AUTH,
                json={"conf": {}}
            )
            if response.status_code == 200:
                st.success("Pipeline triggered successfully! 🚀")
                st.info("Data will refresh automatically once the pipeline completes.")
            else:
                st.error(f"Failed to trigger pipeline: {response.status_code} - {response.text}")
        except Exception as e:
            st.error(f"Connection error: {e}")

if df is None:
    st.warning("⚠️ No data found. Please run the Airflow ETL pipeline first.")
else:
    # --- Dynamic Filtering ---
    filtered_df = df.copy()
    
    # 1. Domain
    if selected_domain != "All":
        filtered_df = filtered_df[filtered_df['domain'] == selected_domain]

    # 2. Topic
    if selected_topic != "All":
        filtered_df = filtered_df[filtered_df['topic'] == selected_topic]
        
    # 3. Year
    filtered_df = filtered_df[
        (filtered_df['publication_year'] >= selected_years[0]) & 
        (filtered_df['publication_year'] <= selected_years[1])
    ]
    
    # 4. Institution
    if selected_insts:
        filtered_df = filtered_df[filtered_df['institution'].isin(selected_insts)]

    # --- Aggregations ---
    
    # Unique works (for works-based metrics)
    unique_works_df = filtered_df.drop_duplicates(subset=['work_id'])
    
    # Metrics
    total_works = len(unique_works_df)
    total_citations = unique_works_df['cited_by_count'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Works Analyzed", f"{total_works:,}")
    col2.metric("Total Citations", f"{total_citations:,}")
    if not unique_works_df.empty:
        col3.metric("Years Covered", f"{unique_works_df['publication_year'].min()} - {unique_works_df['publication_year'].max()}")

    st.divider()

    # Charts
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("📈 Research Growth Over Time")
        # Group unique works by year
        yearly_counts = unique_works_df.groupby('publication_year').size().reset_index(name='total_works')
        fig_growth = px.line(yearly_counts, x='publication_year', y='total_works', markers=True)
        st.plotly_chart(fig_growth, use_container_width=True)
        
    with col_right:
        st.subheader("🏆 Top Authors by Citations")
        # Group filtered_df (includes author info) by author
        top_authors = filtered_df.groupby('author_name').agg(
            total_citations=('cited_by_count', 'sum')
        ).reset_index().sort_values('total_citations', ascending=False).head(20)
        
        fig_authors = px.bar(top_authors, x='total_citations', y='author_name', orientation='h')
        fig_authors.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_authors, use_container_width=True)

    st.subheader("🏛️ Top Institutions by Volume")
    # Group filtered_df by institution
    top_insts = filtered_df.groupby('institution').size().reset_index(name='total_works').sort_values('total_works', ascending=False).head(20)
    fig_inst = px.bar(top_insts, x='institution', y='total_works')
    st.plotly_chart(fig_inst, use_container_width=True)

    st.subheader("📚 Top 10 Most Cited Articles")
    top_works = unique_works_df.sort_values('cited_by_count', ascending=False).head(10)
    fig_works = px.bar(top_works, x='cited_by_count', y='title', orientation='h', 
                       hover_data=['publication_year', 'topic'],
                       color='publication_year')
    fig_works.update_layout(yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_works, use_container_width=True)

    st.divider()
    st.subheader("📋 Detailed Data View")
    st.markdown("Explore the raw data for the selected filters.")
    # Show relevant columns
    display_cols = ['title', 'publication_year', 'cited_by_count', 'domain', 'topic', 'author_name', 'institution']
    st.dataframe(filtered_df[display_cols].sort_values('cited_by_count', ascending=False).head(1000), use_container_width=True)
