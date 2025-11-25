# 📚 OpenAlex Research Dashboard

A modular **ETL pipeline** and **interactive dashboard** for analyzing bibliometric data from [OpenAlex](https://openalex.org/). This project tracks research trends, top authors, institutions, and influential works across multiple fields of study (e.g., Artificial Intelligence, Economics, Physics).

![openalex-research-dashboard](https://github.com/user-attachments/assets/edc5b295-9127-4c28-8546-f431fdb2d738)

## 🎯 Objectives

*   **Analyze Trends**: Visualize the growth of research output over time.
*   **Identify Leaders**: Highlight top authors and institutions based on citation impact and publication volume.
*   **Discover Content**: Surface the most cited articles in specific fields.
*   **Dynamic Exploration**: Allow users to filter data by topic, year, and institution interactively.

## 🏗️ Architecture & Technology

This project follows the **Medallion Architecture** pattern for data engineering:

### 1. Data Pipeline (Airflow)
*   **Orchestration**: Apache Airflow (running in Docker).
*   **Bronze Layer**: Fetches raw JSON data from the OpenAlex API for selected topics.
*   **Silver Layer**: Flattens nested JSON structures (authorships, institutions) and cleans data types. Saved as Parquet.
*   **Gold Layer**: Aggregates data for specific metrics (Yearly Trends, Top Authors) and creates a consolidated **Master Table** (`all_works.parquet`) for dynamic dashboarding.

### 2. Visualization (Streamlit)
*   **Interactive Dashboard**: Built with Streamlit.
*   **Dynamic Filtering**: Users can filter by **Field of Study**, **Publication Year**, and **Institution**.
*   **On-the-fly Aggregation**: Metrics and charts are calculated in real-time based on user filters.
*   **Pipeline Control**: Includes a button to trigger the Airflow ETL pipeline directly from the UI.
*   **Custom Theme**: Styled with a "Monokai Rainstorm" (Dark Blue) aesthetic.

### 🛠️ Tech Stack
*   **Language**: Python 3.12
*   **Dependency Management**: `uv` (fast Python package installer)
*   **Containerization**: Docker & Docker Compose
*   **Data Processing**: Pandas, PyArrow
*   **Visualization**: Plotly Express, Streamlit

## 🚀 How to Run

### Prerequisites
*   Docker Desktop installed and running.
*   `uv` installed (recommended) or Python 3.12+.

### 1. Start the Data Pipeline
Initialize and run the Airflow services using Docker Compose:

```bash
docker-compose up -d
```

*   **Airflow UI**: Access at [http://localhost:8080](http://localhost:8080)
*   **Credentials**: `airflow` / `airflow`

The DAG `openalex_medallion_etl` will run daily, or you can trigger it manually.

### 2. Run the Dashboard
Run the Streamlit app locally:

```bash
# Install dependencies
uv sync

# Run the app
uv run streamlit run dashboard/app.py
```

*   **Dashboard**: Access at [http://localhost:8501](http://localhost:8501)

## 📂 Project Structure

```
OpenAlex_Dashboard/
├── airflow/
│   └── dags/           # Airflow DAG definitions
├── dashboard/
│   └── app.py          # Streamlit application
├── data/               # Local data storage (mounted to Docker)
│   ├── bronze/         # Raw JSON files
│   ├── silver/         # Cleaned Parquet files
│   └── gold/           # Aggregated Parquet files
├── etl/                # Python ETL scripts
│   ├── bronze.py       # API Fetching
│   ├── silver.py       # Transformation
│   └── gold.py         # Aggregation
├── .streamlit/         # Streamlit configuration (Theme)
├── docker-compose.yaml # Airflow services configuration
├── Dockerfile          # Custom Airflow image with uv
└── pyproject.toml      # Python dependencies
```

## 📊 Data Sources

All data is sourced from **OpenAlex**, a free and open catalog of the global research system.
*   **API Endpoint**: `https://api.openalex.org/works`
*   **Attribution**: Data provided by OpenAlex.

## 📄 License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

