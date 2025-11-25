FROM apache/airflow:2.10.0-python3.12

USER root

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy project definition
COPY pyproject.toml .

# Install dependencies using uv
# We use --system to install into the system python environment of the container
RUN uv pip install --system -r pyproject.toml

USER airflow
