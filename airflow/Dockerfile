# 1. Start with the modern Airflow image
FROM apache/airflow:3.3.2

# 2. Switch to 'root' for system-level installations
USER root

# 3.copy uv from the official image to install it and then use it to install the requirements
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 4. Copy your requirements file
COPY requirements.txt .

# 5. Install all Python dependencies into system Python
RUN uv pip install --no-cache --system -r requirements.txt

# 6. dbt (Fusion engine, v2) in its own virtualenv, kept apart from Airflow's
#    Python packages. Fusion ships the Snowflake adapter built in.
#    The DAG calls /opt/dbt-venv/bin/dbt directly.
RUN uv venv /opt/dbt-venv && \
    uv pip install --no-cache --python /opt/dbt-venv/bin/python "dbt==2.0.9"

# 7. Drop privileges for runtime
USER airflow