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

# 6. Drop privileges for runtime
USER airflow