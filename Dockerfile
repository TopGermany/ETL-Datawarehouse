FROM apache/airflow:2.10.0-python3.10

USER root
# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libvulkan1 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY agents/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install playwright

# Install Playwright browser binaries
USER root
RUN playwright install-deps chromium

USER airflow
RUN playwright install chromium
