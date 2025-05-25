# Stage 1: Copy uv binary from official image
FROM python:3.12-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Stage 2: Final image
FROM ubuntu:22.04

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
ENV AIRFLOW_HOME="/app/airflow"
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=1000
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
# ENV PYSPARK_PYTHON=/usr/bin/python3
# ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PYSPARK_PYTHON=/app/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/app/.venv/bin/python

# Install system dependencies
RUN apt-get update -y \
 && apt-get install -y software-properties-common \
 && add-apt-repository ppa:deadsnakes/ppa \
 && apt-get install -y openjdk-8-jdk curl ca-certificates build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Copy uv from builder
COPY --from=builder /bin/uv /usr/local/bin/uv
COPY --from=builder /bin/uvx /usr/local/bin/uvx

# Create app directory and copy code
WORKDIR /app
COPY . .

# Create Python virtual environment and install deps via uv
RUN uv venv .venv --python=python3.11.12 && \
    . .venv/bin/activate && \
    uv pip install -r pyproject.toml && \
    uv pip install -e .

# Ensure airflow DB is migrated
RUN . .venv/bin/activate && airflow db migrate

# Make start script executable
# RUN chmod 777 start.sh
RUN echo '#!/bin/sh\nairflow standalone' > start.sh && chmod 777 start.sh

# Activate venv & run start.sh on container start
ENTRYPOINT ["/bin/sh", "-c"]
CMD [". .venv/bin/activate && ./start.sh"]

