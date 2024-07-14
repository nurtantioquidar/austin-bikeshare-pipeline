FROM python:3.9.10-slim-buster

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_VERSION=2.6.3
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.9.txt"
ENV PATH=$PATH:${AIRFLOW_HOME}/.local/bin

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libpq-dev \
    git \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    openjdk-11-jdk \
    && apt-get clean

# Install Google Cloud SDK manually
RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-367.0.0-linux-x86_64.tar.gz && \
    tar -xf google-cloud-sdk-367.0.0-linux-x86_64.tar.gz && \
    ./google-cloud-sdk/install.sh --quiet && \
    rm google-cloud-sdk-367.0.0-linux-x86_64.tar.gz

# Add Google Cloud SDK to PATH
ENV PATH $PATH:/google-cloud-sdk/bin

# Create airflow user
RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

# Switch to airflow user
USER airflow
WORKDIR ${AIRFLOW_HOME}

# Install Airflow with subpackages
RUN pip install --user --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Install additional Python packages
COPY --chown=airflow:airflow requirements.txt /
RUN pip install --user --no-cache-dir -r /requirements.txt

# Install pyspark
RUN pip install --user --no-cache-dir pyspark

# Install psycopg2
RUN pip install --user --no-cache-dir psycopg2-binary

# Copy DAGs
COPY --chown=airflow:airflow dags/ ${AIRFLOW_HOME}/dags/

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/google-credentials.json

# Copy the entrypoint script
COPY --chown=airflow:airflow entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
