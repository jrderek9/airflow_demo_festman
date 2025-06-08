FROM apache/airflow:2.9.1-python3.9
USER root

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r /requirements.txt

# Install Airflow providers
RUN pip3 install \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    gcc \
    python3-dev \
    openjdk-17-jdk \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/airflow/dags/sql /opt/airflow/dags/incremental_data /opt/airflow/dags/reports \
    && chown -R airflow:root /opt/airflow/dags

USER airflow