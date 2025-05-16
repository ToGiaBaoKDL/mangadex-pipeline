FROM apache/airflow:2.10.5

# Set Airflow version
ENV AIRFLOW_VERSION=2.10.5

# Copy requirements from parent directory
COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt