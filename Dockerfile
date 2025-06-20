FROM apache/airflow:2.8.0-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         curl \
         build-essential \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy source code
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root config/ /opt/airflow/config/
COPY --chown=airflow:root sql/ /opt/airflow/sql/
COPY --chown=airflow:root dbt/ /opt/airflow/dbt/

# Set DBT profiles directory
ENV DBT_PROFILES_DIR=/opt/airflow/dbt