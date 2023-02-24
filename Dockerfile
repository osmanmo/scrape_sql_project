FROM apache/airflow:2.2.0-python3.9 AS builder

USER root
RUN apt-get update \
    && apt-get install -yqq gcc libpq-dev python3-dev wget make zlib1g-dev libncurses5-dev  \
    libgdbm-dev libnss3-dev libssl-dev libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN python -m pip install --no-cache-dir -r ./requirements.txt

ARG ETL_DIR="/opt/airflow/etl"
ARG TMP_DIR="/opt/airflow/tmp"

ENV PYTHONPATH ${ETL_DIR}:${PYTHONPATH}

RUN umask 0002; mkdir -p ${TMP_DIR}

WORKDIR ${ETL_DIR}
