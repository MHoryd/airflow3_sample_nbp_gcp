FROM apache/airflow:3.0.2

USER root

RUN python -m pip install --no-cache-dir defusedxml httpx pandas pendulum google-cloud-storage

WORKDIR /opt/airflow

COPY src /opt/airflow/src
COPY dags /opt/airflow/dags

ENV PYTHONPATH=/opt/airflow/src

USER airflow
