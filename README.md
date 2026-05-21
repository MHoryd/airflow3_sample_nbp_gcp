# Airflow 3 Standalone ETL Project (NBP Currency Rates)

This project is a small standalone test environment for Apache Airflow 3, designed to demonstrate a simple ETL pipeline using publicly available NBP currency exchange rate data.

The pipeline is implemented as a daily DAG that:

- extracts XML currency rates from the National Bank of Poland (NBP),
- stores raw data in a bronze layer,
- transforms and normalizes the data using Pandas,
- stores the final dataset in a silver layer in Parquet format.

The project is intentionally minimal and focuses on:

- Airflow 3 DAG execution model,
- metadata-driven configuration,
- basic ETL architecture (bronze/silver),
- external storage integration.

## Installation

To start the environment, simply run:

```bash
docker compose build
docker compose up -d
```

## Airflow Authentication (Important)

This project uses the Airflow 3 Simple Auth Manager.

Unlike previous versions of Airflow, user creation via CLI (airflow users create) is not supported.

After starting the containers, you must retrieve the generated admin password from logs:

```bash
docker compose logs | grep "airflow3-example  | Simple auth manager | Password for user 'admin':"
```

## Configuration (.env)

The DAG requires a configuration bucket that stores metadata.

Create a .env file:

```bash
CONFIG_BUCKET=airflow_sample
```
This bucket is used to load DAG configuration dynamically at runtime.


## Google Cloud Credentials

he project expects a Google Cloud service account key inside:



```bash
keys/gcp-key.json
```
This file is required for accessing the destination storage bucket.


## DAG Configuration Schema

The DAG is fully metadata-driven and expects the following JSON structure stored in the configuration bucket. Currently the path in bucket is hardcoded to metadata/config.json:


```json
{
  "dag_id": "name_of_the_dag",
  "schedule": "0 7 * * 1-5",
  "catchup": false,
  "nbp_url": "https://static.nbp.pl/dane/kursy/xml/",
  "destination_bucket": "name of destination bucket to store data",
  "bronze_path": "bronze/nbp",
  "silver_path": "silver/nbp",
  "request_timeout": 30,
  "column_rename_pattern": {
    "xml_source_column_name": "target_column_name"
  }
}

```
