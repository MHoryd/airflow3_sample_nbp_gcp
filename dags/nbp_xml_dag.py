import os

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)

from airflow_kata.custom_operators.nbp_currency_rates import NbpCurrencyRates
from airflow_kata.plugins.google_bucket_handler import GoogleCloudStorageHandler

metadata = GoogleCloudStorageHandler().get_json_from_bucket(
    bucket_name=os.getenv("config_bucket"),
    path="metadata/config.json",
)


with DAG(
    dag_id=metadata["dag_id"],
    start_date=pendulum.datetime(2026, 5, 1, tz=pendulum.timezone("Europe/Warsaw")),
    schedule=metadata["schedule"],
    catchup=metadata.get("catchup", False),
) as dag:
    bronze_task = PythonOperator(
        task_id="Bronze_layer_ingestion",
        op_kwargs={"metadata": metadata},
        python_callable=NbpCurrencyRates.get_rates_and_store_in_bronze,
    )

    silver_task = PythonOperator(
        task_id="Silver_layer_transformation",
        op_kwargs={"metadata": metadata},
        python_callable=NbpCurrencyRates.transform_rates_to_silver_layer,
    )

    no_data_available_task = EmptyOperator(
        task_id="No_data_available",
        doc="Dummy operator for possible notification sending if API responded correctly but data is not available",
    )

    branching_task = BranchPythonOperator(
        task_id="branching",
        python_callable=NbpCurrencyRates.choose_branch,
    )

    bronze_task >> branching_task >> [silver_task, no_data_available_task]
