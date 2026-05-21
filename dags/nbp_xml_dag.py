import os

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import (
    BranchPythonOperator,
    PythonOperator,
)
from airflow.sdk import DAG

from airflow_kata.custom_operators.nbp_currency_rates import NbpCurrencyRates
from airflow_kata.plugins.google_bucket_handler import GoogleCloudStorageHandler

metadata = GoogleCloudStorageHandler().get_json_from_bucket(
    bucket_name=os.getenv("CONFIG_BUCKET"),
    path="metadata/config.json",
)


with DAG(
    dag_id=metadata["dag_id"],
    start_date=pendulum.datetime(2026, 5, 1, tz=pendulum.timezone("Europe/Warsaw")),
    schedule=metadata["schedule"],
    catchup=metadata.get("catchup", False),
    params={"metadata": metadata},
) as dag:
    Bronze_layer_ingestion = PythonOperator(
        task_id="Bronze_layer_ingestion",
        op_kwargs={"metadata": metadata},
        python_callable=NbpCurrencyRates.get_rates_and_store_in_bronze,
    )

    Silver_layer_transformation = PythonOperator(
        task_id="Silver_layer_transformation",
        op_kwargs={"metadata": metadata},
        python_callable=NbpCurrencyRates.transform_rates_to_silver_layer,
    )

    No_data_available = EmptyOperator(
        task_id="No_data_available",
        doc="Dummy operator for possible notification sending if API responded correctly but data is not available",
    )

    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=NbpCurrencyRates.choose_branch,
    )

    (
        Bronze_layer_ingestion
        >> branching
        >> [Silver_layer_transformation, No_data_available]
    )  # type: ignore
