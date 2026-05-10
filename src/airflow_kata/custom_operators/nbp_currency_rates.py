import httpx
import json
from airflow_kata.plugins.google_bucket_handler import GoogleCloudStorageHandler
import defusedxml.ElementTree as ET
import pendulum
import pandas as pd
import logging


class NbpCurrencyRates:
    """
    Collection of ETL operations responsible for processing
    NBP currency exchange rates data.
    """

    @staticmethod
    def get_rates_and_store_in_bronze(logical_date=None, **kwargs) -> bool:
        """
        Download NBP exchange rates XML file for Airflow logical date
        and store raw payload in bronze layer.

        The function uses NBP dir.txt index to resolve the correct
        historical XML file name.

        Args:
            logical_date: Airflow logical execution date.
            **kwargs: Airflow task context and DAG metadata.

        Returns:
            True if data for requested date exists, otherwise False.
        """
        logger = logging.getLogger(__name__)
        metadata = kwargs["metadata"]
        nbp_url = metadata["nbp_url"]
        run_date = logical_date.strftime("%y%m%d")
        logger.info(f"Processing data for logical date: {logical_date}")
        request_for_rates_list = httpx.get(
            f"{nbp_url}dir.txt",
            timeout=metadata.get("request_timeout", 30),
        )
        request_for_rates_list.raise_for_status()
        result_list = request_for_rates_list.text.splitlines()
        target = next(
            (i for i in result_list if i[-6:] == run_date and i[0] == "a"),
            None,
        )
        if target:
            logger.info(f"Found target and making request for: {target}")
            request_for_target_rates = httpx.get(
                f"{nbp_url}{target}.xml",
                timeout=metadata.get("request_timeout", 30),
            )
            request_for_target_rates.raise_for_status()
            now = pendulum.from_format(run_date, "YYMMDD")
            bucket_client = GoogleCloudStorageHandler()
            blob_path = f"{metadata['bronze_path']}year={now.year}/month={now.month:02d}/day={now.day:02d}/table.xml"
            bucket_client.upload_to_bucket_from_string(
                content=request_for_target_rates.content.decode("windows-1250"),
                path=blob_path,
                bucket_name=metadata["destination_bucket"],
                content_type="application/xml; charset=utf-8",
            )
            logger.info(f"Uploaded blob: {blob_path}")
            return True
        return False

    @staticmethod
    def choose_branch(ti) -> str:
        """
        Decide which downstream branch should be executed
        based on bronze ingestion result.

        Args:
            ti: Airflow task instance object.

        Returns:
            Task id of downstream branch.

        """
        bronze_result = ti.xcom_pull(task_ids="Bronze_layer_ingestion")

        if bronze_result:
            return "Silver_layer_transformation"

        return "No_data_available"

    @staticmethod
    def transform_rates_to_silver_layer(logical_date=None, **kwargs) -> None:
        """
        Read raw XML data from bronze layer, normalize currency rates
        and store transformed dataset as Parquet in silver layer.

        Transformation steps:
        - parse XML structure,
        - normalize decimal separators,
        - cast columns to proper data types,
        - calculate normalized exchange rate,
        - serialize dataset into parquet format.

        Args:
            logical_date: Airflow logical execution date.
            **kwargs: Airflow task context and DAG metadata.

        """
        logger = logging.getLogger(__name__)
        metadata = kwargs["metadata"]
        run_date = logical_date.strftime("%y%m%d")
        searched_year = run_date[:2]
        searched_month = run_date[2:4]
        searched_day = run_date[4:6]
        bucket_client = GoogleCloudStorageHandler()
        blob_name = f"{metadata['bronze_path']}year=20{searched_year}/month={searched_month}/day={searched_day}/table.xml"
        logger.info(f"Searching for blob: {blob_name}")
        xml_text = bucket_client.get_text_file_from_bucket(
            bucket_name=metadata["destination_bucket"], path=blob_name
        )

        root = ET.fromstring(xml_text)
        rates_list = []

        try:
            effective_date = root.findtext("data_publikacji")
            for child in root.findall("pozycja"):
                rates_list.append(
                    {
                        "nazwa_waluty": child.findtext("nazwa_waluty"),
                        "przelicznik": child.findtext("przelicznik"),
                        "kod_waluty": child.findtext("kod_waluty"),
                        "kurs_sredni": child.findtext("kurs_sredni"),
                        "data_publikacji": effective_date,
                    }
                )
        except ET.ParseError as e:
            logger.error(f"Parsing error: {e}")
            raise
        logger.info(f"Parsed xml into list")
        logger.info("::group::")
        logger.info("\n".join(json.dumps(r, ensure_ascii=False) for r in rates_list))
        logger.info("::endgroup::")
        df = pd.DataFrame.from_records(rates_list).rename(
            columns=metadata["column_rename_pattern"],
        )
        df["average_rate"] = df["average_rate"].str.replace(",", ".")
        df["rate"] = df["rate"].astype(int)
        df["average_rate"] = df["average_rate"].astype(float)
        df["normalize_rate"] = df["average_rate"] / df["rate"]
        df["effective_date"] = pd.to_datetime(df["effective_date"]).dt.date
        blob_name = f"{metadata['silver_path']}year=20{searched_year}/month={searched_month}/day={searched_day}/transformed.parquet"
        bucket_client.upload_to_bucket_from_parquet_file(
            df=df, path=blob_name, bucket_name=metadata["destination_bucket"]
        )
