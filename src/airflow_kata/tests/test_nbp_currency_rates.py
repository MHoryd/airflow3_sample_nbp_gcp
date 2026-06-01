import datetime
from unittest.mock import MagicMock, patch
from airflow_kata.custom_operators.nbp_currency_rates import NbpCurrencyRates


def test_choose_branch_with_data():
    mock_ti = MagicMock()

    mock_ti.xcom_pull.return_value = True

    returned_value = NbpCurrencyRates.choose_branch(mock_ti)

    assert returned_value == "Silver_layer_transformation"
    mock_ti.xcom_pull.assert_called_once_with(task_ids="Bronze_layer_ingestion")


def test_choose_branch_without_date():
    mock_ti = MagicMock()

    mock_ti.xcom_pull.return_value = False

    returned_value = NbpCurrencyRates.choose_branch(mock_ti)

    assert returned_value == "No_data_available"
    mock_ti.xcom_pull.assert_called_once_with(task_ids="Bronze_layer_ingestion")


def test_get_rates_success(mock_httpx_get, airflow_context, gcs_handler_instance):
    test_date = datetime.date(year=2026, month=5, day=29)
    return_value = NbpCurrencyRates.get_rates_and_store_in_bronze(
        logical_date=test_date
    )
    print(return_value)
    assert return_value is not None
