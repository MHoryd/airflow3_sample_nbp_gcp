import pytest
from unittest.mock import MagicMock, patch
from airflow_kata.plugins.google_bucket_handler import GoogleCloudStorageHandler


@pytest.fixture
def gcs_handler_instance():
    with patch(
        "airflow_kata.plugins.google_bucket_handler.storage.Client",
    ) as _mock_clinet_class:
        handler = GoogleCloudStorageHandler()
        yield handler


@pytest.fixture
def airflow_context():
    with patch(
        "airflow_kata.custom_operators.nbp_currency_rates.get_current_context"
    ) as _mock_current_context:
        _mock_current_context.return_value = {
            "metadata": {
                "nbp_url": "sample_url.com",
                "request_timeout": 20,
                "bronze_path": "test_path",
                "destination_bucket": "target_bucket",
            }
        }
        yield _mock_current_context


@pytest.fixture
def mock_httpx_get():
    with patch(
        "airflow_kata.custom_operators.nbp_currency_rates.httpx.get"
    ) as _mock_get:
        mock_response_1 = MagicMock()
        mock_response_1.return_value = "c001z240101\na001z240101\nb001z240101"

        mock_response_2 = MagicMock()
        mock_response_2.content = b"<xml>dummy data</xml>"

        _mock_get.side_effect = [mock_response_1, mock_response_2]

        yield _mock_get
