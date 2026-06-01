from unittest.mock import MagicMock, patch

import pandas as pd


def test_upload_to_bucket_from_string(gcs_handler_instance):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    gcs_handler_instance.client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    return_value = gcs_handler_instance.upload_to_bucket_from_string(
        content="Some_text",
        path="sample/test.txt",
        bucket_name="test_bucket",
    )

    gcs_handler_instance.client.bucket.assert_called_once_with("test_bucket")
    mock_bucket.blob.assert_called_once_with("sample/test.txt")
    mock_blob.upload_from_string.assert_called_once_with(
        data="Some_text",
        content_type="text/plain",
    )

    assert return_value is None


def test_upload_to_bucket_from_parquet_file(gcs_handler_instance):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    gcs_handler_instance.client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    test_df = pd.DataFrame([{"field1": 22}])

    with patch.object(pd.DataFrame, "to_parquet") as mocked_to_parquet:
        return_value = gcs_handler_instance.upload_to_bucket_from_parquet_file(
            df=test_df,
            path="sample/test.parquet",
            bucket_name="test_bucket",
        )
        mocked_to_parquet.assert_called_once()

        wrtitten_temp_path = mocked_to_parquet.call_args[0][0]
        mock_bucket.blob.assert_called_once_with("sample/test.parquet")
        mock_blob.upload_from_filename.assert_called_once_with(wrtitten_temp_path)

        assert return_value is None


def test_get_json_from_bucket(gcs_handler_instance):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    gcs_handler_instance.client.get_bucket.return_value = mock_bucket
    mock_bucket.get_blob.return_value = mock_blob

    mock_blob.download_as_string.return_value = b'{"key1": "value1"}'

    return_value = gcs_handler_instance.get_json_from_bucket(
        path="test/path.txt",
        bucket_name="test_bucket",
    )

    assert isinstance(return_value, dict)
    assert return_value == {"key1": "value1"}


def test_get_text_file_from_bucket(gcs_handler_instance):
    mock_bucket = MagicMock()
    mock_blob = MagicMock()

    gcs_handler_instance.client.get_bucket.return_value = mock_bucket
    mock_bucket.get_blob.return_value = mock_blob

    mock_blob.download_as_string.return_value = b"Z\xc3\xb3\xc5\x82ta g\xc4\x99\xc5\x9b"

    return_value = gcs_handler_instance.get_text_file_from_bucket(
        path="test/path.txt",
        bucket_name="test_bucket",
    )

    gcs_handler_instance.client.get_bucket.assert_called_once_with("test_bucket")
    mock_bucket.get_blob.assert_called_once_with("test/path.txt")

    assert isinstance(return_value, str)
    assert return_value == "Zółta gęś"
