from google.cloud import storage
from typing import Any
import json
import tempfile
import pandas as pd


class GoogleCloudStorageHandler:
    """
    Utility wrapper around Google Cloud Storage client used for
    uploading and downloading project assets and datasets.
    """

    def __init__(self) -> None:
        self.client = storage.Client()

    def upload_to_bucket_from_string(
        self,
        content: str,
        path: str,
        bucket_name: str,
        content_type: str = "text/plain",
    ) -> None:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(path)
        blob.upload_from_string(content, content_type=content_type)

    def upload_to_bucket_from_parquet_file(
        self, df: pd.DataFrame, path: str, bucket_name: str
    ) -> None:
        bucket = self.client.bucket(bucket_name)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
            df.to_parquet(tmp.name)
            tmp_path = tmp.name
            blob = bucket.blob(path)
            blob.upload_from_filename(tmp_path)

    def get_json_from_bucket(self, path: str, bucket_name: str) -> dict[str, Any]:
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(path)
        return json.loads(blob.download_as_string())

    def get_text_file_from_bucket(self, path: str, bucket_name: str) -> dict[str, Any]:
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(path)
        return blob.download_as_string().decode("utf-8")
