from google.cloud import storage
from src.configuration.aws_connection import GCSClient
from io import StringIO
from typing import Union, List
import os, sys
from src.logger import logging
from src.exception import MyException
import pandas as pd
import pickle

class GCSService:
    """
    A class for interacting with Google Cloud Storage (GCS), providing methods for file management,
    data uploads, and data retrieval in GCS buckets.
    """

    def __init__(self):
        gcs_client_wrapper = GCSClient()
        self.gcs_client = gcs_client_wrapper.gcs_client
        self.gcs_resource = gcs_client_wrapper.gcs_resource

    def gcs_key_path_available(self, bucket_name: str, gcs_key: str) -> bool:
        """
        Checks if a specified GCS key path (file path) exists in the specified bucket.
        """
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            return blob.exists()
        except Exception as e:
            raise MyException(e, sys)

    @staticmethod
    def read_object(blob: storage.Blob, decode: bool = True, make_readable: bool = False) -> Union[StringIO, bytes, str]:
        """
        Reads the specified GCS object with optional decoding and formatting.
        """
        try:
            content = blob.download_as_bytes() if not decode else blob.download_as_text()
            if make_readable:
                return StringIO(content if isinstance(content, str) else content.decode())
            return content
        except Exception as e:
            raise MyException(e, sys)

    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Retrieves the GCS bucket object.
        """
        try:
            bucket = self.gcs_client.bucket(bucket_name)
            return bucket
        except Exception as e:
            raise MyException(e, sys)

    def get_file_object(self, filename: str, bucket_name: str) -> Union[List[storage.Blob], storage.Blob]:
        """
        Retrieves the file object(s) from the specified bucket based on the filename.
        """
        try:
            bucket = self.get_bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=filename))
            if len(blobs) == 1:
                return blobs[0]
            return blobs
        except Exception as e:
            raise MyException(e, sys)

    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None) -> object:
        """
        Loads a serialized model from the specified GCS bucket.
        """
        try:
            model_file = f"{model_dir}/{model_name}" if model_dir else model_name
            blob = self.get_file_object(model_file, bucket_name)
            model_bytes = self.read_object(blob, decode=False)
            model = pickle.loads(model_bytes)
            logging.info("Production model loaded from GCS bucket.")
            return model
        except Exception as e:
            raise MyException(e, sys)

    def create_folder(self, folder_name: str, bucket_name: str) -> None:
        """
        Creates a folder in the GCS bucket by creating a zero-byte blob with a trailing slash.
        """
        try:
            bucket = self.get_bucket(bucket_name)
            folder_blob = bucket.blob(folder_name + "/")
            if not folder_blob.exists():
                folder_blob.upload_from_string("")
        except Exception as e:
            raise MyException(e, sys)

    def upload_file(self, from_filename: str, to_filename: str, bucket_name: str, remove: bool = True):
        """
        Uploads a local file to GCS.
        """
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(to_filename)
            blob.upload_from_filename(from_filename)
            logging.info(f"Uploaded {from_filename} to {bucket_name}/{to_filename}")
            if remove:
                os.remove(from_filename)
                logging.info(f"Removed local file {from_filename}")
        except Exception as e:
            raise MyException(e, sys)

    def upload_df_as_csv(self, data_frame: pd.DataFrame, local_filename: str, bucket_filename: str, bucket_name: str) -> None:
        """
        Uploads a DataFrame as a CSV to GCS.
        """
        try:
            data_frame.to_csv(local_filename, index=False, header=True)
            self.upload_file(local_filename, bucket_filename, bucket_name)
        except Exception as e:
            raise MyException(e, sys)

    def get_df_from_object(self, blob: storage.Blob) -> pd.DataFrame:
        """
        Converts a GCS blob to a DataFrame.
        """
        try:
            content = self.read_object(blob, make_readable=True)
            df = pd.read_csv(content, na_values="na")
            return df
        except Exception as e:
            raise MyException(e, sys)

    def read_csv(self, filename: str, bucket_name: str) -> pd.DataFrame:
        """
        Reads a CSV file from the GCS bucket into a DataFrame.
        """
        try:
            blob = self.get_file_object(filename, bucket_name)
            df = self.get_df_from_object(blob)
            return df
        except Exception as e:
            raise MyException(e, sys)
