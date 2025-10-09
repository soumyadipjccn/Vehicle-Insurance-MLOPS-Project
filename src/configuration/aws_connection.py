from google.cloud import storage
import os
from src.constants import GCP_SERVICE_ACCOUNT_JSON_ENV_KEY, GCP_REGION_NAME


class GCSClient:

    gcs_client = None
    gcs_resource = None

    def __init__(self, region_name=GCP_REGION_NAME):
        """ 
        This Class gets Google Cloud credentials from environment variable 
        and creates a connection with GCS bucket.
        Raises an exception when environment variable is not set.
        """

        if GCSClient.gcs_client is None or GCSClient.gcs_resource is None:
            service_account_json = os.getenv(GCP_SERVICE_ACCOUNT_JSON_ENV_KEY)

            if service_account_json is None:
                raise Exception(f"Environment variable: {GCP_SERVICE_ACCOUNT_JSON_ENV_KEY} is not set.")

            # Initialize Google Cloud Storage client
            GCSClient.gcs_client = storage.Client.from_service_account_json(service_account_json)
            GCSClient.gcs_resource = GCSClient.gcs_client  # To match structure with AWS code

        self.gcs_client = GCSClient.gcs_client
        self.gcs_resource = GCSClient.gcs_resource
        self.region_name = region_name

    def upload_file(self, bucket_name, source_file_path, destination_blob_name):
        """Upload a file to the bucket."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to {destination_blob_name}.")

    def download_file(self, bucket_name, blob_name, destination_file_path):
        """Download a file from the bucket."""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(destination_file_path)
        print(f"Blob {blob_name} downloaded to {destination_file_path}.")
