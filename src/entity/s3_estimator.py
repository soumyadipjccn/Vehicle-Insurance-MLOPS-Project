from src.cloud_storage.aws_storage import GCSService  # Changed from SimpleStorageService
from src.exception import MyException
from src.entity.estimator import MyModel
import sys
from pandas import DataFrame

class Proj1Estimator:
    """
    This class is used to save and retrieve our model from GCS bucket and to do prediction.
    """

    def __init__(self, bucket_name: str, model_path: str):
        """
        :param bucket_name: Name of your model bucket
        :param model_path: Location of your model in bucket
        """
        self.bucket_name = bucket_name
        self.gcs = GCSService()  # Use GCSService instead of SimpleStorageService
        self.model_path = model_path
        self.loaded_model: MyModel = None

    def is_model_present(self, model_path: str) -> bool:
        """
        Checks if the model exists in the bucket.
        """
        try:
            return self.gcs.gcs_key_path_available(bucket_name=self.bucket_name, gcs_key=model_path)
        except MyException as e:
            print(e)
            return False

    def load_model(self) -> MyModel:
        """
        Load the model from the GCS bucket.
        """
        try:
            return self.gcs.load_model(model_name=self.model_path, bucket_name=self.bucket_name)
        except MyException as e:
            raise e

    def save_model(self, from_file: str, remove: bool = False) -> None:
        """
        Save the model to the GCS bucket.
        :param from_file: Your local system model path
        :param remove: If True, deletes the local file after upload
        """
        try:
            self.gcs.upload_file(
                from_filename=from_file,
                to_filename=self.model_path,
                bucket_name=self.bucket_name,
                remove=remove
            )
        except Exception as e:
            raise MyException(e, sys)

    def predict(self, dataframe: DataFrame):
        """
        Make predictions using the loaded model.
        :param dataframe: Input DataFrame
        :return: Predictions
        """
        try:
            if self.loaded_model is None:
                self.loaded_model = self.load_model()
            return self.loaded_model.predict(dataframe=dataframe)
        except Exception as e:
            raise MyException(e, sys)



