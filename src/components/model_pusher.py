import sys
from src.cloud_storage.aws_storage import GCSService  # <-- changed from aws_storage
from src.exception import MyException
from src.logger import logging
from src.entity.artifact_entity import ModelPusherArtifact, ModelEvaluationArtifact
from src.entity.config_entity import ModelPusherConfig
from src.entity.s3_estimator import Proj1Estimator  # You should use your GCS version of Proj1Estimator

class ModelPusher:
    def __init__(self, model_evaluation_artifact: ModelEvaluationArtifact,
                 model_pusher_config: ModelPusherConfig):
        """
        :param model_evaluation_artifact: Output reference of data evaluation artifact stage
        :param model_pusher_config: Configuration for model pusher
        """
        self.gcs = GCSService()  # <-- use GCSService instead of SimpleStorageService
        self.model_evaluation_artifact = model_evaluation_artifact
        self.model_pusher_config = model_pusher_config

        # Use GCS version of Proj1Estimator
        self.proj1_estimator = Proj1Estimator(
            bucket_name=model_pusher_config.bucket_name,
            model_path=model_pusher_config.s3_model_key_path  # You can rename to gcs_model_key_path if you like
        )

    def initiate_model_pusher(self) -> ModelPusherArtifact:
        """
        Uploads the trained model to GCS bucket and returns a ModelPusherArtifact
        """
        logging.info("Entered initiate_model_pusher method of ModelPusher class")

        try:
            print("------------------------------------------------------------------------------------------------")
            logging.info("Uploading artifacts folder to GCS bucket")
            
            logging.info("Uploading new model to GCS bucket....")
            self.proj1_estimator.save_model(from_file=self.model_evaluation_artifact.trained_model_path)

            model_pusher_artifact = ModelPusherArtifact(
                bucket_name=self.model_pusher_config.bucket_name,
                s3_model_path=self.model_pusher_config.s3_model_key_path  # Keep same attribute for compatibility
            )

            logging.info("Uploaded artifacts folder to GCS bucket")
            logging.info(f"Model pusher artifact: [{model_pusher_artifact}]")
            logging.info("Exited initiate_model_pusher method of ModelPusher class")
            
            return model_pusher_artifact

        except Exception as e:
            raise MyException(e, sys) from e
