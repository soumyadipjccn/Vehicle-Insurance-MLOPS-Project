# from google.cloud import storage

# client = storage.Client()  # Automatically uses the service account
# buckets = list(client.list_buckets())

# print("Buckets in your project:")
# for bucket in buckets:
#     print(bucket.name)

from src.pipline.training_pipeline import TrainPipeline

pipline = TrainPipeline()
pipline.run_pipeline()