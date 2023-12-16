import pandas as pd
from io import BytesIO
from google.cloud import storage
from utils import GCPUtils

class TransformData:
    def __init__(self, bucket_name, gcp_utils):
        self.bucket_name = bucket_name
        self.gcp_utils = gcp_utils
        self.storage_client = storage.Client.from_service_account_info(self.gcp_utils.credentials)

    def read_data_from_gcs(self):
        bucket = self.storage_client.bucket(self.bucket_name)
        blobs = bucket.list_blobs()  # List all objects in the bucket

        combined_df = pd.DataFrame()

        for blob in blobs:
            if blob.name.endswith('.csv'):  # Process only CSV files
                content = blob.download_as_text()
                df = pd.read_csv(BytesIO(content.encode()))
                combined_df = pd.concat([combined_df, df], ignore_index=True)

        return combined_df

    def transform_data(self, df):
        # this block I initiated for cleaning the data
        return df

    def save_transformed_data(self, df, destination_file_name):
        file_name = f"transformed_{destination_file_name}"
        self.gcp_utils.save_data_to_gcs(df, self.bucket_name, file_name)


def transform_data_main():
    bucket_name = 'merged_bucket17'
    gcp_utils = GCPUtils()

    transformer = TransformData(bucket_name, gcp_utils)
    data_df = transformer.read_data_from_gcs()
    transformed_df = transformer.transform_data(data_df)
    transformer.save_transformed_data(transformed_df, 'combined_transformed_data.csv')

if __name__ == "__main__":
    transform_data_main()
