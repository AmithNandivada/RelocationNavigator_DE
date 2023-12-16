import pandas as pd
from io import BytesIO
from utils import GCPUtils
from google.cloud import storage

class MergeData:
    def __init__(self, read_bucket, write_bucket, gcp_utils, zip_code):
        self.read_bucket = read_bucket
        self.write_bucket = write_bucket
        self.gcp_utils = gcp_utils # Pass the GCPUtils instance
        self.zip_code = zip_code
        self.storage_client = storage.Client.from_service_account_info(self.gcp_utils.credentials)

    def read_combined_data(self):
        file_prefix = f'property_data_zip_{self.zip_code}_page_'
        blobs = self.storage_client.bucket(self.read_bucket).list_blobs(prefix=file_prefix)

        page_numbers = set(int(blob.name.split('_page_')[-1].split('.')[0]) for blob in blobs)
        total_pages = len(page_numbers)
        print("Total no of pages:", total_pages)

        combined_df = pd.DataFrame()

        for blob in self.storage_client.bucket(self.read_bucket).list_blobs(prefix=file_prefix):
            # Download CSV file content
            content = blob.download_as_text()
            df = pd.read_csv(BytesIO(content.encode()))  # Convert string to bytes

            # Concatenate the data
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        return combined_df
    
    def write_merged_data(self, combined_df):
        file_name = f"property_data_zip_{self.zip_code}.csv"
        self.gcp_utils.save_data_to_gcs(combined_df, self.write_bucket, file_name)


def merge_data_main():
    gcp_utils = GCPUtils()
    read_bucket = 'landing_bucket17'
    write_bucket = 'merged_bucket17'
    # zip_codes = ['80301', '03301']
    zip_codes = ['35203', '99501', '85001', '72201', '90001', '80201', 
                 '06101', '19901', '33101', '30301', '96801', '83701', 
                 '60601', '46201', '52801', '66601', '40601', '70112', 
                 '04101', '21201', '02101', '49201', '55101', '39201', 
                 '65101', '59601', '68501', '89701', '03301', '07001', 
                 '87501', '12201', '27601', '58501', '43201', '73101', 
                 '97201', '17101', '02901', '29201', '57501', '37201', 
                 '73301', '84101', '05601', '23218', '98501', '25301', 
                 '53701', '82001']

    for zip_code in zip_codes:
        print("Fetching data for zipcode : ", zip_code)
        merge_data = MergeData(read_bucket, write_bucket, gcp_utils, zip_code)
        combined_df = merge_data.read_combined_data()
        merge_data.write_merged_data(combined_df)

if __name__ == "__main__":
    merge_data_main()



    
