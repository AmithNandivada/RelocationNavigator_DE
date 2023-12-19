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

        zip_to_state = {
                        '02903': 'Rhode Island', '03301': 'New Hampshire', '04101': 'Maine', '05601': 'Vermont', '06101': 'Connecticut',
                        '07001': 'New Jersey', '12201': 'New York', '17101': 'Pennsylvania', '19901': 'Delaware', '21201': 'Maryland',
                        '22101': 'Virginia', '23218': 'Virginia', '26505': 'West Virginia', '27601': 'North Carolina', '29201': 'South Carolina',
                        '30301': 'Georgia', '33101': 'Florida', '35203': 'Alabama', '37201': 'Tennessee', '39201': 'Mississippi',
                        '40601': 'Montana', '43201': 'Ohio', '46201': 'Indiana', '49201': 'Michigan', '52801': 'Iowa', '53202': 'Wisconsin',
                        '55101': 'Minnesota', '57501': 'South Dakota', '58501': 'North Dakota', '59601': 'Montana', '60601': 'Illinois',
                        '65101': 'Missouri', '67202': 'Kansas', '68501': 'Nebraska', '70112': 'Louisiana', '72201': 'Arkansas',
                        '73101': 'Oklahoma', '75001': 'Texas', '80201': 'Colorado', '82001': 'Wyoming', '83402': 'Idaho', '84101': 'Utah',
                        '85001': 'Arizona', '87501': 'New Mexico', '89701': 'Nevada', '90001': 'California', '96789': 'Hawaii',
                        '97201': 'Oregon', '98501': 'Washington', '99501': 'Alaska'
                        }

        combined_df['zipcode'] = str(self.zip_code)
        combined_df['state'] = combined_df['zipcode'].map(zip_to_state)

        return combined_df
    
    def write_merged_data(self, combined_df):
        file_name = f"property_data_zip_{self.zip_code}.csv"
        self.gcp_utils.save_data_to_gcs(combined_df, self.write_bucket, file_name)


def merge_data_main():
    gcp_utils = GCPUtils()
    read_bucket = 'landing_bucket17'
    write_bucket = 'merged_bucket17'
    zip_codes = ['02903', '03301', '04101', '05601', '06101', '07001', '12201', '17101', '19901', '21201',
                '22101', '23218', '26505', '27601', '29201', '30301', '33101', '35203', '37201', '39201',
                '40601', '43201', '46201', '49201', '52801', '53202', '55101', '57501', '58501', '59601',
                '60601', '65101', '67202', '68501', '70112', '72201', '73101', '75001', '80201', '82001',
                '83402', '84101', '85001', '87501', '89701', '90001', '96789', '97201', '98501', '99501']

    for zip_code in zip_codes:
        print("Fetching data for zipcode : ", zip_code)
        merge_data = MergeData(read_bucket, write_bucket, gcp_utils, zip_code)
        combined_df = merge_data.read_combined_data()
        merge_data.write_merged_data(combined_df)

if __name__ == "__main__":
    merge_data_main()