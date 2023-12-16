import requests
import pandas as pd
import time
from google.oauth2 import service_account
from utils import GCPUtils


class ExtractData:
    def __init__(self, api_key, bucket_name, gcp_utils):
        self.api_key = api_key
        self.bucket_name = bucket_name
        self.gcp_utils = gcp_utils  # Pass the GCPUtils instance

        self.base_url = 'https://api.gateway.attomdata.com'
        self.property_version = "/propertyapi/v1.0.0"
        self.property_endpoint = '/property/detail'
        self.base_params = {'pageSize': 200}
        # self.zip_codes = ['05601']
        # self.zip_codes = ['96789', '83402', '67202', '02840', '75001', '22101', '26505', '53202']
        self.zip_codes = ['02903']

    def extract_data_from_api(self, params):
        headers = {'accept': "application/json", 'apikey': self.api_key}
        response = requests.get(f'{self.base_url}{self.property_version}{self.property_endpoint}', params=params, headers=headers)

        if response.status_code != 200:
            response_data = response.json()
            if response_data.get('status', {}).get('msg') == 'SuccessWithoutResult':
                return response_data
            else:
                print("Failed to fetch data: ", response.text)
                response.raise_for_status()

        return response.json()

    def fetch_and_save_property_data(self):
        for zip_code in self.zip_codes:
            params = self.base_params.copy()
            params['postalcode'] = zip_code
            page = 1

            while True:
                params['page'] = page
                property_data = self.extract_data_from_api(params)

                if 'property' not in property_data or not property_data['property']:
                    print(f"No more data available for ZIP code: {zip_code} at page {page}")
                    break  # Break if no more data

                df = pd.json_normalize(property_data['property'])
                file_name = f"property_data_zip_{zip_code}_page_{page}.csv"
                self.gcp_utils.save_data_to_gcs(df, self.bucket_name, file_name)
                page += 1
                time.sleep(1)  # To respect API rate limits

        print("Data fetching completed.")


def extract_data_main():
    # e2f6093efe653fa0cf74a5902912f64a, a32aebb47b8078c9d90e487e5f816cf7
    api_key = 'a32aebb47b8078c9d90e487e5f816cf7'
    gcs_bucket_name = 'landing_bucket17'

    gcp_utils = GCPUtils()

    extract_data = ExtractData(api_key, gcs_bucket_name, gcp_utils)
    extract_data.fetch_and_save_property_data()

if __name__ == "__main__":
    extract_data_main()