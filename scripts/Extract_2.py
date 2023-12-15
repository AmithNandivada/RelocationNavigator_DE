import requests
import pandas as pd
from google.oauth2 import service_account
from google.cloud import storage
import time
# from google.auth.transport.requests import Request

def extract_data_from_api(api_key, version, endPoint, parameters):
    base_url = 'https://api.gateway.attomdata.com'
    headers = {'accept': "application/json", 'apikey': api_key}
    response = requests.get(f'{base_url}{version}{endPoint}', params=parameters, headers=headers)
    
    if response.status_code != 200:
        # Check if it's a 'SuccessWithoutResult' case
        response_data = response.json()
        if response_data.get('status', {}).get('msg') == 'SuccessWithoutResult':
            return response_data
        else:
            print("Failed to fetch data: ", response.text)
            response.raise_for_status()
    
    return response.json()

def save_data_to_gcs(data, bucket_name, destination_blob_name, credentials):
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(data.to_csv(index=False), content_type='text/csv')
    print(f"Data uploaded to {destination_blob_name} in bucket {bucket_name}.")

def fetch_and_save_property_data(api_key, bucket_name, credentials):
    # credentials.refresh(Request())
    property_version = "/propertyapi/v1.0.0"
    property_endpoint = '/property/detail'
    base_params = {'pageSize': 200}  # Removed yearbuilt and state filters

    # Representative ZIP code for each state

    zip_codes = ['80301']

    for zip_code in zip_codes:
        params = base_params.copy()
        params['postalcode'] = zip_code
        page = 1

        while True:
            params['page'] = page
            property_data = extract_data_from_api(api_key, property_version, property_endpoint, params)

            if 'property' not in property_data or not property_data['property']:
                print(f"No more data available for ZIP code: {zip_code} at page {page}")
                break  # Break if no more data

            df = pd.json_normalize(property_data['property'])
            file_name = f"property_data_zip_{zip_code}_page_{page}.csv"
            # file_name = f"o_run_{zip_code}_{page}.csv"
            save_data_to_gcs(df, bucket_name, file_name, credentials)
            page += 1
            time.sleep(1)  # To respect API rate limits

    print("Data fetching completed.")




# Usage
#API_KEY = 'e2f6093efe653fa0cf74a5902912f64a'  # Replace with your actual API key
#BUCKET_NAME = 'landing_bucket17'
#fetch_and_save_property_data(API_KEY, BUCKET_NAME, gcp_credentials)


def main():
    # GCP Credentials
    gcp_credentials_dict = {
    "type": "service_account",
    "project_id": "housingdataanalysis",
    "private_key_id": "dd03d494e9715442ccddb0869017e174b36b142b",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCnyldPobY5w8Sp\ndu6DVg5ZrC4tV1A800iEmZ7NDZuU9wuy4VFxsa239XIRdPaqC31ZEyNVFPe5Nn0s\nQvBQ7FDtdKpshjUADPoCsel2BalamuskctLEwDrtr8XXRMf5V+qugF4B7J2oOmxd\nDIu1yQFqsdQ9y1s8K1Fx0SCSX8GzrN3A8HN8OF6//QXEVG8oP6pNViB/02OSfaiX\nKxT1nhqya13zTrcpTxzploUvCdyYuXONT+plHiT5WcSicdKvLb+1X2ztCknnNE5U\ngNulwf6odKnczk3FfoG5K5jzWXwjRdUdShebhty1Ip773h22zCNwtqPVgMbz9PvZ\nXrEoflLJAgMBAAECggEACmxiOKbv2k5MTwe9oOiNayosUzI/oIuyj1Q4vn3IKryf\nHC1toQ274Dft7mIw+ZbNomUzNnOfQQD+zStaYPiRPXMYLDY5ICfuNxEk0uleoCuM\nDH9akxDrCD15YZpa/uUkhdMhI+b2U09P0Ib5PeF/BkkGSZFyuT3qqRKn/pfWwWnT\nuAFKWfni7eV6nGr85GiCFmuVUO42Sp0R6EYW8XG90hpjMqaSWkQ+ywXNm4D74fkd\nVqnXUf8oJB9VaQNHTZpxfIynrhwDGGL5fVi0icKIg0kU4zGZTSMNBPHTQAGgGdbF\ni+lCCwO09xVFiF84BfJffvi1Y0Jc/lMdDiDt/qLngQKBgQDWVvD7tKxo9RlAYytD\nVVdusgnJvIuWlq/38MguN5uNInmIYYpK0Njy0JW9rXS0lppEKV14+4U2c/r6OYps\n6qsSjs/S8G+1P2akyoRXGyF1KWYEY298exRCLrHU8DKBxniZha8MFlGRlwtqCOSa\nX/6AqG1IwsV9/Z+zmJwV/PFGwQKBgQDIZzcEAC/EsmtjCrHWRDkcyMPv/BM0Eg5U\nTgpKM1EOwkhenSW2yjSLo5mh3nNZh2lpOPURvDEPpCLJEUjXcj97xkxuaPcgic4k\nQq8ao12pr8B1wykuZqxTQQqpCBykqMzUza3RSsbosTL6DZKh32KSFPCoHm1cYV0i\nUjXXc75WCQKBgAyUEZRT6AJIz/CNYU6URYtDe3uRSwfNVApS4QyFSuWfbk5omvsF\nApZNU1xMP5sRc4AohUnCSPEHIWVp1wvJQbzXEK8qWQPj8pwdHmMWPoJnqYr8YuCF\nQI0ZvGnopq4i/ZTU2Y72CFdo68yPNQsyMvdN6wvoEjrwlVyuygooS+iBAoGBAMgn\nBjSRtyPeCxEFSm60/tG31rImckuMSRF8TByluixicINGsrcop2hcAoI/ubdOXkKA\nZ4vKCLoVdcSZ5cWATtVhTU7suP2fhOqES3zwcwiaBz/Wppe4Zh5UlMAT4P+3s7RK\npHKnG5il+kLyMWRIxpseHOncUd91Qt6XezzvVcd5AoGBAK1BULtIAMeNvSotdaak\ndZbrUyLbbr5oQgxlBmx4Wr90NxiD6xqrPUk2Go1pV7rGnHQfN2gFpWnmpQj9OLFH\njmx6mwZKBLaG0PqhjN0ObQxeP/NMDCmYnDe4DXfP2RVVhBBrTCJT5hVIk/cOGuak\npFAOVmIUTJGwbi0GKgWfhTQM\n-----END PRIVATE KEY-----\n",
    "client_email": "housingdataanalysis@housingdataanalysis.iam.gserviceaccount.com",
    "client_id": "100429455219898402347",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/housingdataanalysis%40housingdataanalysis.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    }

    gcp_credentials = service_account.Credentials.from_service_account_info(gcp_credentials_dict)

    # API Key and Bucket Name
    # e2f6093efe653fa0cf74a5902912f64a, a32aebb47b8078c9d90e487e5f816cf7
    API_KEY = 'e2f6093efe653fa0cf74a5902912f64a'  # Replace with your actual API key
    BUCKET_NAME = 'landing_bucket17'

    # Fetch and save property data
    fetch_and_save_property_data(API_KEY, BUCKET_NAME, gcp_credentials)

if __name__ == "__main__":
    main()