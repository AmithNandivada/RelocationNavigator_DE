import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account

class GCPUtils:
    def __init__(self):
        self.credentials = self.get_credentials()

    def read_data_from_gcs(self, file, bucket_name):
        print(f"Reading data from GCS.....")
        client = storage.Client.from_service_account_info(self.credentials)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file)
        csv_data = blob.download_as_text()
        dataframe = pd.read_csv(BytesIO(csv_data.encode()))
        print(f"Finished reading data from {file} in bucket {bucket_name}")
        return dataframe

    def save_data_to_gcs(self, dataframe, bucket_name, file_path):
        print(f"Writing data to GCS.....")
        client = storage.Client.from_service_account_info(self.credentials)
        csv_data = dataframe.to_csv(index=False)
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.upload_from_string(csv_data, content_type='text/csv')
        print(f"Finished writing data to {file_path} in bucket {bucket_name}.")

    @staticmethod
    def get_credentials():
        # Replace with your actual credentials information
        creds_info1 = {
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

        return creds_info1