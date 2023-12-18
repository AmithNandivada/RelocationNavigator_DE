import pandas as pd
from io import BytesIO
from google.cloud import storage
from google.oauth2 import service_account

def get_gcp_credentials():
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

    # return service_account.Credentials.from_service_account_info(creds_info1)
    return creds_info1

def initialize_gcs_client(credentials_info):
    return storage.Client.from_service_account_info(credentials_info)

def read_and_combine_csv_from_gcs(bucket_name, client):
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    combined_df = pd.DataFrame()
    for blob in blobs:
        if blob.name.endswith('.csv'):
            content = blob.download_as_text()
            if content.strip():
                df = pd.read_csv(BytesIO(content.encode()))
                combined_df = pd.concat([combined_df, df], ignore_index=True)

    return combined_df

def process_dataframe(df):
    # Dropping columns with a high percentage of null values, except 'coolingtype'
    threshold_percentage = 60
    null_percentages = (df.isnull().sum() / len(df)) * 100
    columns_to_drop = [column for column, null_percentage in null_percentages.items() 
                       if column.lower() != 'coolingtype' and null_percentage > threshold_percentage]
    df = df.drop(columns=columns_to_drop)

    # Ensuring desired column order and dropping missing columns
    desired_columns_order = ['property_id', 'lot_size_acres', 'lot_size_sqrft', 'pool_access',
       'property_address', 'property_country', 'latitude', 'longitude',
       'property_type', 'propsubtype', 'propIndicator', 'proptype',
       'property_year_built', 'absenteeInd', 'coolingtype', 'heatingtype',
       'wallType', 'saleSearchDate', 'saleTransDate', 'transactionIdent',
       'saleAmt', 'saleCode', 'saleRecDate', 'saleDocType', 'saleDocNum',
       'saleTransType', 'universalsize', 'bldgsize', 'grosssize',
       'grosssizeadjusted', 'groundfloorsize', 'livingsize', 'baths_total',
       'baths_partial', 'baths_full', 'bed', 'rooms_total', 'bsmtsize',
       'bsmttype', 'bsmtFinishedPercent', 'condition', 'constructiontype',
       'frameType', 'roofcover', 'garagetype', 'prkgSize', 'prkgType',
       'prkgSpaces', 'levels', 'bldgType', 'view', 'archStyle', 'quality',
       'yearbuilteffective', 'assdImprValue', 'assdLandValue', 'assdTtlValue',
       'mktImprValue', 'mktLandValue', 'mktTtlValue', 'taxAmt',
       'taxPerSizeUnit', 'taxYear', 'delinquentyear', 'improvementPercent',
       'lastModified', 'pubDate']
    df = df[[col for col in desired_columns_order if col in df.columns]]

    # Replacing missing values in 'saleAmt' with its mean
    df['saleAmt'] = df['saleAmt'].fillna(df['saleAmt'].mean())

    # Dropping additional specified columns
    columns_to_drop = ['saleSearchDate', 'saleTransDate', 'transactionIdent', 'saleDocNum', 'propIndicator', 
                       'pubDate', 'lastModified', 'universalsize', 'bldgsize', 'constructiontype', 'view']
    df.drop(columns=columns_to_drop, inplace=True)

    # Filling certain columns with 'None', mode, mean, or zero as specified
    columns_to_fillna_with_none = ['coolingtype', 'absenteeInd']
    columns_to_fillna_with_mode = ['heatingtype', 'wallType', 'saleTransType', 'rooms_total', 'bed', 
                                   'condition', 'levels']
    columns_to_fillna_with_mean = ['grosssize', 'grosssizeadjusted', 'assdLandValue', 'assdTtlValue', 
                                   'lot_size_sqrft', 'mktImprValue', 'mktLandValue', 'assdImprValue', 
                                   'mktTtlValue', 'taxAmt', 'improvementPercent']
    columns_to_replace_with_zero = ['baths_full']

    for column in columns_to_fillna_with_none:
        df[column].fillna('None', inplace=True)
    for column in columns_to_fillna_with_mode:
        mode_value = df[column].mode()[0]
        df[column].fillna(mode_value, inplace=True)
    for column in columns_to_fillna_with_mean:
        mean_value = df[column].mean()
        df[column].fillna(mean_value, inplace=True)
    for column in columns_to_replace_with_zero:
        df[column].fillna(0, inplace=True)

    # Additional specific replacements
    df.loc[df['heatingtype'] == 'YES', 'heatingtype'] = "OTHER"
    df.loc[df['coolingtype'] == 'YES', 'coolingtype'] = "OTHER"
    df.loc[df['heatingtype'] == 'None', 'heatingtype'] = "NONE"
    df.loc[df['coolingtype'] == 'None', 'coolingtype'] = "NONE"

    return df


def upload_df_to_gcs(df, bucket_name, blob_name, client):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    print(f"Data saved to {blob_name} in bucket {bucket_name}.")

def main():
    credentials_info = get_gcp_credentials()
    client = initialize_gcs_client(credentials_info)

    bucket_name = 'merged_bucket17'
    combined_df = read_and_combine_csv_from_gcs(bucket_name, client)

    processed_df = process_dataframe(combined_df)

    destination_bucket_name = 'staging_bucket17'
    destination_blob_name = 'desired_df.csv'
    upload_df_to_gcs(processed_df, destination_bucket_name, destination_blob_name, client)

if __name__ == "__main__":
    main()
