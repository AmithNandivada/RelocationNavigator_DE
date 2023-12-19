import pandas as pd
from io import BytesIO
from utils import GCPUtils
from google.cloud import storage


class CleanData:
    def __init__(self, read_bucket, write_bucket, gcp_utils):
        self.read_bucket = read_bucket
        self.write_bucket = write_bucket
        self.gcp_utils = gcp_utils
        self.storage_client = storage.Client.from_service_account_info(self.gcp_utils.credentials)

    def read_and_combine_csv_from_gcs(self):
        bucket = self.storage_client.bucket(self.read_bucket)
        blobs = bucket.list_blobs()

        combined_df = pd.DataFrame()
        for blob in blobs:
            if blob.name.endswith('.csv'):
                content = blob.download_as_text()
                if content.strip():
                    df = pd.read_csv(BytesIO(content.encode()))
                    combined_df = pd.concat([combined_df, df], ignore_index=True)

        return combined_df

    def process_dataframe(self, df):
        print("Cleaning Data....")
        num_rows, num_columns = df.shape
        print(f"Number of rows: {num_rows}")
        print(f"Number of columns: {num_columns}")

        print(df.info())

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
        # df['saleAmt'] = df['saleAmt'].fillna(df['saleAmt'].mean())

        # Dropping additional specified columns
        columns_to_drop = ['saleSearchDate', 'saleTransDate', 'transactionIdent', 'saleDocNum', 'propIndicator', 
                           'pubDate', 'lastModified', 'universalsize', 'bldgsize', 'constructiontype', 'view', 
                           'lot_size_acres', 'proptype', 'livingsize', 'taxPerSizeUnit']
        df.drop(columns=columns_to_drop, inplace=True)

        # Filling certain columns with 'None', mode, mean, or zero as specified
        columns_to_fillna_with_none = ['coolingtype']
        columns_to_fillna_with_mode = ['heatingtype', 'wallType', 'saleTransType', 'rooms_total', 'bed', 'condition',
                                       'levels', 'lot_size_sqrft', 'grosssize', 'grosssizeadjusted', 'assdLandValue',
                                       'assdTtlValue', 'mktImprValue', 'mktLandValue', 'assdImprValue', 'mktTtlValue',
                                       'taxAmt', 'improvementPercent', 'property_year_built', 'saleRecDate','baths_total',
                                       'taxYear', 'absenteeInd', 'saleAmt']
        columns_to_replace_with_zero = ['baths_full']

        for column in columns_to_fillna_with_none:
            df[column].fillna('None', inplace=True)
        for column in columns_to_fillna_with_mode:
            mode_value = df[column].mode()[0]
            df[column].fillna(mode_value, inplace=True)
        # for column in columns_to_fillna_with_mean:
        #     mean_value = df[column].mean()
        #     df[column].fillna(mean_value, inplace=True)
        for column in columns_to_replace_with_zero:
            df[column].fillna(0, inplace=True)

        # Additional specific replacements
        df.loc[df['heatingtype'] == 'YES', 'heatingtype'] = "OTHER"
        df.loc[df['coolingtype'] == 'YES', 'coolingtype'] = "OTHER"
        df.loc[df['heatingtype'] == 'None', 'heatingtype'] = "NONE"
        df.loc[df['coolingtype'] == 'None', 'coolingtype'] = "NONE"

        df = df.dropna(subset=['latitude', 'longitude'])

        print("Finished Cleaning Data.")
        num_rows, num_columns = df.shape
        print(f"Number of rows: {num_rows}")
        print(f"Number of columns: {num_columns}")

        print(df.info())

        return df

    def write_cleaned_data(self, dataframe):
        file_name = f"cleaned_data.csv"
        self.gcp_utils.save_data_to_gcs(dataframe, self.write_bucket, file_name)


def clean_data_main():
    gcp_utils = GCPUtils()
    read_bucket = 'merged_bucket17'
    write_bucket = 'staging_bucket17'

    clean_data = CleanData(read_bucket, write_bucket, gcp_utils)
    combined_df = clean_data.read_and_combine_csv_from_gcs()
    cleaned_df = clean_data.process_dataframe(combined_df)
    clean_data.write_cleaned_data(cleaned_df)


if __name__ == "__main__":
    clean_data_main()