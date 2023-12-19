import pandas as pd
from utils import GCPUtils

class TransformData:
    def __init__(self, read_bucket, write_bucket, gcp_utils):
        self.read_bucket = read_bucket
        self.write_bucket = write_bucket
        self.gcp_utils = gcp_utils

    def read_cleaned_data(self):
        print(f"Reading Cleaned Data from bucket {self.read_bucket}")
        filename = "cleaned_data.csv"
        cleaned_df = self.gcp_utils.read_data_from_gcs(filename, self.read_bucket)
        return cleaned_df

    def create_property_data(self, df):
        property_cols = ['property_id', 'lot_size_sqrft', 'property_address', 'property_country',
                        'latitude', 'longitude', 'property_type', 'propsubtype', 'property_year_built']
        property_df = df[property_cols].copy()
        property_df.columns = ['Property_id', 'Lot_size_sqrft', 'Property_address', 'Property_country',
                            'Latitude', 'Longitude', 'Property_type', 'Propsubtype', 'Property_year_built']

        # Fill NaN with the mode of the column
        mode_year_built = property_df['Property_year_built'].mode().iloc[0]
        property_df['Property_year_built'] = property_df['Property_year_built'].fillna(mode_year_built)

        property_df['Property_year_built'] = property_df['Property_year_built'].astype('Int64')
        property_df.drop_duplicates(subset='Property_id', inplace=True)
        return property_df

    def create_sale_data(self, df, property_df):
        sale_cols = ['property_id', 'saleAmt', 'saleRecDate', 'saleTransType']
        sale_df = df[sale_cols].copy()
        sale_df.insert(0, 'Sale_ID', range(1, 1 + len(sale_df)))  # Adding a unique Sale_ID
        sale_df.columns = ['Sale_ID', 'Property_id', 'SaleAmt', 'SaleRecDate', 'SaleTransType']
        sale_df['SaleRecDate'] = pd.to_datetime(sale_df['SaleRecDate']).dt.date  # Formatting date
        sale_df = sale_df[sale_df['Property_id'].isin(property_df['Property_id'])]  # Ensuring foreign key constraint
        return sale_df

    def create_tax_data(self, df, property_df):
        tax_cols = ['property_id', 'taxAmt', 'taxYear']
        tax_df = df[tax_cols].copy()
        tax_df.insert(0, 'Tax_ID', range(1, 1 + len(tax_df)))  # Adding a unique Tax_ID
        tax_df.columns = ['Tax_ID', 'Property_id', 'TaxAmt', 'TaxYear']
        tax_df = tax_df.astype({'TaxYear': 'Int64'})  # Ensuring correct data type
        tax_df = tax_df[tax_df['Property_id'].isin(property_df['Property_id'])]  # Ensuring foreign key constraint
        return tax_df

    def create_value_assessment_data(self, df, property_df):
        assessment_cols = ['property_id', 'assdImprValue', 'assdLandValue', 'assdTtlValue',
                        'mktImprValue', 'mktLandValue', 'mktTtlValue', 'improvementPercent']
        assessment_df = df[assessment_cols].copy()
        assessment_df.insert(0, 'Assessment_ID', range(1, 1 + len(assessment_df)))  # Adding a unique Assessment_ID
        assessment_df.columns = ['Assessment_ID', 'Property_id', 'AssdImprValue', 'AssdLandValue',
                                'AssdTtlValue', 'MktImprValue', 'MktLandValue', 'MktTtlValue', 'ImprovementPercent']
        assessment_df = assessment_df[assessment_df['Property_id'].isin(property_df['Property_id'])]  # Ensuring foreign key constraint
        return assessment_df

    def create_property_details_data(self, df, property_df):
        details_cols = ['property_id', 'coolingtype', 'heatingtype', 'wallType', 'grosssize',
                        'grosssizeadjusted', 'baths_total', 'baths_full', 'bed', 'rooms_total',
                        'condition', 'levels', 'zipcode', 'state']
        details_df = df[details_cols].copy()
        details_df.columns = ['Property_id', 'Coolingtype', 'Heatingtype', 'WallType', 'Grosssize',
                            'Grosssizeadjusted', 'Baths_total', 'Baths_full', 'Bed', 'Rooms_total',
                            'Condition', 'Levels', 'zipcode', 'state']

        # Convert baths_total and baths_full to string
        details_df['Baths_total'] = details_df['Baths_total'].fillna(0).apply(lambda x: f"{x} baths")
        details_df['Baths_full'] = details_df['Baths_full'].fillna(0).apply(lambda x: f"{x} full baths")

        # Ensure all other integer columns are properly formatted
        int_columns = ['Grosssize', 'Grosssizeadjusted', 'Bed', 'Rooms_total', 'Levels']
        for col in int_columns:
            mode_val = details_df[col].dropna().mode().iloc[0]
            details_df[col] = details_df[col].fillna(mode_val)
            details_df[col] = details_df[col].astype('Int64')

        details_df = details_df[details_df['Property_id'].isin(property_df['Property_id'])]
        return details_df

    def create_ownership_data(self, df, property_df):
        ownership_cols = ['property_id', 'absenteeInd']
        ownership_df = df[ownership_cols].copy()
        ownership_df.insert(0, 'Ownership_ID', range(1, 1 + len(ownership_df)))  # Adding a unique Ownership_ID
        ownership_df.columns = ['Ownership_ID', 'Property_id', 'AbsenteeInd']
        ownership_df = ownership_df[ownership_df['Property_id'].isin(property_df['Property_id'])]  # Ensuring foreign key constraint
        return ownership_df

    def write_transformed_data(self, dataframe, filename):
        file = filename
        self.gcp_utils.save_data_to_gcs(dataframe, self.write_bucket, file)


def transform_data_main():
    gcp_utils = GCPUtils()
    read_bucket = 'staging_bucket17'
    write_bucket = 'transformed_data17'

    transform_data = TransformData(read_bucket, write_bucket, gcp_utils)
    cleaned_df = transform_data.read_cleaned_data()

    property_data = transform_data.create_property_data(cleaned_df)
    sale_data = transform_data.create_sale_data(cleaned_df, property_data)
    tax_data = transform_data.create_tax_data(cleaned_df, property_data)
    value_assessment_data = transform_data.create_value_assessment_data(cleaned_df, property_data)
    property_details_data = transform_data.create_property_details_data(cleaned_df, property_data)
    ownership_data = transform_data.create_ownership_data(cleaned_df, property_data)

    transform_data.write_transformed_data(property_data, 'property_data.csv')
    transform_data.write_transformed_data(sale_data, 'sale_data.csv')
    transform_data.write_transformed_data(tax_data, 'tax_data.csv')
    transform_data.write_transformed_data(value_assessment_data, 'value_assessment_data.csv')
    transform_data.write_transformed_data(property_details_data, 'property_details_data.csv')
    transform_data.write_transformed_data(ownership_data, 'ownership_data.csv')


if __name__ == "__main__":
    transform_data_main()