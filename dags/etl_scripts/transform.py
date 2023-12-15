import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')

# creating the global mapping for outcome types
outcomes_map = {'Rto-Adopt':1, 
                'Adoption':2, 
                'Euthanasia':3, 
                'Transfer':4,
                'Return to Owner':5, 
                'Died':6,
                'Disposal':7,
                'Missing': 8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}


def get_credentials():
    bucket_name = "datacenter1798"
    credentials_info = {
                    "type": "service_account",
                    "project_id": "warm-physics-405522",
                    "private_key_id": "81fa8d3a1b7492d9a69dd23d64d34a925103ed0f",
                    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDIg1et67MkPfDn\nmFRgQE5zfExjKdX7LHNvVFYACX6kN6lE6WRaKmxlnWBmpGsRcNRJ4Wo6HSV6NOao\nfmeKnefZ3dc5kQjmaxdxxVQZlU0ie16Ynsj+VlmXR4z2FihCXI8jm+m0LmcaaWbe\nz2/eT56uC5dL+vfijg33m76fit5kI/+X77LSlPV7lxkiKkboJFk+NKaU7tvpyH4i\nmF1866ZVWHt5rihM3f9e0h/1waByssDIsmjYtdJmGTvvvReARMMYvdKQduWQSn8r\ntuVLLTG+BrUNaACDO85aefrvmkkUtaNYMlzmYo6hF+hivkgXQjXbekS17/ObsIpG\nKVrHTXpPAgMBAAECggEAI1D25XwpLk32m2P6IIXTC4YuEh0xQi8fGdG54AHMG3Ju\nTuPot/TW6MLiUtHMxeKgkW6xfhDaI/8jTTQOWpzbVEU9fjcsYSElnPVLjcH9NwCR\ntcHp1towp3ODwWg/qQiScYwpioHNyRodc0sIAhj18uO5vzkx5eZtUVpOJd2Ys/xP\nJbleJBT8U7e0GbcRg48HZ0a5TmaBMOGUTH+3Octj+AQcojxiI05QMO8fpGlkilD0\nbvVZIi9f1c32esJ+fCzrkUKdmiOoDtxYscyvQU4K11S6oxHr5pXdNnX8mA3pp0AP\nEcsiN7P95+6TmPLUpTbWTfYoK6233U7vOfJjbFvZOQKBgQDz/rpTJ7mw/kjRzW6t\nqeGFFSnBV/srnHoNOme0NP2/SWh1EmYPoNM2ihT6ooCY5PXKpXoL+1npESgk3PbM\nSmtAVdiyY7trnb3i43mgOEdMXSkokjp2B9dW7yYwe2ONOarQpro0f2aoTqY0CCHW\ng/viIBn42OKwIo52I1TswVD7AwKBgQDSYO5++QI/TnFvSU6zJ8K1JhcO6F6SwJ1Y\nntM7T7jnCaEZ+pZPR/FskLZNXeBhDFGgMzFw8jHtm7Gh+t4KevJ3d/78+9iuKkBU\nBfDo8maXZHfAsUeZxgbE5tqhqvW8myds7AO+3G8IuMMyW30df2otZAd4guajxOtr\nm8ZQ/4EbxQKBgQDbpI8ulDBA+GetFfVwN+Ff3/E6r2zXkYD9r3nza1CRhg+Wc/2U\nS/5Wtm60QNzqxhHNXrFDX/1MJbmxlYhF1yg9PgpYbBcnhVSOjp/Kb18fiy2l7Bzc\na6qaA6apNioj06nFMpGk+Jr9H+/WHwv3A9EXejZnITbPwAvmpV+p0UyI6QKBgQCh\nFAyn9XquBB7Aaa2zaNchIif3hx2aWZZgG0N6n0DgzTOnk4Fw9JG6YVbkB+PcCrWY\n5nmNlDN8TYCFmHJYLejmZl87To2KVNlqPB5IDglVE1zJkjNTXxchvexaam662UUn\nldIMWfU+BVGXhgtXAY7HcFZ0BC4Z6JWkj+IZdHhjTQKBgGMevGX290oV4tO1Fd1J\ntuf0CBNmuDkeImxjXu/RqAxJANNpOVZDQ0HMOB8R94LghZstXoSDRjGfWv+fGlJe\nu6oeWd1WxKNHGSkCi9lKQa+/xBkXe/gGm8kKT12XmBc3RZdiGMkmBOOcBQ6t1opX\nTnjNgClRMXpP6krPqDL8+1bB\n-----END PRIVATE KEY-----\n",
                    "client_email": "accessdatacenterdata@warm-physics-405522.iam.gserviceaccount.com",
                    "client_id": "115239160111022688572",
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/accessdatacenterdata%40warm-physics-405522.iam.gserviceaccount.com",
                    "universe_domain": "googleapis.com"
                }
    return credentials_info, bucket_name


def connect_to_gcs_and_get_data(credentials_info, gcs_bucket_name):
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    client = storage.Client.from_service_account_info(credentials_info)
    
    bucket = client.get_bucket(gcs_bucket_name)
    
    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    # Format the file path with the current date
    formatted_file_path = gcs_file_path.format(current_date, current_date)
    
    # Read the CSV file from GCS into a DataFrame
    blob = bucket.blob(formatted_file_path)
    csv_data = blob.download_as_text()
    df = pd.read_csv(StringIO(csv_data))

    return df


def write_data_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCS.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    # current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    # formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")
    
    
def prep_data(data):
    # remove stars from animal names. Need regex=False so that * isn't read as regex
    data['name'] = data['name'].str.replace("*","",regex=False)

    # separate the "sex upon outcome" column into property of an animal (male or female) 
    # and property of an outcome (was the animal spayed/neutered at the shelter or not)
    data['sex'] = data['sex_upon_outcome'].replace({"Neutered Male":"M",
                                                    "Intact Male":"M", 
                                                    "Intact Female":"F", 
                                                    "Spayed Female":"F", 
                                                    "Unknown":np.nan})

    data['is_fixed'] = data['sex_upon_outcome'].replace({"Neutered Male":True,
                                                        "Intact Male":False, 
                                                        "Intact Female":False, 
                                                        "Spayed Female":True, 
                                                        "Unknown":np.nan})

    # prepare the data table for introducing the date dimension
    # we'll use condensed date as the key, e.g. '20231021'
    # time can be a separate dimension, but here we'll keep it as a field
    data['ts'] = pd.to_datetime(data.datetime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time

    # prepare the data table for introducing the outcome type dimension:
    # introduce keys for the outcomes
    data['outcome_type_id'] = data['outcome_type'].fillna("N/A")
    data['outcome_type_id'] = data['outcome_type'].replace(outcomes_map)

    return data


def prep_animal_dim(data):
    print("Preparing Animal Dimensions Table Data")
    # extract columns only relevant to animal dim
    animal_dim = data[['animal_id','name','date_of_birth', 'sex', 'animal_type', 'breed', 'color']]
    
    # rename the columns to agree with the DB tables
    animal_dim.columns = ['animal_id', 'name', 'dob', 'sex', 'animal_type', 'breed', 'color']

    mode_sex = animal_dim['sex'].mode().iloc[0]
    animal_dim['sex'] = animal_dim['sex'].fillna(mode_sex)
    
    # drop duplicate animal records
    return animal_dim.drop_duplicates()


def prep_date_dim(data):
    print("Preparing Date Dimension Table Data")
    # use string representation as a key
    # separate out year, month, and day
    dates_dim = pd.DataFrame({
        'date_id':data.ts.dt.strftime('%Y%m%d'),
        'date':data.ts.dt.date,
        'year':data.ts.dt.year,
        'month':data.ts.dt.month,
        'day':data.ts.dt.day,
        })
    return dates_dim.drop_duplicates()


def prep_outcome_types_dim(data):
    print("Preparing Outcome Types Dimension Table Data")
    # map outcome string values to keys
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
    # keep only the necessary fields
    outcome_types_dim.columns=['outcome_type', 'outcome_type_id']    
    return outcome_types_dim


def prep_outcomes_fct(data):
    print("Preparing Outcome Fact Table Data")
    # pick the necessary columns and rename
    outcomes_fct = data[["animal_id", 'date_id','time', 'outcome_type_id', 'is_fixed']]
    return outcomes_fct


def transform_data():
    credentials_info, bucket_name = get_credentials()

    new_data = connect_to_gcs_and_get_data(credentials_info, bucket_name)
    
    new_data = prep_data(new_data)
    
    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)

    fct_outcomes = prep_outcomes_fct(new_data)

    dim_animal_path = "transformed_data/dim_animal.csv"
    dim_dates_path = "transformed_data/dim_dates.csv"
    dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
    fct_outcomes_path = "transformed_data/fct_outcomes.csv"

    write_data_to_gcs(dim_animal, credentials_info, bucket_name, dim_animal_path)
    write_data_to_gcs(dim_dates, credentials_info, bucket_name, dim_dates_path)
    write_data_to_gcs(dim_outcome_types, credentials_info, bucket_name, dim_outcome_types_path)
    write_data_to_gcs(fct_outcomes, credentials_info, bucket_name, fct_outcomes_path)