import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCSDataLoader:

    def __init__(self):
        self.bucket_name = 'datacenter1798'

    def get_credentials(self):
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
        return credentials_info

    def connect_to_gcs_and_get_data(self, file_name):
        gcs_file_path = f'transformed_data/{file_name}'

        credentials_info = self.get_credentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCS into a DataFrame
        blob = bucket.blob(gcs_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcs_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'shelterdata_db',
            'user': 'amith',
            'password': 'pgadmin17',
            'host': '34.68.213.85',
            'port': '5432',
        }

    def get_queries(self, table_name):
        if table_name =="dim_animals":
            query = """CREATE TABLE IF NOT EXISTS dim_animals (
                            animal_id VARCHAR(7) PRIMARY KEY,
                            name VARCHAR,
                            dob DATE,
                            sex VARCHAR(1), 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR
                        );
                        """
        elif table_name =="dim_outcome_types":
            query = """CREATE TABLE IF NOT EXISTS dim_outcome_types (
                            outcome_type_id INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
        elif table_name =="dim_dates":
            query = """CREATE TABLE IF NOT EXISTS dim_dates (
                            date_id VARCHAR(8) PRIMARY KEY,
                            date DATE NOT NULL,
                            year INT2  NOT NULL,
                            month INT2  NOT NULL,
                            day INT2  NOT NULL
                        );
                        """
        else:
            query = """CREATE TABLE IF NOT EXISTS fct_outcomes (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_id VARCHAR(7) NOT NULL,
                            date_id VARCHAR(8) NOT NULL,
                            time TIME NOT NULL,
                            outcome_type_id INT NOT NULL,
                            is_fixed BOOL,
                            FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
                            FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
                            FOREIGN KEY (outcome_type_id) REFERENCES dim_outcome_types(outcome_type_id)
                        );
                        """
        return query

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcs_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcs_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcs_data)}")
        
def load_data_to_postgres_main(file_name, table_name):
    gcs_loader = GCSDataLoader()
    table_data_df = gcs_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)