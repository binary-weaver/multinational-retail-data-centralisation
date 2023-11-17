import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import create_engine, inspect
import tabula 
import requests
import json
import boto3

class DataExtractor:

    def __init__(self, db_connector=None):
        # Check if a DatabaseConnector instance is provided
        if db_connector is not None and isinstance(db_connector, DatabaseConnector):
            self.db_connector = db_connector
        else:
            # If not provided, read database credentials from db_creds.yaml
            self.db_creds = DatabaseConnector.read_db_creds("db_creds.yaml")
            db_creds = self.db_creds
            # Create a new instance with the retrieved connection details
            self.db_connector = DatabaseConnector(
                host=db_creds.get('host'),
                port=db_creds.get('port'),
                database=db_creds.get('database'),
                user=db_creds.get('user'),
                password=db_creds.get('password')
            )

    @staticmethod
    def read_rds_table(db_connector, table_name):
        try:
            query = f"SELECT * FROM {table_name}"
            cursor = db_connector.conn.cursor()
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
                return df
            else:
                print(f"No data found in table '{table_name}'.")
                return None
        except Exception as error:
            print(f"Error reading RDS table: {error}")
            return None
        
    def retrieve_pdf_data(self, url):
        pdf_data = tabula.read_pdf(url, pages=all)
        df_pdf = pd.concat(pdf_data, ignore_index=True, join='inner')
        return df_pdf
    
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        r = requests.get(number_of_stores_endpoint, headers=headers)
        return json.loads(r.text)["number_stores"] 

    def retrieve_stores_data(stores_endpoint, headers):
        n = DataExtractor.list_number_of_stores(stores_endpoint, headers)
        base_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
        endpoints_list = [
            f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
            for store_number in range(n)
        ]
        response_list = [
            requests.get(URL_string, headers=headers).text
            for URL_string in endpoints_list
        ]
        response_list = [json.loads(response) for response in response_list]
        df_stores_info = pd.DataFrame(response_list)
        return df_stores_info

    def extract_from_s3(
        bucket_name="data-handling-public",
        local_file_path="./products.csv",
    ):
        s3 = s3.client("s3")
        df_products = pd.read_csv(local_file_path)
        return df_products
    


if __name__ == "__main__":

    creds_file = "C:/Users/moeza/AICore/Multinational Retail Database Centralisation/db_creds.yaml"
    db_credentials = DatabaseConnector.read_db_creds(creds_file)

    if db_credentials:
        db_connector = DatabaseConnector(
            host=db_credentials['host'],
            port=db_credentials['port'],
            database=db_credentials['database'],
            user=db_credentials['user'],
            password=db_credentials['password']
        )    

        db_engine = DatabaseConnector.init_db_engine(db_credentials)

        if db_engine:
           print("Database Engine Initialized") 
           table_list = db_connector.list_db_tables(db_engine)

        if table_list:
            user_data_table = None

            for table in table_list:
                if 'legacy_user' in table.lower():
                    user_data_table = table
                    break

            if user_data_table:
                print(f"User data found in table: {user_data_table}")
                user_data_df = DataExtractor.read_rds_table(db_connector, user_data_table)
                user_data_df = DataCleaning.clean_user_data(user_data_df)


                if user_data_df is not None:
                    print("User Data:")
                    print(user_data_df)
            else:
                print("No user data table found.")

