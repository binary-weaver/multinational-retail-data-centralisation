import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from sqlalchemy import create_engine, inspect, text
import requests
import json
import boto3
import tabula
import pdfplumber
from io import BytesIO


class DataExtractor:
    """
    Class for extracting data from various sources.
    """

    def __init__(self, db_connector=None):
        """
        Initializes the DataExtractor instance.

        Args:
            db_connector (DatabaseConnector, optional): DatabaseConnector instance. If not provided,
                                                        credentials will be read from 'db_creds.yaml'.
        """
        if db_connector is not None and isinstance(db_connector, DatabaseConnector):
            self.db_connector = db_connector
        else:
            db_creds = DatabaseConnector.read_db_creds("db_creds.yaml")
            self.db_connector = DatabaseConnector(
                host=db_creds.get('host',),
                port=db_creds.get('port'),
                database=db_creds.get('database'),
                user=db_creds.get('user'),
                password=db_creds.get('password', env="RDS")
            )

    @staticmethod
    def read_rds_table(table_name):
        """
        Reads a table from RDS into a Pandas DataFrame.

        Args:
            table_name (str): Name of the table to read from RDS.

        Raises:
            ValueError: If the table cannot be read.

        Returns:
            pd.DataFrame: DataFrame containing table data.
        """
        
        engine = DatabaseConnector.init_db_engine()
        return pd.read_sql_table(table_name, engine)
         
    @staticmethod
    def retrieve_pdf_data_2(
        URL="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf",
    ):
        """
        Uses tabula-py to read a PDF document's table into a DataFrame.

        Args:
            URL (str): Location of PDF.

        Returns:
            pd.DataFrame: DataFrame containing PDF table data.
        """
        all_pages = tabula.read_pdf(URL, pages="all")
        df_pdf = pd.concat(all_pages, ignore_index=True, join="inner")
        return df_pdf

    @staticmethod
    def list_number_of_stores(number_of_stores_endpoint, headers):
        """
        Lists the number of stores using a provided API endpoint and headers.

        Args:
            number_of_stores_endpoint (str): API endpoint for obtaining the number of stores.
            headers (dict): Request headers.

        Returns:
            int: Number of stores.
        """
        r = requests.get(number_of_stores_endpoint, headers=headers)
        return json.loads(r.text)["number_stores"] 

   
    @staticmethod
    def retrieve_stores_data(
        headers=None,
        base_URL="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}",
        n_stores_API_endpoint="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
        ):
        """
        Retrieves store data from multiple API endpoints.

        Args:
            headers (dict, optional): Request headers. Defaults to None.
            base_URL (str, optional): Base URL for store details. Defaults to a preset URL.
            n_stores_API_endpoint (str, optional): API endpoint for obtaining the number of stores.
                                                    Defaults to a preset URL.

        Returns:
            pd.DataFrame: DataFrame containing store information.
        """
        api_key = "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
        headers = {"x-api-key": api_key}

        # Corrected variable name
        n = DataExtractor.list_number_of_stores(n_stores_API_endpoint, headers)
        if n is None:
            return None  # or raise an exception

        endpoints_list = [base_URL.format(store_number=i) for i in range(n)]

        response_list = []
        for URL_string in endpoints_list:
            r = requests.get(URL_string, headers=headers)
            if r.status_code == 200:
                response_list.append(r.json())
        df_stores_info = pd.DataFrame(response_list)
        return df_stores_info


    def extract_from_s3():
        """
        Extracts data from an S3 bucket.

        Returns:
            pd.DataFrame: DataFrame containing S3 data.
        """
        s3 = boto3.client("s3")
        bucket_name="data-handling-public"
        file_key = "products.csv"
        local_file_path="./products.csv"
        s3.download_file(bucket_name, file_key, local_file_path)
        df_products = pd.read_csv(local_file_path)
        print(df_products.head())
        return df_products
    
    @staticmethod
    def extract_json_from_URL(
        endpoint_URL="https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json",
    ):
        """
        Extracts JSON data from a URL into a Pandas DataFrame.

        Args:
            endpoint_URL (str, optional): The URL where the JSON data is located. Defaults to a preset URL.

        Returns:
            pd.DataFrame: DataFrame containing the JSON data.
        """
        response = requests.get(endpoint_URL).text
        j = json.loads(response)
        df_date_events = pd.DataFrame(j)
        return df_date_events