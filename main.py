import argparse
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
import pandas as pd
import psycopg2
import yaml
from config import Config
from sqlalchemy import create_engine, text
import traceback


headers = {"x-api-key": Config.api_key}

def main():
    """ETL Data Centralisation Tool: Extract, Transform, and Load data into a local database.

    This script performs data extraction, cleaning, and uploading into a local database based on the specified data type.

    Command-line Arguments:
    - type: The type of data to clean, one of ["user", "card", "store", "product", "order", "date_event"].

    Usage Example:
    python main.py user
    """
    try:

        parser = argparse.ArgumentParser(description="ETL Data Centralisation Tool")

        parser.add_argument(
            "type",
            type=str,
            choices=["user", "card", "store", "product", "order", "date_event"],
            help="""The type of data to clean:
                                    - user: Cleans user data by removing duplicates, formatting dates, etc.
                                    - card: Cleans card data, validates card numbers, etc.
                                    - store: Cleans store data, removes redundant columns, etc.
                                    - product: Cleans product data, standardizes weight units, etc.
                                    - order: Cleans order data, removes incomplete rows, etc.
                                    - date_event: Cleans date event data, combines date and time fields, etc.""",
        )

        args = parser.parse_args()

        # Database Connection
        db_credentials_rds = DatabaseConnector.read_db_creds("db_creds.yaml", env="RDS")
        db_credentials_local = DatabaseConnector.read_db_creds("db_creds.yaml", env="LOCAL")
        dc_rds = DatabaseConnector(host=db_credentials_rds['host'], port=db_credentials_rds['port'], user=db_credentials_rds['user'], password=db_credentials_rds['password'], database=db_credentials_rds['database'])
        dc_local = DatabaseConnector(host=db_credentials_local['host'], port=db_credentials_local['port'], user=db_credentials_local['user'], password=db_credentials_local['password'], database=db_credentials_local['database'])
        db_engine_rds = dc_rds.init_db_engine(env="RDS")
        db_engine_local = dc_local.init_db_engine(env="LOCAL")
        
    
        if args.type == "user":
                # Data Extraction
                df_user = DataExtractor.read_rds_table("legacy_users")

                if df_user is not None:
                    # Data Cleaning
                    df_user_cleaned = DataCleaning.clean_user_data(df_user)
                    # Data Upload
                    dc_local.upload_to_db(df_user_cleaned, "dim_users_table", db_engine_local)     
                    DataCleaning.change_dim_users_column_types(db_engine_local)
                    DataCleaning.add_primary_key(db_engine_local, "dim_users_table", "user_uuid")
                    result = DataCleaning.check_primary_key(db_engine_local, 'dim_users_table', 'user_uuid')
                    print(f"Primary key present: {result}")
                   
                else:
                    print("No data found in the 'legacy_users' column")

        if args.type == "card":
            # Data Extraction
            url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
            df_card = DataExtractor.retrieve_pdf_data_2(url)

            # Data Cleaning
            df_card_cleaned = DataCleaning.clean_card_data(df_card)

            # Data Upload
            dc_local.upload_to_db(df_card_cleaned, "dim_card_details", db_engine_local)
            DataCleaning.change_dim_card_details_types(db_engine_local)
            DataCleaning.add_primary_key(db_engine_local, "dim_card_details", "card_number")
            

        if args.type == "store":
            # Data Extraction
            df_store = DataExtractor.retrieve_stores_data(headers)
    
            # Data Cleaning
            df_store_cleaned = DataCleaning.clean_store_data(df_store)
            print(df_store_cleaned.columns)

            # Data Upload
            dc_local.upload_to_db(df_store_cleaned, "dim_store_details", db_engine_local)
            DataCleaning.merge_latitudes(db_engine_local) 
            DataCleaning.change_dim_store_details_column_types(db_engine_local)
            DataCleaning.add_primary_key(db_engine_local, "dim_store_details", "store_code")
           


        if args.type == "product":
            # Data Extraction
            data = DataExtractor.extract_from_s3()
            print(type(data))
            # Data Cleaning
            df_cleaned = pd.DataFrame(DataCleaning.clean_product_data(data))
            # Data Upload
            dc_local.upload_to_db(df_cleaned, "dim_products", db_engine_local)
            DataCleaning.clean_product_price(db_engine_local)
            DataCleaning.add_weight_class_column(db_engine_local)
            DataCleaning.change_dim_product_column_types(db_engine_local)
            DataCleaning.add_primary_key(db_engine_local, "dim_products", "product_code")
            
        if args.type == "order":
            # Data Extraction
            df_orders = DataExtractor.read_rds_table("orders_table")

            # Data Cleaning
            df_orders_cleaned = DataCleaning.clean_orders_data(df_orders)

            # Data Upload
            dc_local.upload_to_db(df_orders_cleaned, "orders_table", db_engine_local)
            DataCleaning.change_orders_table_column_types(db_engine_local)
            drop_card_query = text('''
                                    DELETE FROM orders_table
                                    WHERE card_number IS NOT NULL 
                                    AND card_number NOT IN (SELECT card_number FROM dim_card_details); 
                                   ''')
            DataCleaning.execute_sql_query(db_engine_local, drop_card_query)

        if args.type == "date_event":
            
            # Data Extraction
            url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
            df_date_events = DataExtractor.extract_json_from_URL()

            # Data Cleaning
            df_date_events_cleaned = DataCleaning.clean_date_events(df_date_events)

            # Data Upload
            dc_local.upload_to_db(df_date_events_cleaned, "dim_date_times", db_engine_local)
            DataCleaning.change_dim_date_times_types(db_engine_local)
            DataCleaning.add_primary_key(db_engine_local, "dim_date_times", "date_uuid")
        
        DataCleaning.create_foreign_keys(db_engine_local)

    except psycopg2.Error as pg_error:
        print(f"PostgreSQL Error: {pg_error}")
        print(traceback.format_exc())
    except FileNotFoundError as file_not_found_error:
        print(f"File Not Found Error: {file_not_found_error}")
        print(traceback.format_exc())
    except yaml.YAMLError as yaml_error:
        print(f"YAML Error: {yaml_error}")
        print(traceback.format_exc())
    except Exception as general_error:
        print(f"An unexpected error occurred: {general_error}")
        print(traceback.format_exc())


if __name__ == "__main__":
    main()