import data_cleaning
import data_extraction
import database_utils
import argparse
import pandas as pd
from config import api_key

headers = {"x-api-key": api_key}

def main():
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
    creds_file = "db_creds.yaml"
    db_credentials_rds = database_utils.DatabaseConnector.read_db_creds(creds_file, env="RDS")
    de = data_extraction.DataExtractor()
    db_credentials_local = database_utils.DatabaseConnector.read_db_creds(creds_file, env="LOCAL")
    de = data_extraction.DataExtractor()
    dc_rds = database_utils.DatabaseConnector(host=db_credentials_rds["host"],
                                          port=db_credentials_rds["port"],
                                          database=db_credentials_rds["database"],
                                          user=db_credentials_rds["user"],
                                          password=db_credentials_rds["password"]                                        
                                        )
    dc_local = database_utils.DatabaseConnector(host=db_credentials_local["host"],
                                          port=db_credentials_local["port"],
                                          database=db_credentials_local["database"],
                                          user=db_credentials_local["user"],
                                          password=db_credentials_local["password"]                                        
                                        )
    db_engine_local = dc_local.init_db_engine(env="LOCAL")
    
    if args.type == "user":
        data = de.read_rds_table(dc_rds, "legacy_users")

        if data is not None:
            cleaned_data = data_cleaning.DataCleaning.clean_user_data(data)

            dc_local.upload_to_db(cleaned_data, "dim_users", db_engine_local)
        else:
            print("No data found in the 'legacy_users column")

    if args.type == "card":
        url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        data = de.retrieve_pdf_data(url)
    
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        dc_local.upload_to_db("dim_card_details", cleaned_data, db_engine_local)

    if args.type == "store":
        data = data_extraction.DataExtractor.retrieve_stores_data()
        cleaned_data = data_cleaning.DataCleaning.clean_store_data(data)
        dc_local.upload_to_db("dim_store_details", cleaned_data)

    if args.type == "product":
        data = data_extraction.DataExtractor.extract_from_s3(
            "s3://data-handling-public/products.csv"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_product_data(data)
        dc_local.upload_to_db("dim_products", cleaned_data)

    if args.type == "order":
        de = data_extraction.DataExtractor()
        data = de.read_rds_table("orders_table")
        cleaned_data = data_cleaning.DataCleaning.clean_orders_data(data)
        dc_local.upload_to_db("orders_table", cleaned_data)

    if args.type == "date_event":
        data = data_extraction.DataExtractor.extract_from_s3(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )
        cleaned_data = data_cleaning.DataCleaning.clean_card_data(data)
        database_utils.DatabaseConnector.upload_to_db("dim_date_times", cleaned_data)


if __name__ == "__main__":
    main()