import pandas as pd
from data_transformations import Transformations
from sqlalchemy import create_engine, MetaData, Table, Column, PrimaryKeyConstraint, inspect, text
import traceback
import psycopg2

class DataCleaning:

    @staticmethod
    def alter_column_type(engine, table_name, column_name, new_type):
        """
        Alter the data type of a column in a table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.
            table_name (str): Name of the table.
            column_name (str): Name of the column to be altered.
            new_type (str): New data type for the column.

        Returns:
            None
        """
        alter_statement = f"""ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};"""

        try:
            # Create a connection from the engine
            with engine.connect() as conn:
                # Use the connection to execute the ALTER TABLE statement
                conn.execute(text(alter_statement))
                conn.commit()

            print(f"Column '{column_name}' type altered successfully.")
        except Exception as error:
            print(f"Error altering column type: {error}")

    @staticmethod
    def clean_user_data(df_user):
        """
        Cleans the user data DataFrame.

        Args:
            df_user (pd.DataFrame): DataFrame containing raw user data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned user data.
        """
        if "index" in df_user.columns:
            df_user = df_user.drop("index", axis=1)
            df_user = Transformations.clean_upper_or_numeric_rows(df_user)
            df_user["address"] = Transformations.remove_newline_character(df_user["address"])
            df_user = Transformations.email_address_cleaner(df_user)
            df_user = Transformations.clean_country_code_ggb(df_user)
            df_user["date_of_birth"] = pd.to_datetime(df_user["date_of_birth"], errors='coerce')
            df_user["join_date"] = pd.to_datetime(df_user["join_date"], errors='coerce')
            return df_user

    def clean_card_data(df):
        """
        Cleans the card data DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw card data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned card data.
        """
        df = Transformations.drop_rows_with_invalid_card_numbers(df)
        df = Transformations.clean_upper_or_numeric_rows(df)
        return df

    @staticmethod
    def clean_store_data(df):
        """
        Cleans the store data DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw store data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned store data.
        """
        try:
            df = df.drop("lat", axis=1)
            df = df.drop("index", axis=1)
            df.dropna()
            df.continent = df.continent.str.replace("ee", "")
            df = Transformations.clean_upper_or_numeric_rows(df)
            df.address = Transformations.remove_newline_character(df.address)
            df = Transformations.clean_user_data_rows_all_NULL(df)
            df = Transformations.clean_country_code_ggb(df)
            # Remove letters from staff_numbers if it contains a combination of letters and numbers
            df['staff_numbers'] = df['staff_numbers'].apply(lambda x: ''.join(filter(str.isdigit, str(x))))

            df.staff_numbers = pd.to_numeric(df.staff_numbers, errors="coerce")
            df.dropna(subset=['staff_numbers'], inplace=True)  # Drop rows where staff_numbers couldn't be converted
            df.staff_numbers = df.staff_numbers.astype('int')  # Convert staff_numbers to int
            return df
        except Exception as error:
            print(f"Error in clean_store_data: {error}")
            print(traceback.format_exc())
            return None  # or handle the error appropriately

    @staticmethod
    def clean_product_data(df):
        """
        Cleans the product data DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw product data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned product data.
        """
        df.drop("Unnamed: 0", axis=1, inplace=True)
        df = Transformations.clean_upper_or_numeric_rows(df)
        df.dropna(inplace=True)
        df.rename(columns={"weight": "weight (kg)", "removed" : "still_available"}, inplace=True)
        Transformations.convert_product_weights(df)
        return df
    
    @staticmethod
    def clean_orders_data(df):
        """
        Cleans the orders data DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw orders data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned orders data.
        """
        df.drop(columns={"level_0", "index", "1", "first_name", "last_name"}, inplace=True)
        df.dropna(axis=0, subset=["card_number"], inplace=True)
        return df

    @staticmethod
    def clean_date_events(df):
        """
        Cleans the date events DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw date events data.

        Returns:
            pd.DataFrame: DataFrame containing cleaned date events data.
        """
        df = Transformations.clean_upper_or_numeric_rows(df)
        df["datetime_str"] = df["year"].astype(str) + "-" + df["month"].astype(str) + "-" + df["day"].astype(str) + " " + df["timestamp"]
        df["datetime"] = pd.to_datetime(df["datetime_str"])
        df.drop("datetime_str", axis=1, inplace=True)
        return df
    
    @staticmethod
    def execute_sql_query(engine, query):
        """
        Execute an SQL query using the provided engine.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.
            query (str): SQL query to be executed.

        Returns:
            None
        """
        try:
            with engine.connect() as conn:
                conn.execute(query)
                conn.commit()
        except Exception as error:
            print(f"Error executing SQL query: {error}")

    
    @staticmethod
    def merge_latitudes(engine):
        """
        Merge latitude/longitude columns in the dim_store_details table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        try:
            merge_latitudes_query = text("""
                UPDATE dim_store_details
                SET latitude = COALESCE(latitude, longitude)
            """)
            with engine.connect() as conn:
                conn.execute(merge_latitudes_query)   
                conn.commit()
                print("Latitude columns merged successfully.")
        except Exception as error:
            print(f"Error merging latitude columns: {error}")

    
    @staticmethod
    def clean_product_price(engine):
        """
        Clean the product price column in the dim_products table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        try:
            with engine.connect() as conn:
                # Remove currency symbol and non-numeric characters
                query = "UPDATE dim_products SET product_price = NULLIF(REPLACE(product_price::text, '£', '')::FLOAT, 0)"
                conn.execute(text(query))
                conn.commit()
                print("Product price column cleaned successfully.")
        except Exception as error:
            print(f"Error cleaning product price: {error}")

    @staticmethod
    def add_weight_class_column(engine):
        """
        Add a weight class column to the dim_products table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        try:
            with engine.connect() as conn:
                add_column_query = """
                    ALTER TABLE dim_products
                    ADD COLUMN weight_class VARCHAR(255);
                """
                update_column_query = """
                    UPDATE dim_products
                    SET weight_class =
                        CASE
                            WHEN "weight (kg)" < 2 THEN 'Light'
                            WHEN "weight (kg)" >= 2 AND "weight (kg)" < 40 THEN 'Mid_Sized'
                            WHEN "weight (kg)" >= 40 AND "weight (kg)" < 140 THEN 'Heavy'
                            WHEN "weight (kg)" >= 140 THEN 'Truck_Required'
                            ELSE NULL -- Handle other cases if needed
                        END;
                """
                conn.execute(text(add_column_query))
                conn.commit()
                conn.execute(text(update_column_query))
                conn.commit()
                print("Added weight class column successfully.")
        except Exception as error:
            print(f"Error adding weight class column: {error}")

    def change_dim_users_column_types(engine):
        DataCleaning.alter_column_type(engine, "dim_users_table", "first_name", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_users_table", "last_name", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_users_table", "date_of_birth", "DATE")
        DataCleaning.alter_column_type(engine, "dim_users_table", "country_code", "VARCHAR(2)")
        DataCleaning.alter_column_type(engine, "dim_users_table", "user_uuid", "UUID USING user_uuid::uuid")
        DataCleaning.alter_column_type(engine, "dim_users_table", "join_date", "DATE")

    def change_orders_table_column_types(engine):
        """
        Change the data types of columns in the orders_table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        DataCleaning.alter_column_type(engine, "orders_table", "product_quantity", "SMALLINT")
        DataCleaning.alter_column_type(engine, "orders_table", '"store_code"', "VARCHAR")
        DataCleaning.alter_column_type(engine, "orders_table", "card_number", "VARCHAR")
        DataCleaning.alter_column_type(engine, "orders_table", "user_uuid", "UUID USING user_uuid::uuid")
        DataCleaning.alter_column_type(engine, "orders_table", "date_uuid", "UUID USING date_uuid::uuid")
        DataCleaning.alter_column_type(engine, "orders_table", "product_code", "VARCHAR")

    def change_dim_product_column_types(engine):
        """
        Change the data types of columns in the dim_products table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        DataCleaning.alter_column_type(engine, "dim_products", "product_price", "FLOAT USING product_price::double precision")
        DataCleaning.alter_column_type(engine, "dim_products", '"weight (kg)"', "FLOAT")
        DataCleaning.alter_column_type(engine, "dim_products", '"EAN"', "VARCHAR")
        DataCleaning.alter_column_type(engine, "dim_products", "product_code", "VARCHAR")
        DataCleaning.alter_column_type(engine, "dim_products", "date_added", "DATE USING date_added::date")
        DataCleaning.alter_column_type(engine, "dim_products", "uuid", "UUID USING uuid::uuid")
        DataCleaning.alter_column_type(engine, "dim_products", "still_available", "BOOL USING still_available ='still_available';")
        DataCleaning.alter_column_type(engine, "dim_products", "weight_class", "VARCHAR")

    def change_dim_store_details_column_types(engine):
        """
        Change the data types of columns in the dim_store_details table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        DataCleaning.alter_column_type(engine, "dim_store_details", "longitude", "FLOAT")
        DataCleaning.alter_column_type(engine, "dim_store_details", "locality", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_store_details", "store_code", "VARCHAR")
        DataCleaning.alter_column_type(engine, "dim_store_details", "staff_numbers", "SMALLINT")
        DataCleaning.alter_column_type(engine, "dim_store_details", "opening_date", "DATE USING opening_date::date")
        DataCleaning.alter_column_type(engine, "dim_store_details", "store_type", "VARCHAR(255)")
        DataCleaning.execute_sql_query(engine, text("""UPDATE dim_store_details SET latitude = 0 WHERE latitude = 'N/A';"""))
        DataCleaning.alter_column_type(engine, "dim_store_details", "latitude", "FLOAT USING latitude::double precision")
        DataCleaning.alter_column_type(engine, "dim_store_details", "country_code", "VARCHAR(2)")
        DataCleaning.alter_column_type(engine, "dim_store_details", "continent", "VARCHAR(255)")

    def change_dim_date_times_types(engine):
        """
        Change the data types of columns in the dim_date_times table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        DataCleaning.alter_column_type(engine, "dim_date_times", "month", "VARCHAR(40)")
        DataCleaning.alter_column_type(engine, "dim_date_times", "day", "VARCHAR(40)")
        DataCleaning.alter_column_type(engine, "dim_date_times", "year", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_date_times", "time_period", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_date_times", "date_uuid", "UUID USING date_uuid::uuid")
        
    def change_dim_card_details_types(engine):
        """
        Change the data types of columns in the dim_card_details table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        DataCleaning.alter_column_type(engine, "dim_card_details", "card_number", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_card_details", "expiry_date", "VARCHAR(255)")
        DataCleaning.alter_column_type(engine, "dim_card_details", "date_payment_confirmed", "DATE USING date_payment_confirmed::date")

    def add_primary_key(engine, table_name, column_name):
        """
        Add primary key constraint to a column in a table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.
            table_name (str): Name of the table.
            column_name (str): Name of the column to be set as primary key.

        Returns:
            None
        """
        inspector = inspect(engine)
        primary_key_columns = inspector.get_pk_constraint(table_name)['constrained_columns']

        if column_name not in primary_key_columns:
            query = text(f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name});")
            with engine.connect() as conn:
                try:
                    conn.execute(query)
                    conn.commit()
                    print(f"Primary key added to column '{column_name}' in table '{table_name}'.")
                except Exception as error:
                    print(f"Unable to add primary key: ", {error})
                    print(traceback.format_exc())
        else:
            print(f"Column '{column_name}' is already a primary key in table '{table_name}'.")
            
    def check_primary_key(engine, table_name, column_name):
        """
        Check if a primary key is set on a column in a table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.
            table_name (str): Name of the table.
            column_name (str): Name of the column.

        Returns:
            bool: True if the column has a primary key, False otherwise.
        """
        inspector = inspect(engine)
        constraints = inspector.get_unique_constraints(table_name)

        for constraint in constraints:
            if column_name in constraint['column_names']:
                return True  # Primary key found

        return False  # Primary key not found
    

    def create_foreign_keys(engine):
        """
        Create foreign key constraints in the orders_table.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        # Replace these table and column names with your actual table and column names
        tables_and_columns = {
            'dim_users_table': 'user_uuid',
            'dim_card_details': 'card_number',
            'dim_store_details': 'store_code',
            'dim_products': 'product_code',
            'dim_date_times': 'date_uuid',
        }

        try:
            with engine.connect() as conn:
                # Iterate over tables and columns to create foreign key constraints
                for table, column in tables_and_columns.items():
                    foreign_key_query = text(f"""
                        ALTER TABLE orders_table
                        ADD CONSTRAINT fk_{table}_{column}
                        FOREIGN KEY ({column})
                        REFERENCES {table} ({column});
                    """)
                    
                    # Use the connection to execute the foreign key query
                    conn.execute(foreign_key_query)
                    print(f"Foreign key added to orders_table referencing {table} ({column}).")
        except Exception as error:
            print(f"Error executing SQL query: {error}")