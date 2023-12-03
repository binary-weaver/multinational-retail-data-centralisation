import psycopg2
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    """
    A utility class for connecting to and interacting with a PostgreSQL database.
    """
    def __init__(self, host, port, database, user, password):
        """
        Initializes a DatabaseConnector instance with the provided connection details.

        Args:
            host (str): The hostname or IP address of the PostgreSQL server.
            port (int): The port number on which the PostgreSQL server is listening.
            database (str): The name of the PostgreSQL database.
            user (str): The username used to authenticate with the PostgreSQL server.
            password (str): The password used to authenticate with the PostgreSQL server.
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        self.cursor = self.conn.cursor()
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    @staticmethod
    def read_db_creds(db_creds_file="db_creds.yaml", env="RDS"):
        """
        Reads database credentials from a YAML file.

        Args:
            db_creds_file (str): Path to the YAML file containing database credentials.
                                Defaults to "db_creds.yaml".
            env (str): The environment or section name in the YAML file. Defaults to "RDS".

        Returns:
            dict: A dictionary containing the database credentials.
        """
        try:
            with open(db_creds_file, 'r') as file:
                creds = yaml.safe_load(file)
                return creds.get(env)
        except FileNotFoundError:
            print(f"Error: Credentials file '{creds}' not found.")
            return None
        except yaml.YAMLError as error:
            print(f"Error reading YAML file: {error}")
            return None

    @staticmethod
    def init_db_engine(env="RDS"):
        """
        Initializes a SQLAlchemy database engine using credentials from a YAML file.

        Args:
            env (str): The environment or section name in the YAML file. Defaults to "RDS".

        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy database engine.
        """
        try:
            creds = DatabaseConnector.read_db_creds(env=env)
            database_url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
            engine = create_engine(database_url)
            return engine
        except Exception as error:
            print(f"Error initializing database engine: {error}")
            return None

    @staticmethod
    def list_db_tables(engine):
        """
        Lists the tables in the connected PostgreSQL database.

        Args:
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            list: A list of table names in the database.
        """
        try:
            with engine.connect():
                inspector = inspect(engine)
                tables = inspector.get_table_names()
            return tables
        except Exception as error:
            print(f"Error listing database tables: {error}")
            return None

    @staticmethod
    def upload_to_db(dataframe, table_name, engine):
        """
        Uploads a Pandas DataFrame to a PostgreSQL table.

        Args:
            dataframe (pd.DataFrame): The DataFrame to be uploaded.
            table_name (str): The name of the PostgreSQL table to upload the data.
            engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine.

        Returns:
            None
        """
        try:
            if not isinstance(dataframe, pd.DataFrame):
                raise ValueError("Input must be a DataFrame.")

            with engine.connect() as connection:
                dataframe.to_sql(table_name, connection, if_exists='replace', index=False)

            print(f"Data uploaded to table '{table_name}' successfully.")

        except Exception as error:
            print(f"Error uploading data to table '{table_name}': {error}")

