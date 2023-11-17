import psycopg2
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = psycopg2.connect(
            host = host,
            port = port,
            database = database,
            user = user,
            password = password
        )
        self.cursor = self.conn.cursor()
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}")

    @staticmethod
    def read_db_creds(db_creds_file ="db_creds.yaml", env="RDS"):
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
        try:
            creds = DatabaseConnector.read_db_creds(env=env)
            database_url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
            engine = create_engine(database_url)
            return engine
        except Exception as error:
            print(f"Error initialising database engine: {error}")
            return None
        
    @staticmethod    
    def list_db_tables(engine):
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
        try:
            # Ensure dataframe is a DataFrame
            if not isinstance(dataframe, pd.DataFrame):
                raise ValueError("Input must be a DataFrame.")

            # Use the provided engine
            with engine.connect() as connection:
                # Use the connection to execute to_sql
                dataframe.to_sql(table_name, connection, if_exists='replace', index=False)
                
            print(f"Data uploaded to table '{table_name}' successfully.")
        except Exception as error:
            print(f"Error uploading data to table '{table_name}': {error}")    



       #     dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
       #     print(f"Data uploaded to table '{table_name}' successfully.")
       # except Exception as error:
        #    print(f"Error uploading data to table '{table_name}': {error}")