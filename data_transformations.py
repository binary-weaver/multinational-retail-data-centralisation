import pandas as pd

class Transformations:
    @staticmethod
    def rows_with_alphabetic_dob_locator(df):
        """
        Locate rows with alphabetic characters in the 'date_of_birth' column.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame containing rows with alphabetic characters in the 'date_of_birth' column.
        """
        return df[df["date_of_birth"].str.contains("[a-zA-Z]", regex=True)]

    @staticmethod
    def rows_without_atsymbol_email_address_locator(df):
        """
        Locate rows without the '@' symbol in the 'email_address' column.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame containing rows without the '@' symbol in the 'email_address' column.
        """
        return df[~df["email_address"].str.contains("@")]

    @staticmethod
    def remove_rows_without_atsymbol_email_address(df):
        """
        Remove rows without the '@' symbol in the 'email_address' column.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with rows removed where 'email_address' does not contain the '@' symbol.
        """
        return df[df["email_address"].str.contains("@")]

    @staticmethod
    def clean_user_data_rows_all_NULL(df):
        """
        Remove rows where all values are 'NULL' in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with rows removed where all values are 'NULL'.
        """
        cleaned_df = df.drop(df[df.eq("NULL").all(axis=1)].index)
        return cleaned_df

    @staticmethod
    def is_upper_or_numeric(val):
        """
        Check if a value is uppercase or numeric.

        Args:
            val: Value to be checked.

        Returns:
            bool: True if the value is uppercase or numeric, False otherwise.
        """
        return val.isupper() or val.isnumeric()

    @staticmethod
    def clean_upper_or_numeric_rows(df):
        """
        Clean rows containing values that are either uppercase or numeric.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with rows removed containing values that are either uppercase or numeric.
        """
        if df is not None:
            return  df.drop(df[df.applymap(lambda x: isinstance(x, str) and Transformations.is_upper_or_numeric(x)).all(axis=1)].index)
        else:
            return print("Dataframe is type: ", type(df))

    @staticmethod
    def remove_newline_character(series):
        """
        Remove newline characters from a Pandas Series.

        Args:
            series (pd.Series): Pandas Series containing text data.

        Returns:
            pd.Series: Series with newline characters removed.
        """
        series = series.str.replace("\n", ", ")
        return series

    @staticmethod
    def email_address_cleaner(df):
        """
        Clean the 'email_address' column in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with 'email_address' column cleaned.
        """
        df["email_address"] = df["email_address"].str.replace("@@", "@")
        return df

    @staticmethod
    def clean_country_code_ggb(df):
        """
        Clean the 'country_code' column in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with 'country_code' column cleaned.
        """
        df["country_code"] = df["country_code"].str.replace("GGB", "GB")
        return df

    @staticmethod
    def find_na_rows(df):
        """
        Locate rows containing "N/A" in any column.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame containing rows with "N/A".
        """
        return df[df.apply(lambda row: row.astype(str).str.contains("N/A").any(), axis=1)]

    @staticmethod
    def drop_rows_with_invalid_card_numbers(df):
        """
        Drop rows with invalid card numbers containing '?'.

        Args:
            df (pd.DataFrame): DataFrame containing raw data.

        Returns:
            pd.DataFrame: DataFrame with rows dropped where 'card_number' contains '?'.
        """
        # Ensure 'card_number' column exists in the DataFrame
        if "card_number" in df.columns:
            # Drop rows where "card_number" contains "?"
            return df[~df["card_number"].astype(str).str.contains("\?", regex=True)]
        else:
            print("Error: 'card_number' column not found in the DataFrame.")
            return df


    @staticmethod
    def convert_product_weights(df_products):
        """
        Convert weight entries to kilograms in the 'weight (kg)' column.

        Args:
            df_products (pd.DataFrame): DataFrame containing raw product data.

        Returns:
            None
        """
        def clean_g(gram_string):
            """
            Clean and convert a weight entry in grams to kilograms.

            Args:
                gram_string (str): String representing weight in grams.

            Returns:
                float: Weight in kilograms.
            """
            return float(gram_string.replace("g", "")) / 1000

        def clean_x_g(x_g_string):
            """
            Clean and convert a weight entry in the format 'x g' to kilograms.

            Args:
                x_g_string (str): String representing weight in the format 'x g'.

            Returns:
                float: Weight in kilograms.
            """
            x_g_string = x_g_string.strip("g")
            x_g_string = x_g_string.split("x")
            result = float(x_g_string[0]) * float(x_g_string[1])
            return result / 1000

        def clean_ml(ml_string):
            """
            Clean and convert a volume entry in milliliters to liters.

            Args:
                ml_string (str): String representing volume in milliliters.

            Returns:
                float: Volume in liters.
            """
            return float(ml_string.replace("ml", "")) / 1000

        def clean_kg(kg_string):
            """
            Clean and convert a weight entry in kilograms.

            Args:
                kg_string (str): String representing weight in kilograms.

            Returns:
                float: Weight in kilograms.
            """
            return float(kg_string.replace("kg", ""))

        def clean_oz(oz_string):
            """
            Clean and convert a weight entry in ounces to kilograms.

            Args:
                oz_string (str): String representing weight in ounces.

            Returns:
                float: Weight in kilograms.
            """
            return float(oz_string.replace("oz", "")) * 0.0283495

        def clean_weight_entry(entry):
            """"
            Clean and convert a general weight entry to kilograms.

            Args:
                entry (str): String representing a weight entry.

            Returns:
                float or None: Weight in kilograms, or None if the entry cannot be processed.
            """
            entry = str(entry).lower()
            if "x" in entry and "g" in entry:
                return clean_x_g(entry)
            elif "kg" in entry:
                return clean_kg(entry)
            elif "g" in entry:
                return clean_g(entry)
            elif "ml" in entry:
                return clean_ml(entry)
            elif "oz" in entry:
                return clean_oz(entry)
            else:
                return None

        df_products["weight (kg)"] = Transformations.remove_newline_character(df_products["weight (kg)"])
        df_products["weight (kg)"] = df_products["weight (kg)"].str.strip(".")
        df_products["weight (kg)"] = df_products["weight (kg)"].apply(clean_weight_entry)   