"""
Configurations for the project.

This module reads key-value pairs from a `.env` file and sets them as environment variables. 
It then retrieves the API_KEY from the environment.

Attributes:
    api_key (str): The API key retrieved from the environment.
"""

import os
import dotenv

dotenv.load_dotenv()

class Config:
    api_key = os.getenv("API_KEY")