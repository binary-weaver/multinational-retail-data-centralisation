# Multinational Retail Database Centralisation

## Description

This Python-based ETL (Extract, Transform, Load) tool is designed to centralize and clean various types of data from different sources. It includes functionality for extracting data from PDFs, APIs, and databases, performing data cleaning operations, and uploading cleaned data to a PostgreSQL database.

## Table of Contents
- [Features](#features)
- [Usage](#usage)
- [Installation](#installation)
- [File Structure](#file-structure)
- [License](#license)

## Features

- **User Data Cleaning:**
  - Removes duplicates, formats dates, and handles data types.
  - Uploads cleaned user data to the local PostgreSQL database.

- **Card Data Cleaning:**
  - Extracts data from a PDF document.
  - Validates card numbers and performs data cleaning.
  - Uploads cleaned card data to the local PostgreSQL database.

- **Store Data Cleaning:**
  - Retrieves store data from an API.
  - Cleans and standardizes store data.
  - Uploads cleaned store data to the local PostgreSQL database.

- **Product Data Cleaning:**
  - Extracts product data from an S3 bucket.
  - Cleans and standardizes product data.
  - Uploads cleaned product data to the local PostgreSQL database.

- **Order Data Cleaning:**
  - Reads order data from an RDS table.
  - Cleans and validates order data.
  - Uploads cleaned order data to the local PostgreSQL database.

- **Date Event Data Cleaning:**
  - Extracts date event data from a JSON file.
  - Cleans and formats date event data.
  - Uploads cleaned date event data to the local PostgreSQL database.

## Usage

### Prerequisites

- Python 3.x
- PostgreSQL Database
- Additional Python packages (listed in requirements.txt)


## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/multinational-retail-database-centralisation.git
   cd multinational-retail-database-centralisation
2. Install dependencies
    ```bash
    conda env create -f environment.yml
    conda activate mrdc
3. Set up environment variables; create a .env file in the project root with your API key:
    ```bash
    API_KEY=your_api_key_here

## Usage

1. Run the main script to perform ETL:
    ```bash
    python main user   
Replace user with the data type you want to process (card, store, product, order, date_event).

## File Structure
multinational-retail-database-centralisation/
|-- data_cleaning.py
|-- data_extraction.py
|-- database_utils.py
|-- main.py
|-- config.py
|-- environment.yml
|-- db_creds.yaml
|-- data_queries.md
|-- README.md
|-- .env

## License Information
MIT License

Copyright (c) [2023] [Moez Abdu]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

