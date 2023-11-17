# Multinational Retail Database Centralisation

## Description

This project aims to centralize and clean data from a multinational retail database. It includes modules for data extraction, transformation, and loading (ETL) into a centralized database. The data comes from various sources, including PDFs and APIs.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [File Structure](#file-structure)
- [License](#license)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/multinational-retail-database-centralisation.git
   cd multinational-retail-database-centralisation
2. Install dependencies
    ```bash
    conda env create -f environment.yml
    conda activate multinational-retail-database-centralisation
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
|-- README.md
|-- .env

## License Information
