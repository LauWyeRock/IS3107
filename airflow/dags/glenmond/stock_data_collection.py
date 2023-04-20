from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import requests
import psycopg2
import json


{
  "host": "localhost",
  "database": "postgres",
  "user": "postgres",
  "password": "wyerock",
  "schema_name": "db_test",
  "table_name": "stocks"
}


"""
A Python script that retrieves stock market data from Alpha Vantage and Financial Modeling Prep APIs and stores the data in a PostgreSQL database.
The script contains three main functions:
- `get_stocks`: retrieves the symbols of the top 5 stocks of a specified market performance (gainers, losers, or actives)
- `get_stock_data`: retrieves the volume, price, change percent, and name of the specified stock symbols
- `store_stock_data_in_database`: stores the stock market data in a PostgreSQL database
"""

#-- Create a database
#CREATE DATABASE stock_data_db;
#\c stock_data_db

#-- Create a schema for the project
#CREATE SCHEMA stock_data;

#-- Create a table to store the stock data
#CREATE TABLE stock_data.stocks (
#    id SERIAL PRIMARY KEY,
#    symbol VARCHAR(20) NOT NULL,
#    name VARCHAR(50) NOT NULL,
#    price NUMERIC(10,2) NOT NULL,
#    change_percent NUMERIC(5,2) NOT NULL,
#    date_collected TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
#);


# Define the base URL and API key for Alpha Advantage API
ALPHA_BASE_URL = "https://www.alphavantage.co/query?"
ALPHA_API_KEY = "S1HB81M1BIAB0ML2"

# Define the base URL and API key for Financial Modeling Prep API

PREP_BASE_URL = "https://financialmodelingprep.com/api/v3/"
PREP_API_KEY = "e18f8efccbac4ac741c48162fec73d2e"

# These variables are used to make API requests to both Alpha Advantage and Financial Modeling Prep
# The base URL and API key are used to build the complete URL to make the request

def get_stocks(market_performance: str) -> list:
    """
    Get the symbols of the top 5 stocks of the specified market performance.
    :param market_performance: string indicating the type of market data to retrieve (Gainers, losers, or actives)
    :return: a list of symbols of the top 5 stocks in the specified market performance
    :raise: Exception if the request fails or if no data was retrieved
    """
    # Define the URL for the requested market performance
    url = f"{PREP_BASE_URL}stock_market/{market_performance}?apikey={PREP_API_KEY}"

    # Send a GET request to the API
    response = requests.get(url, timeout=5)

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data from the API: {response.text}")

    # Retrieve the data from the API response
    data = response.json()

    # Check if the data is empty
    if not data:
        raise Exception(f"No data was retrieved for market performance '{market_performance}'")

    # Get symbol of top 5 stocks in the specified market performance
    stock_symbols = [item['symbol'] for item in data[:5]]

    return stock_symbols

def get_stock_data(symbols: list) -> dict:
    """
    This function retrieves the volume, price, change percent, and name for the given symbols from Alpha Vantage's API.
    :param symbols: A list of symbols for the stocks to retrieve data for
    :return: A dictionary with the symbol as the key and a dictionary of volume, price, change percent, and name as the value
    """
    alpha_endpoint = "GLOBAL_QUOTE"
    prep_endpoint = "profile"
    stock_data = {}
    for symbol in symbols:
        try:
            # Build the URL to request data for the given symbol from the global quote endpoint 
            alpha_url = f"{ALPHA_BASE_URL}function={alpha_endpoint}&symbol={symbol}&apikey={ALPHA_API_KEY}"
            # Request data from the API and convert the response to a dictionary
            alpha_response = requests.get(alpha_url)
            alpha_data = alpha_response.json()

            # Validate the data returned from the API
            if "Error Message" in alpha_data:
                raise ValueError(f"Error retrieving data for symbol {symbol}: {alpha_data['Error Message']}")

            # Extract the volume, price, and change percent data from the response
            volume = alpha_data["Global Quote"]["06. volume"]
            price = alpha_data["Global Quote"]["05. price"]
            change_percent = alpha_data["Global Quote"]["10. change percent"]

            # Build the URL to request data for the given symbol from the profile endpoint
            prep_url = f"{PREP_BASE_URL}{prep_endpoint}/{symbol}?apikey={PREP_API_KEY}"
            # Request data from the API and convert the response to a dictionary
            prep_response = requests.get(prep_url)
            prep_data = prep_response.json()

            # Validate the data returned from the API
            if not prep_data:
                raise ValueError(f"Error retrieving name for symbol {symbol}")

            # Extract the name data from the response
            name = prep_data[0]['companyName']

            # Store the data in the stock_data dictionary
            stock_data[symbol] = {
                "volume": int(volume),
                "price": float(price),
                "change_percent": float(change_percent.rstrip('%')),
                "name": name,
            }
        except (ValueError, KeyError) as error:
            print(f"An error occurred while retrieving data for symbol {symbol}: {error}")

    return stock_data 

def store_stock_data_in_database(data: dict, config_file: str):
    """
    Store the stock market data in a PostgreSQL database
    :param data: A dictionary with the stock symbol as the key and the stock data as the value
    :param config_file: A JSON file containing the database configuration
    """
    # Load the database configuration
    with open(config_file, "r") as f:
        config = json.load(f)
    schema_name = config.get("schema_name")
    table_name = config.get("table_name")

    # Connect to the database
    conn = None
    cur = None
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=config["host"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )
        cur = conn.cursor()
        # Loop through the stock data
        for symbol, stock_data in data.items():
            # Extract the relevant information
            name = stock_data.get("name")
            volume = stock_data.get("volume")
            price = stock_data.get("price")
            change_percent = stock_data.get("change_percent")

            # Validate the data
            if not all([symbol, name, volume, price, change_percent]):
                raise ValueError("One or more required fields are missing from the stock data")
            if not isinstance(volume, int):
                raise TypeError("Volume must be of type int")
            if not isinstance(price, float):
                raise TypeError("Price must be of type float")
            if not isinstance(change_percent, float):
                raise TypeError("Change percent must be of type float")

            # Insert the data into the table
            cur.execute(f"INSERT INTO {schema_name} . {table_name} (symbol, name, volume, price, change_percent) VALUES (%s, %s, %s, %s, %s)",
                       (symbol, name, volume, price, change_percent))

        # Commit the changes to the database
        conn.commit()
    except (psycopg2.Error, ValueError, TypeError) as error:
        print(f"An error occurred while storing the data in the database: {error}")
        # Rollback the changes if there was an error
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

"""
This script sets up a DAG for collecting and storing stock data from the financialmodelingprep and Alpha Advantage APIs.  
It consists of three tasks:
    1. get_stocks: retrieves the symbol of the top 5 stocks of the specified market performance (gainers, by default)
    2. get_stock_data: retrieves detailed information of the stocks retrieved in task 1
    3. store_stock_data_in_database: stores the stock data in a database
The tasks run every day at 7 PM, as specified by the schedule_interval parameter. Task dependencies are established
such that get_stocks must complete before get_stock_data, and get_stock_data must complete before store_stock_data_in_database.
"""

  
# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'stock_data_collection', 
    default_args=default_args, 
    schedule='50 15 * * *', # Schedule to run everyday at 7 PM
    description='Collect and store stock data' # Add description for the DAG
)

# Define the first task - get_stocks
get_stocks_task = PythonOperator(
    task_id='get_stock',
    python_callable=get_stocks,
    op_args=['gainers'], # Add a descriptive comment for the op_args
    dag=dag,
)

# Define the second task - get_stock_data
get_stock_data_task = PythonOperator(
    task_id='get_data',
    python_callable=get_stock_data,
    op_args=[get_stocks_task.output], # Pass the output of the previous task as input
    dag=dag,
)

# Define the third task - store_stock_data_in_database
store_stock_data_in_database_task = PythonOperator(
    task_id='store_data',
    python_callable=store_stock_data_in_database,
    op_args=[get_stock_data_task.output], # Pass the output of the previous task as input
    dag=dag,
)