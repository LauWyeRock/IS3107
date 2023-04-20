
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
import yfinance as yf
from sklearn.preprocessing import StandardScaler, MinMaxScaler 
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
import os
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

def predicted_price(ticker,stock_data):
    # stock_data = yf.download(ticker, start='2018-01-01', end='2023-03-20')
    # close_prices = stock_data['Close']
    
    
    subset_stock_data = stock_data[stock_data['ticker']==ticker]
    subset_stock_data = subset_stock_data.sort_values(by='date',ascending=True)
    close_prices = subset_stock_data['close']

    # Scale the data using MinMaxScaler
    FullData = close_prices.values.reshape(-1, 1)
    DataScaler = MinMaxScaler()
    X = DataScaler.fit_transform(FullData)

    # Create sequences of past 1000 prices
    X_samples = []
    for i in range(1000, len(X)):
        X_samples.append(X[i-1000:i])

    # Convert data to numpy arrays and reshape for LSTM input
    X_data = np.array(X_samples).reshape(-1, 1000, 1)

    # Build LSTM model
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(1000, 1)))
    model.add(LSTM(units=50, return_sequences=True))
    model.add(LSTM(units=50))
    model.add(Dense(units=3))
    model.compile(optimizer='adam', loss='mean_squared_error')
    model.fit(X_data, X[1000:], epochs=5, batch_size=5)

    # Use the model to predict next 3 days of prices
    last_1000_days = X[-1000:].reshape(-1, 1000, 1)
    predicted_prices = model.predict(last_1000_days)
    predicted_prices = DataScaler.inverse_transform(predicted_prices)
    
    price1 = predicted_prices[0][0]
    price2 = predicted_prices[0][1]
    price3 = predicted_prices[0][2]

    return ticker,price1,price2,price3

def process_data(ti): 
    tickers = ti.xcom_pull(task_ids='get_data')
    tickers = [x for t in tickers for x in t]
    stock_data = pd.read_csv('/tmp/top10_volume_closing_prices.csv')
    
    df = pd.DataFrame(columns = ['Ticker','Predicted Day 1','Predicted Day 2','Predicted Day 3'])
    for ticker in tickers:
        ticker, price1,price2,price3 = predicted_price(ticker,stock_data)
        df = df.append({'Ticker': ticker, 'Predicted Day 1':price1, 'Predicted Day 2': price2, 'Predicted Day 3': price3},ignore_index=True)

    os.makedirs('/tmp',exist_ok=True)
    df.to_csv('/tmp/top10_predicted_prices.csv',index=False)
    

with DAG(
    dag_id="ml_predictions",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    get_data = BigQueryGetDataOperator(
        task_id = 'get_data',
        gcp_conn_id = 'gcp_3107_official',
        dataset_id = 'project_dataset',
        table_id = 'top_10_volume',
        max_results='10',
        selected_fields = 'Ticker',
    )

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=process_data,
         provide_context=True
    )

    gcs_to_local = GoogleCloudStorageDownloadOperator(
        task_id = "gcs_to_local",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        object_name ='top10_volume_closing_prices',
        filename = '/tmp/top10_volume_closing_prices.csv'
    )

    create_table_predicted_prices = PostgresOperator(
        task_id = 'create_table_predicted_prices',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''CREATE TABLE IF NOT EXISTS top10_predicted_prices(
            ticker VARCHAR(100),
            Predicted_Day_1 FLOAT,
            Predicted_Day_2 FLOAT,
            Predicted_Day_3 FLOAT   
        )    
        '''
    )

    delete_table_predicted_prices = PostgresOperator(
        task_id = 'delete_predicted_prices',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''DELETE FROM top10_predicted_prices '''
    )

    def load_predicted_prices():
        pg_hook = PostgresHook(
            postgres_conn_id = 'postgres_is3107_official',
            schema = 'is3107'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        with open('/tmp/top10_predicted_prices.csv','r') as file:
            
            next(file)
            cursor.copy_from(file, 'top10_predicted_prices' ,sep=',')
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
        
    task_load_predicted_prices = PythonOperator(
        task_id = 'load_predicted_prices',
        python_callable=load_predicted_prices
    )


gcs_to_local >> get_data >> task_process_data >> create_table_predicted_prices >> delete_table_predicted_prices >> task_load_predicted_prices
