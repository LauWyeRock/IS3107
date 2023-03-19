import datetime
import pandas as pd
import yfinance as yf
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_ticker_data():

    # initialise output dataframe
    returns_df = pd.DataFrame()
    tickers = ["MSFT"]
    start_date = '2022-01-01'
    end_date = '2022-12-31'
    for ticker in tickers:
        # retrieve stock data (includes Date, OHLC, Volume, Adjusted Close)
        format='%Y-%m-%d'
        s = yf.download(ticker, datetime.datetime.strptime(start_date, format), datetime.datetime.strptime(end_date, format))
        # calculate log returns
        s['Log Returns'] = np.log(s['Adj Close']/s['Adj Close'].shift(1))
        # append to returns_df
        returns_df[ticker] = s['Log Returns']
        
    # skip the first row (that will be NA)
    # and fill other NA values by 0 in case there are trading halts on specific days
    returns_df = returns_df.iloc[1:].fillna(0)
    returns_df = returns_df.reset_index()
    returns_df.to_csv(Variable.get('tmp_logreturns_csv_location'),index=False)

def load_ticker_data():
    pg_hook = PostgresHook(
        postgres_conn_id = 'postgres_db',
        schema = 'db_test'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    with open('C:\Users\wyero\Downloads\logreturns.csv','r') as file:
        next(file)
        cursor.copy_from(file, 'msftlogreturns',sep=',')
    pg_conn.commit()
    cursor.close()
    pg_conn.close()

with DAG(
    dag_id="project_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    task_get_ticker_data = PythonOperator(
        task_id = 'get_ticker_data',
        python_callable=get_ticker_data,
        do_xcom_push =True
    )

    task_load_ticker_data = PythonOperator(
        task_id = 'load_ticker_data',
        python_callable=load_ticker_data,
    )

