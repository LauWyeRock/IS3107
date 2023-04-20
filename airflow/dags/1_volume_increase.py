import yfinance as yf
import pandas as pd
from datetime import date
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os


def process_data():
    MAX = 10

    snp500_companies=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = snp500_companies[0]['Symbol']

    net_volumes = {}

    for stock in tickers:
        stock = stock.upper()
        try:
            stock_info = yf.Ticker(stock)
            hist = stock_info.history(period="5d")
            previous_averaged_volume = hist['Volume'].iloc[1:4:1].mean()
            todays_volume = hist['Volume'][-1]
            net_volume = todays_volume - previous_averaged_volume
            net_volumes[stock] = net_volume
        except:
            pass

    top_tickers = []
    sorted_tickers_by_volume = sorted(net_volumes.items(), key=lambda x:x[1], reverse=True)
    current_date = date.today()
    for i in range(MAX):
        ticker_data = ((current_date,) + sorted_tickers_by_volume[i])
        top_tickers.append(ticker_data)

    top_tickers = pd.DataFrame(top_tickers, columns = ['Date','Ticker','Volume_Increase']) 
    os.makedirs('/tmp',exist_ok=True)
    top_tickers.to_csv("/tmp/top_10_volume.csv",index=False)

with DAG(
    dag_id="volume_increase_processed",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        src = '/tmp/top_10_volume.csv',
        dst = 'top_10_volume'   
    )

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=process_data
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['top_10_volume'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.top_10_volume',
        schema_fields=[
        {'name':'Date','type':'DATE','mode':'NULLABLE'},
        {'name':'Ticker','type':'STRING','mode':'NULLABLE'},
        {'name':'Volume_Increase','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

task_process_data >> local_to_gcs >> gcs_to_bq