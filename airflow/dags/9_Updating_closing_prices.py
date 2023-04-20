import datetime
import pandas as pd
import yfinance as yf
import numpy as np
import os
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


def get_ticker_data():
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=1)
    ticker_data = pd.DataFrame()
    snp500_companies=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    tickers = snp500_companies[0]['Symbol']
    # tickers = ['AAPL','MSFT','VVV']
    for ticker in tickers:
        data = yf.download(ticker,start =start_date, end=end_date)
        data.insert(0,'Ticker',ticker)
        data = data.reset_index()
        ticker_data = ticker_data.append(data)
        ticker_data = ticker_data.loc[:, ['Date', 'Ticker', 'Close']]
    
    os.makedirs('/tmp',exist_ok=True)
    ticker_data.to_csv('/tmp/snp500_update_data.csv',index=False)

with DAG(
    dag_id="update_table_snp500",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_get_ticker_data = PythonOperator(
        task_id = 'get_ticker_data',
        python_callable=get_ticker_data,
        do_xcom_push =True
    )

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        src = '/tmp/snp500_update_data.csv',
        dst = 'snp500_ticker_update_data'   
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq',
        bucket='is3107',
        gcp_conn_id = 'gcp_3107_official',
        source_objects=['snp500_ticker_update_data'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.snp500_ticker_update_data',
        schema_fields=[
        {'name':'Date','type':'DATE','mode':'NULLABLE'},
        {'name':'Ticker','type':'STRING','mode':'NULLABLE'},
        {'name':'Close','type':'STRING','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        
    )

    merge_data_query = """
    MERGE able-brace-379917.project_dataset.snp500_ticker_data AS target
    USING (
      SELECT * FROM `able-brace-379917.project_dataset.snp500_ticker_update_data`
      WHERE Date > (SELECT MAX(Date) FROM able-brace-379917.project_dataset.snp500_ticker_data)
    ) AS source
    ON target.Date = source.Date AND target.Ticker = source.Ticker
    WHEN NOT MATCHED THEN
      INSERT (Date, Ticker, Close) VALUES (source.Date, source.Ticker, source.Close)
    """


    merge_data_operator = BigQueryExecuteQueryOperator(
        task_id='merge_data_from_query',
        sql=merge_data_query,
        gcp_conn_id = 'gcp_3107_official',
        use_legacy_sql=False,
        dag=dag,
    )



task_get_ticker_data >> local_to_gcs >> gcs_to_bq >> merge_data_operator