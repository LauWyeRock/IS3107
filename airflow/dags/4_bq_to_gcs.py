import datetime
import pandas as pd
import yfinance as yf
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator


with DAG(
    dag_id="bq_to_gcs",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    # extract_from_bq = BigQueryOperator(
    #     task_id = 'extract_from_bq',
    #     gcp_conn_id = 'gcp_bq_conn',
    #     sql = 'SELECT * FROM project-381114.project.stocks1',
    #     destination_dataset_table = 'project-381114.project.stocks2',
    #     write_disposition ='WRITE_TRUNCATE',
    #     create_disposition ='CREATE_IF_NEEDED',
    #     use_legacy_sql = False
    # )
    

    # create_table = PostgresOperator(
    #     task_id = 'create_table',
    #     postgres_conn_id = 'postgres_db',
    #     sql = '''CREATE TABLE IF NOT EXISTS stocks (
        
    #         Ticker VARCHAR(100),
    #         Open FLOAT,
    #         Close FLOAT,
    #         High FLOAT,
    #         Low FLOAT,
    #         Adj_Close FLOAT,
    #         Volume FLOAT 
    #     )'''
    # )

    # # load_to_postgres = PostgresOperator(
    # #     task_id = 'load_to_postgres',
    # #     postgres_conn_id = 'postgres_db',
    # #     sql = '''INSERT INTO stocks (Date, Ticker, Open, Close, High, Low, Adj_Close, Volume)
    # #             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    # #     ''',
    # #     parameters = {{ ti.xcom.pull(task_ids = 'extract_from_bq') }}
    # # )

    # @dag.task()
    # def process_data(data):
    #     return [(datetime.datetime.strptime(row['Date'],'%Y-%m-%d').date(),
    #              row['Ticker'],
    #              float(row['Open']),
    #              (row['Close']),
    #              float(row['High']),
    #              float(row['Low']),
    #              float(row['Adj_Close']),
    #              float(row['Volume'])) for row in data]
    
    bq_to_gcs_fundamental = BigQueryToGCSOperator(
        force_rerun = True,
        task_id = "bq_to_gcs_fundamental",
        gcp_conn_id = 'gcp_3107_official',
        source_project_dataset_table = 'able-brace-379917.project_dataset.fundamental_data',
        destination_cloud_storage_uris = ["gs://is3107/fundamental_data_to_pg"],
        export_format = 'CSV'
    )

    bq_to_gcs_technical = BigQueryToGCSOperator(
        force_rerun = True,
        task_id = "bq_to_gcs_technical",
        gcp_conn_id = 'gcp_3107_official',
        source_project_dataset_table = 'able-brace-379917.project_dataset.technical_data',
        destination_cloud_storage_uris = ["gs://is3107/technical_data_to_pg"],
        export_format = 'CSV'
    )

    bq_to_gcs_sentiment = BigQueryToGCSOperator(
        force_rerun = True,
        task_id = "bq_to_gcs_sentiment",
        gcp_conn_id = 'gcp_3107_official',
        source_project_dataset_table = 'able-brace-379917.project_dataset.sentiment_analysis',
        destination_cloud_storage_uris = ["gs://is3107/sentiment_analysis_to_pg"],
        export_format = 'CSV'
    )

    # gcs_to_local = GoogleCloudStorageDownloadOperator(
    #     force_rerun = True,
    #     task_id = "gcs_to_local",
    #     gcp_conn_id = 'gcp_bq_conn',
    #     bucket = '3107_bucket',
    #     object_name ='stocks_frm_bq',
    #     filename = '/private/tmp/stocks_frm_bq.csv'
    # )

    
    
    
    [bq_to_gcs_fundamental ,bq_to_gcs_technical,bq_to_gcs_sentiment]