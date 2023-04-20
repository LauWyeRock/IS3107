import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
import os

apiKey = "YU2WS3DFRZNOBW2Q"
apiKey2= '7E01NY5AWTMML6AR'
apiKey3 = 'TMJXU8UWCA0PYW4I'
apiKey4 = 'GMORUOA5ANFIFLYB'
apiKey5 = '6J3WYMQU1IBGY8JT'
apiKey6 = 'Q7S5MY8BOZ30WU9F'
apiKey7 = 'E7RSATAZGN09U7BF'
apiKey8 = 'OJ3HYOQ5R6YQQ7HQ'
apiKey9 = 'RXNCYRXC8HRE67VZ'
apiKey10 ='4IZ6YCBLPVN2WF87'
apiKey11 ='T16XJ7L29ZIC54F5'
apiKey12 = 'T8KDAAX3DMF90GU8'
apiKey13 = 'YPB0AWLC04BSYLCA'
apiKey14 = 'IIDIQCUR0VQHF2K9'

apikeys = [apiKey,apiKey2,apiKey3,apiKey4,apiKey5,apiKey6,apiKey7,apiKey8,apiKey10,apiKey11,apiKey12,apiKey13,apiKey14]
# def vwap(ticker,key):
#     url = f'https://www.alphavantage.co/query?function=VWAP&symbol={ticker}&interval=15min&apikey={key}'
#     r = requests.get(url)
#     data = r.json()

#     df = pd.DataFrame(data["Technical Analysis: VWAP"])
#     df = df.iloc[:, :7]
#     return(df)
    

def macd(ticker,key):
    url = f'https://www.alphavantage.co/query?function=MACD&symbol={ticker}&interval=daily&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()

    df = pd.DataFrame(data["Technical Analysis: MACD"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df

def adxr(ticker,key):
    url = f'https://www.alphavantage.co/query?function=ADXR&symbol={ticker}&interval=daily&time_period=10&apikey={key}'
    r = requests.get(url)
    data_adx = r.json()

    df_adx = pd.DataFrame(data_adx["Technical Analysis: ADXR"])
    df= df_adx.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df


def obv(ticker,key):
    url = f'https://www.alphavantage.co/query?function=OBV&symbol={ticker}&interval=daily&apikey={key}'
    r = requests.get(url)
    data = r.json()

    df = pd.DataFrame(data["Technical Analysis: OBV"])
    df= df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df

def stochRSI(ticker,key):
    url = f'https://www.alphavantage.co/query?function=STOCHRSI&symbol={ticker}&interval=daily&time_period=10&series_type=close&fastkperiod=6&fastdmatype=1&apikey={key}'
    r = requests.get(url)
    data = r.json()

    df = pd.DataFrame(data["Technical Analysis: STOCHRSI"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df

def sma(ticker,key):
    url = f'https://www.alphavantage.co/query?function=SMA&symbol={ticker}&interval=daily&time_period=10&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Technical Analysis: SMA"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df

def ema50(ticker,key):
    url = f'https://www.alphavantage.co/query?function=EMA&symbol={ticker}&interval=daily&time_period=50&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Technical Analysis: EMA"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})

    return df

def ema100(ticker,key):
    url = f'https://www.alphavantage.co/query?function=EMA&symbol={ticker}&interval=daily&time_period=100&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Technical Analysis: EMA"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})

    return df

def ema200(ticker,key):
    url = f'https://www.alphavantage.co/query?function=EMA&symbol={ticker}&interval=daily&time_period=200&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Technical Analysis: EMA"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    
    return df

def wma(ticker,key):
    url = f'https://www.alphavantage.co/query?function=WMA&symbol={ticker}&interval=daily&time_period=10&series_type=open&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Technical Analysis: WMA"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    return df

def ts_daily_adjusted(ticker,key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["Time Series (Daily)"])
    df = df.iloc[:, :10]
    df = df.transpose()

    symbol = [ticker] * 10
    df.insert(0,'Symbol',symbol)
    df = df.reset_index()
    df = df.rename(columns={'index':'Date'})
    
    return df

def process_data(ti): 
    tickers = ti.xcom_pull(task_ids='get_data')
    tickers = [x for t in tickers for x in t]

    sma_df = pd.DataFrame()
    ema50_df = pd.DataFrame()
    ema100_df = pd.DataFrame()
    ema200_df = pd.DataFrame()
    wma_df = pd.DataFrame()
    macd_df = pd.DataFrame()
    adxr_df = pd.DataFrame()
    stochRSI_df = pd.DataFrame()
    obv_df = pd.DataFrame()
    ts_daily_adjusted_df = pd.DataFrame()

    keynumber = 0
    result_dfs = [sma_df,ema50_df,ema100_df,ema200_df,wma_df,macd_df,adxr_df,stochRSI_df,obv_df,ts_daily_adjusted_df]
    result_df_names = ['sma_df','ema50_df','ema100_df','ema200_df','wma_df','macd_df','adxr_df','stochRSI_df','obv_df','ts_daily_adjusted_df']
    
    while len(tickers) != 0:
        flag1 = False
        flag2 = False
        flag3 = False
        flag4 = False
        flag5 = False
        flag6 = False
        flag7 = False
        flag8 = False
        flag9 = False
        flag10 = False
        
        key= apikeys[keynumber]
        ticker = tickers[0]

        while not flag1:
            try:
                df1=sma(ticker,key)
                flag1 = True
                # print('flag1clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
            
        while not flag2:
            try:
                df2 = ema50(ticker,key)
                flag2 = True
                # print('flag2clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 

        while not flag3:
            try:
                df3=ema100(ticker,key)
                flag3 = True
                # print('flag3clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 

        while not flag4:
            try:
                df4=ema200(ticker,key)
                flag4 = True
                # print('flag4clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
        
        while not flag5:
            try:
                df5=wma(ticker,key)
                flag5 = True
                # print('flag5clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
        
        while not flag6:
            try:
                df6=macd(ticker,key)
                flag6 = True
                # print('flag6clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber]
        
        while not flag7:
            try:
                df7=adxr(ticker,key)
                flag7 = True
                # print('flag7clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber]
        
        while not flag8:
            try:
                df8=stochRSI(ticker,key)
                flag8 = True
                # print('flag8clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber]

        while not flag9:
            try:
                df9=obv(ticker,key)
                flag9 = True
                # print('flag9clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber]
        
        while not flag10:
            try:
                df10=ts_daily_adjusted(ticker,key)
                flag10 = True
                # print('flag10clear')
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber]
        

        dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10]

        for i in range(len(dfs)):
            result_dfs[i] = pd.concat([result_dfs[i],dfs[i]])

        tickers = tickers[1:]
        print(tickers)
        sleep(60)

    for df in result_dfs:
        df.replace('None',value=-1,inplace=True)
        df.replace('-',value=-1,inplace=True)
    
    if not os.path.exists('/tmp/technical_analysis/'):
        os.makedirs('/tmp/technical_analysis/')
    
    for i in range(len(result_dfs)):
        result_dfs[i].to_csv(f'/tmp/technical_analysis/{result_df_names[i]}.csv',index=False)

with DAG(
    dag_id="technical_analysis",
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

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        src = ['/tmp/technical_analysis/sma_df.csv',
               '/tmp/technical_analysis/ema50_df.csv',
               '/tmp/technical_analysis/ema100_df.csv',
               '/tmp/technical_analysis/ema200_df.csv',
               '/tmp/technical_analysis/wma_df.csv',
               '/tmp/technical_analysis/macd_df.csv',
               '/tmp/technical_analysis/adxr_df.csv',
               '/tmp/technical_analysis/stochRSI_df.csv',
               '/tmp/technical_analysis/obv_df.csv',
               '/tmp/technical_analysis/ts_daily_adjusted_df.csv',],
        dst = 'technical_analysis/'   
    )

    gcs_to_bq_sma = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_sma',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/sma_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.sma',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'SMA','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_ema50 = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_ema50',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/ema50_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.ema50',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'EMA50','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_ema100 = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_ema100',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/ema100_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.ema100',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'EMA100','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_ema200 = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_ema200',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/ema200_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.ema200',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'EMA200','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_wma = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_wma',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/wma_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.wma',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'WMA','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_macd = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_macd',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/macd_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.macd',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'MACD','type':'FLOAT','mode':'NULLABLE'},
        {'name':'MACD_Signal','type':'FLOAT','mode':'NULLABLE'},
        {'name':'MACD_Hist','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_adxr = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_adxr',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/adxr_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.adxr',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'ADXR','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_stochRSI = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_stochRSI',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/stochRSI_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.stochRSI',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'FastK','type':'FLOAT','mode':'NULLABLE'},
        {'name':'FastD','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_obv = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_obv',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/obv_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.obv',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'OBV','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_ts_daily_adjusted = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_ts_daily_adjusted',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['technical_analysis/ts_daily_adjusted_df.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.ts_daily_adjusted',
        schema_fields=[
        {'name':'date','type':'DATE','mode':'NULLABLE'},
        {'name':'symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'open','type':'FLOAT','mode':'NULLABLE'},
        {'name':'high','type':'FLOAT','mode':'NULLABLE'},
        {'name':'low','type':'FLOAT','mode':'NULLABLE'},
        {'name':'close','type':'FLOAT','mode':'NULLABLE'},
        {'name':'adjustedClose','type':'FLOAT','mode':'NULLABLE'},
        {'name':'volume','type':'FLOAT','mode':'NULLABLE'},
        {'name':'dividendAmount','type':'FLOAT','mode':'NULLABLE'},
        {'name':'splitCoefficient','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    

    
get_data >> task_process_data >> local_to_gcs 
local_to_gcs >> [gcs_to_bq_sma, gcs_to_bq_ema50, gcs_to_bq_ema100, gcs_to_bq_ema200,gcs_to_bq_wma,gcs_to_bq_macd,gcs_to_bq_adxr,gcs_to_bq_stochRSI,gcs_to_bq_obv,gcs_to_bq_ts_daily_adjusted]