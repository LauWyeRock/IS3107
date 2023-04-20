'''
returns the most recent sentiment on the stock
'''

import requests
import pandas as pd
from datetime import date
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from time import sleep
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

apiKeys = [apiKey,apiKey2,apiKey3,apiKey4,apiKey5,apiKey6,apiKey7,apiKey8,apiKey10,apiKey11,apiKey12,apiKey13,apiKey14]
def sentiment(ticker,key):
    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker}&apikey={key}'
    r = requests.get(url)
    data = r.json()

    count = 0
    score = 0

    for article in data["feed"]:
        score += article["overall_sentiment_score"]
        count += 1

    final_score = score/count
    #print(final_score)
    if final_score <= -0.35:
        #print("Bearish")
        sentiment_label = "Bearish"
    elif final_score > -0.35 and final_score <= -0.15:
        #print("Somewhat Bearish")
        sentiment_label = "Somewhat Bearish"
        
    elif final_score > -0.15 and final_score < 0.15:
        #print("Neutral")
        sentiment_label = "Neutral"
    elif final_score >= 0.15 and final_score < 0.35:
        #print("Somewhat Bullish")
        sentiment_label ="Somewhat Bullish"
        
    else:
        #print("Bullish")
        sentiment_label ="Bullish"
    return final_score, sentiment_label


def process_data(ti):

    # snp500_companies=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    # tickers = snp500_companies[0]['Symbol']
    most_recent_date = date.today()
    tickers = ti.xcom_pull(task_ids='get_data')
    sentiment_df = pd.DataFrame(columns = ['Date','Ticker','Sentiment_ticker_score','Sentiment_ticker_label']
                                )
    keyNumber = 0
    key = apiKeys[keyNumber]
    while len(tickers) != 0:
        try:
            ticker = tickers[0]
            ticker_final_score, ticker_sentiment_label = sentiment(ticker[0],key)
            ticker_df = pd.DataFrame({
                'Date':[most_recent_date],
                'Ticker':[ticker[0]],
                'Sentiment_ticker_score': [ticker_final_score],
                'Sentiment_ticker_label': [ticker_sentiment_label]
            })
            sentiment_df = pd.concat([sentiment_df,ticker_df])
            
            tickers = tickers[1:]
            print(tickers)
        except:
            keyNumber = (keyNumber+1)%len(apiKeys)
            key= apiKeys[keyNumber]

    # if not os.path.exists('/private/tmp'):
    #     os.makedirs('/private/tmp',exist_ok=True)
    # sentiment_df.to_csv('/private/tmp/sentiment_analysis.csv',index=False)
    if not os.path.exists('/tmp'):
        os.makedirs('/tmp',exist_ok=True)
    sentiment_df.to_csv('/tmp/sentiment_analysis.csv',index=False)


with DAG(
    dag_id="sentiment_analysis",
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

    # task_process_bq = PythonOperator(
    #     task_id='process_bq',
    #     python_callable=process_bq,
    #     provide_context=True
    # )

    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        src = '/tmp/sentiment_analysis.csv',
        dst = 'sentiment_analysis'   
    )

    task_process_data = PythonOperator(
        task_id = 'process_data',
        python_callable=process_data,
        provide_context=True,
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['sentiment_analysis'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.sentiment_analysis',
        schema_fields=[
        {'name':'Date','type':'DATE','mode':'NULLABLE'},
        {'name':'Ticker','type':'STRING','mode':'NULLABLE'},
        {'name':'Sentiment_ticker_score','type':'FLOAT','mode':'NULLABLE'},
        {'name':'Sentiment_ticker_label','type':'STRING','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

get_data >> task_process_data >> local_to_gcs >> gcs_to_bq
# get_data >> task_process_data
