import requests
import pandas as pd
import numpy as np
import io
from time import sleep
from datetime import date
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
import os
# from alpha_vantage.fundamentaldata import FundamentalData

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

def balance_sheet(ticker,key):
    url = f"https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={ticker}&apikey={key}&datatype=csv"
    r = requests.get(url)
    data = r.json()

    ### To check for what data they have 
    #print(data['annualReports'][0])

    df = pd.DataFrame(data["annualReports"], index=[0,1,2,3,4])
    df = df[['fiscalDateEnding','totalAssets', 'totalCurrentAssets', 'cashAndCashEquivalentsAtCarryingValue', 'cashAndShortTermInvestments', 'inventory', 'currentNetReceivables',
             'totalNonCurrentAssets', 'totalLiabilities', 'totalCurrentLiabilities', 'currentAccountsPayable', 'currentDebt', 'totalNonCurrentLiabilities',
             'retainedEarnings','totalShareholderEquity','totalLiabilities']].iloc[:5]
    #print(df)
    symbol = [ticker] * 5
    df.insert(0,'Symbol',symbol)
    return(df)
    

def cash_flow(ticker,key): 
    url = f"https://www.alphavantage.co/query?function=CASH_FLOW&symbol={ticker}&apikey={key}"
    r = requests.get(url)
    data = r.json()


    ### To check for what data they have 
    #print(data['annualReports'][0])


    df = pd.DataFrame(data["annualReports"], index=[0,1,2,3,4])
    df = df[['fiscalDateEnding','operatingCashflow', 'capitalExpenditures', 'changeInReceivables','changeInInventory','profitLoss', 'cashflowFromInvestment', 'cashflowFromFinancing','netIncome']].iloc[:5]
    #print(df)
    symbol = [ticker] * 5
    df.insert(0,'Symbol',symbol)
    return(df)

def earnings(ticker,key): 
    url = f"https://www.alphavantage.co/query?function=EARNINGS&symbol={ticker}&apikey={key}"
    r = requests.get(url)
    data = r.json()
    df = pd.DataFrame(data["annualEarnings"], index=[0,1,2,3,4,5,6,7,8,9,10,11,12]).iloc[:5]
    # print(df)
    symbol = [ticker] * 5
    df.insert(0,'Symbol',symbol)
    return(df)
    

def income_statement(ticker,key): 
    url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={key}"
    r = requests.get(url)
    data = r.json()
    
    ### To check for what data they have 
    #print(data['annualReports'][0])


    df = pd.DataFrame(data["annualReports"], index=[0,1,2,3,4])
    df = df[['fiscalDateEnding','grossProfit','totalRevenue','costOfRevenue','ebit','ebitda']].iloc[:5]
    #print(df)
    symbol = [ticker] * 5
    df.insert(0,'Symbol',symbol)
    return(df)

def overview(ticker,key): 
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={ticker}&apikey={key}"
    r = requests.get(url)
    data = r.json()

    
    ### To check for what data they have 
    #print(data)

    df = pd.DataFrame(data, index=[0])
    df = df[['Symbol','Description', 'CIK','MarketCapitalization', 'EBITDA','PERatio', 'PEGRatio','BookValue','DividendYield','TrailingPE','ForwardPE',
             'PriceToBookRatio','EVToRevenue','EVToEBITDA','EPS', "ProfitMargin"]].iloc[:5]
    #print(df)
    return(df)


apikeys = [apiKey,apiKey2,apiKey3,apiKey4,apiKey5,apiKey6,apiKey7,apiKey8,apiKey10,apiKey11,apiKey12,apiKey13,apiKey14]

def process_data(ti): 
    tickers = ti.xcom_pull(task_ids='get_data')
    tickers = [x for t in tickers for x in t]
    overview_df = pd.DataFrame()
    balance_sheet_df = pd.DataFrame()
    cash_flow_df = pd.DataFrame()
    # earnings_df = pd.DataFrame()
    income_statement_df = pd.DataFrame()
    keynumber = 0
    result_dfs = [overview_df,balance_sheet_df,cash_flow_df,income_statement_df]
    # tickers = ['MSFT','TSLA']
    while len(tickers) != 0:
        flag1 = False
        flag2 = False
        flag3 = False
        # flag4 = False
        flag5 = False
        key= apikeys[keynumber]
        ticker = tickers[0]
        while not flag1:
            try:
                df1=overview(ticker,key)
                flag1 = True
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
            
        while not flag2:
            try:
                df2=balance_sheet(ticker,key)
                flag2 = True
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 

        while not flag3:
            try:
                df3=cash_flow(ticker,key)
                flag3 = True
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
        # while not flag4:
        #     try:
        #         df4=earnings(ticker,key)
        #         flag4 = True
        #     except: 
        #         keynumber = (keynumber+1)%len(apikeys)
        #         key= apikeys[keynumber] 
        
        while not flag5:
            try:
                df5=income_statement(ticker,key)
                flag5 = True
            except: 
                keynumber = (keynumber+1)%len(apikeys)
                key= apikeys[keynumber] 
        

        dfs = [df1,df2,df3,df5]

        for i in range(len(dfs)):

            result_dfs[i] = pd.concat([result_dfs[i],dfs[i]])

        tickers = tickers[1:]
        print(tickers)
        if len(tickers) != 0:
            sleep(60)

    for df in result_dfs:
        df.replace('None',value=-1,inplace=True)
        df.replace('-',value=-1,inplace=True)
    
    os.makedirs('/tmp/fundamental_data/',exist_ok=True)
    result_df_names = ['overview','balance_sheet','cash_flow','income_statement']
    for i in range(len(result_dfs)):
        result_dfs[i].to_csv(f'/tmp/fundamental_data/{result_df_names[i]}.csv',index=False)

# def local_to_gcs():
#     result_files = ['overview','balance_cash_flow','income_statement']
#     for file in result_files:
#         LocalFilesystemToGCSOperator(
#         gcp_conn_id = 'gcp_3107_official',
#         bucket = 'is3107',
#         src = f'/private/tmp/fundamental_data/{file}.csv',
#         dst = f'fundamental_data/{file}'   
#         )


with DAG(
    dag_id="fundamental_data",
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


    local_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "local_to_gcs",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        src = ['/tmp/fundamental_data/overview.csv',
               '/tmp/fundamental_data/balance_sheet.csv',
               '/tmp/fundamental_data/cash_flow.csv',
               '/tmp/fundamental_data/income_statement.csv'],
        dst = 'fundamental_data/'   
    )

    task_process_data = PythonOperator(
         task_id = 'process_data',
         python_callable=process_data,
         provide_context=True
    )

    gcs_to_bq_balance_sheet = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_balance_sheet',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['fundamental_data/balance_sheet.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.balance_sheet',
        schema_fields=[
        {'name':'Symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'fiscalDateEnding','type':'DATE','mode':'NULLABLE'},
        {'name':'totalAssets','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalCurrentAssets','type':'FLOAT','mode':'NULLABLE'},
        {'name':'cashAndCashEquivalentsAtCarryingValue','type':'FLOAT','mode':'NULLABLE'},
        {'name':'cashAndShortTermInvestments','type':'FLOAT','mode':'NULLABLE'},
        {'name':'inventory','type':'FLOAT','mode':'NULLABLE'},
        {'name':'currentNetReceivables','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalNonCurrentAssets','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalLiabilities','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalCurrentLiabilities','type':'FLOAT','mode':'NULLABLE'},
        {'name':'currentAccountsPayable','type':'FLOAT','mode':'NULLABLE'},
        {'name':'currentDebt','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalNonCurrentLiabilities','type':'FLOAT','mode':'NULLABLE'},
        {'name':'retainedEarnings','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalShareholderEquity','type':'FLOAT','mode':'NULLABLE'},    
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_cash_flow = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_cash_flow',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['fundamental_data/cash_flow.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.cash_flow',
        schema_fields=[
        {'name':'Symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'fiscalDateEnding','type':'DATE','mode':'NULLABLE'},
        {'name':'operatingCashflow','type':'FLOAT','mode':'NULLABLE'},
        {'name':'capitalExpenditures','type':'FLOAT','mode':'NULLABLE'},
        {'name':'changeInReceivables','type':'FLOAT','mode':'NULLABLE'},
        {'name':'changeInInventory','type':'FLOAT','mode':'NULLABLE'},
        {'name':'profitLoss','type':'FLOAT','mode':'NULLABLE'},
        {'name':'cashflowFromInvestment','type':'FLOAT','mode':'NULLABLE'},
        {'name':'cashflowFromFinancing','type':'FLOAT','mode':'NULLABLE'},
        {'name':'netIncome','type':'FLOAT','mode':'NULLABLE'}, 
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_income_statement = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_income_statement',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['fundamental_data/income_statement.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.income_statement',
        schema_fields=[
        {'name':'Symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'fiscalDateEnding','type':'DATE','mode':'NULLABLE'},
        {'name':'grossProfit','type':'FLOAT','mode':'NULLABLE'},
        {'name':'totalRevenue','type':'FLOAT','mode':'NULLABLE'},
        {'name':'costOfRevenue','type':'FLOAT','mode':'NULLABLE'},
        {'name':'ebit','type':'FLOAT','mode':'NULLABLE'},
        {'name':'ebitda','type':'FLOAT','mode':'NULLABLE'}
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )

    gcs_to_bq_overview = GCSToBigQueryOperator(
        task_id= 'gcs_to_bq_overview',
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        source_objects=['fundamental_data/overview.csv'],
        destination_project_dataset_table= 'able-brace-379917.project_dataset.overview',
        schema_fields=[
        {'name':'Symbol','type':'STRING','mode':'NULLABLE'},
        {'name':'Description','type':'STRING','mode':'NULLABLE'},
        {'name':'CIK','type':'INT64','mode':'NULLABLE'},
        {'name':'MarketCapitalization','type':'FLOAT','mode':'NULLABLE'},
        {'name':'EBITDA','type':'FLOAT','mode':'NULLABLE'},
        {'name':'PERatio','type':'FLOAT','mode':'NULLABLE'},
        {'name':'PEGRatio','type':'FLOAT','mode':'NULLABLE'},
        {'name':'BookValue','type':'FLOAT','mode':'NULLABLE'},
        {'name':'DividendYield','type':'FLOAT','mode':'NULLABLE'},
        {'name':'TrailingPE','type':'FLOAT','mode':'NULLABLE'},
        {'name':'ForwardPE','type':'FLOAT','mode':'NULLABLE'},
        {'name':'PriceToBookRatio','type':'FLOAT','mode':'NULLABLE'},
        {'name':'EVToRevenue','type':'FLOAT','mode':'NULLABLE'},
        {'name':'EVToEBITDA','type':'FLOAT','mode':'NULLABLE'},
        {'name':'EPS','type':'FLOAT','mode':'NULLABLE'},
        {'name':'ProfitMargin','type':'FLOAT','mode':'NULLABLE'},
        ],
        create_disposition = 'CREATE_IF_NEEDED',    
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_quoted_newlines=True,
    )
get_data >> task_process_data >> local_to_gcs 
local_to_gcs >> [gcs_to_bq_balance_sheet,gcs_to_bq_cash_flow,gcs_to_bq_income_statement, gcs_to_bq_overview]
