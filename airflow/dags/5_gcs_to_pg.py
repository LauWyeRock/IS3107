import datetime
import pandas as pd
import yfinance as yf
import numpy as np
import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

def calculate_fundamental_score(df):
    df_norm = pd.DataFrame()
    for column in df:
    # normalize data
        try:
            df_norm[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
        except:
            df_norm[column] = df[column]

    pe_ratio_score = 100 - (df_norm["PERatio"] * 100)
    de_ratio_score = 100 - (df_norm["debtToEquityRatio"] * 100)
    roe_score = df_norm["returnOnEquity"] * 100
    current_ratio_score = df_norm["currentRatio"] * 100
    gross_margin_score = df_norm["grossMargin"] * 100
    quick_ratio_score = df_norm["quickRatio"] * 100
    eps_score = df_norm["EPS"] * 100

    # random weights
    weights = [0.2, 0.15, 0.15, 0.1, 0.15, 0.1, 0.15]
    scores = [pe_ratio_score, de_ratio_score, roe_score, current_ratio_score, gross_margin_score, quick_ratio_score, eps_score]

    final_scores = []
    for i in range(len(df)):
        final_score = sum([w * s[i] for w, s in zip(weights, scores)])
        
        final_scores.append(final_score)

    df["Fundamental Score"] = final_scores

    return df

def calculate_technical_score(df):
    indicators = ['SMA', 'EMA50', 'EMA100', 'EMA200', 'WMA', 'MACD', 'MACD_Signal', 'MACD_Hist', 'ADXR', 'FastK', 'FastD', 'OBV']

    weights = [0.15, 0.25, 0.1, 0.15, 0.1, 0.15, 0.1]

    results = {}

    for ticker, group in df.groupby('symbol'):
        stock_data = group[indicators]

        sma_score = calculate_sma_score(stock_data['SMA'])
        ema_score = calculate_ema_score(stock_data['EMA50'], stock_data['EMA100'], stock_data['EMA200'])
        wma_score = calculate_wma_score(stock_data['WMA'])
        macd_score = calculate_macd_score(stock_data['MACD'], stock_data['MACD_Signal'], stock_data['MACD_Hist'])
        adxr_score = calculate_adxr_score(stock_data['ADXR'])
        stoch_score = calculate_stochRSI_score(stock_data['FastK'], stock_data['FastD'])
        obv_score = calculate_obv_score(stock_data['OBV'])

        # Calculate the weighted average of the scores
        overall_score = sum([s * w for s, w in zip([sma_score, ema_score, wma_score, macd_score, adxr_score, stoch_score, obv_score], weights)])

        # Store the result for the current stock ticker
        results[ticker] = overall_score

    df["technical_score"] = 0

    for index, row in df.iterrows():
        ticker = row["symbol"]
        value = results.get(ticker, 0)
        df.at[index, "technical_score"] = value

    return df


# def calculate_technical_score(stock_data):

#     sma = stock_data['SMA']
#     ema_50 = stock_data['EMA50']
#     ema_100 = stock_data['EMA100']
#     ema_200 = stock_data['EMA200']
#     wma = stock_data['WMA']
#     macd = stock_data['MACD']
#     macd_signal = stock_data['MACD_Signal']
#     macd_hist = stock_data['MACD_Hist']
#     adxr = stock_data['ADXR']
#     stoch_fastK = stock_data['FastK']
#     stoch_fastD = stock_data['FastD']
#     obv = stock_data['OBV']

#     # Calculate the scores for each indicator
#     sma_score = calculate_sma_score(sma)
#     ema_score = calculate_ema_score(ema_50, ema_100, ema_200)
#     wma_score = calculate_wma_score(wma)
#     macd_score = calculate_macd_score(macd, macd_signal, macd_hist)
#     adxr_score = calculate_adxr_score(adxr)
#     stoch_score = calculate_stochRSI_score(stoch_fastK, stoch_fastD)
#     obv_score = calculate_obv_score(obv)

#     # Calculate the weighted average of the scores
#     overall_score = (sma_score * 0.15 +
#                      ema_score * 0.25 +
#                      wma_score * 0.1 +
#                      macd_score * 0.15 +
#                      adxr_score * 0.1 +
#                      stoch_score * 0.15 +
#                      obv_score * 0.1)

#     return overall_score


def calculate_sma_score(sma):
    # If the current price is above the SMA, assign a score of 100
    if sma.iloc[-1] < sma.iloc[-2]:
        return 50 + ((sma.iloc[-1] / sma.min()) * 50)
    # If the current price is below the SMA, assign a score of 0
    elif sma.iloc[-1] > sma.iloc[-2]:
        return 50 - ((sma.max() / sma.iloc[-1]) * 50)
    # If the current price is at the SMA, assign a score of 50
    else:
        return 50

def calculate_ema_score(ema50, ema100, ema200):
    # Calculate the difference between the current price and each EMA
    diff50 = ema50.iloc[-1] - ema50.iloc[-2]
    diff100 = ema100.iloc[-1] - ema100.iloc[-2]
    diff200 = ema200.iloc[-1] - ema200.iloc[-2]

    # Assign a score of 100 if the current price is above all three EMAs
    if diff50 > 0 and diff100 > 0 and diff200 > 0:
        return 100
    # Assign a score of 0 if the current price is below all three EMAs
    elif diff50 < 0 and diff100 < 0 and diff200 < 0:
        return 0
    # Assign a score of 50 if the current price is between the 50 EMA and the 200 EMA
    elif diff50 > 0 and diff200 > 0:
        return 50 + ((ema100.iloc[-1] / ema50.iloc[-1]) * 25) + ((ema100.iloc[-1] / ema200.iloc[-1]) * 25)
    # Assign a score of 50 if the current price is between the 100 EMA and the 200 EMA
    elif diff100 > 0 and diff200 > 0:
        return 50 + ((ema50.iloc[-1] / ema100.iloc[-1]) * 25) + ((ema50.iloc[-1] / ema200.iloc[-1]) * 25)
    # Assign a score of 50 if the current price is between the 50 EMA and the 100 EMA
    elif diff50 > 0 and diff100 > 0:
        return 50 + ((ema200.iloc[-1] / ema50.iloc[-1]) * 25) + ((ema200.iloc[-1] / ema100.iloc[-1]) * 25)
    # Assign a score of 50 if the current price is above the 50 EMA
    elif diff50 > 0:
        return 50 + ((ema100.iloc[-1] / ema50.iloc[-1]) * 33.33) + ((ema200.iloc[-1] / ema50.iloc[-1]) * 33.33)
    # Assign a score of 50 if the current price is above the 100 EMA
    elif diff100 > 0:
        return 50 + ((ema50.iloc[-1] / ema100.iloc[-1]) * 33.33) + ((ema200.iloc[-1] / ema100.iloc[-1]) * 33.33)
    # Assign a score of 50 if the current price is above the 200 EMA
    else:
        return 50 + ((ema50.iloc[-1] / ema200.iloc[-1]) * 33.33) + ((ema100.iloc[-1] / ema200.iloc[-1]) * 33.33)

def calculate_wma_score(wma):
    # If the current price is above the WMA, assign a score of 100
    if wma.iloc[-1] < wma.iloc[-2]:
        return 50 + ((wma.iloc[-1] / wma.min()) * 50)
    # If the current price is below the WMA, assign a score of 0
    elif wma.iloc[-1] > wma.iloc[-2]:
        return 50 - ((wma.max() / wma.iloc[-1]) * 50)
    # If the current price is at the WMA, assign a score of 50
    else:
        return 50

def calculate_macd_score(macd, macd_signal, macd_hist):
    # If the MACD line is above the signal line and the MACD histogram is positive, assign a score of 100
    if macd.iloc[-1] > macd_signal.iloc[-1] and macd_hist.iloc[-1] > 0:
        return 100
    # If the MACD line is below the signal line and the MACD histogram is negative, assign a score of 0
    elif macd.iloc[-1] < macd_signal.iloc[-1] and macd_hist.iloc[-1] < 0:
        return 0
    # If the MACD line and the signal line are close and the MACD histogram is small, assign a score of 50
    else:
        return 50

def calculate_adxr_score(adxr):
    # If the ADXR is above 25, assign a score of 100
    if adxr.iloc[-1] > 25:
        return 100
    # If the ADXR is below 20, assign a score of 0
    elif adxr.iloc[-1] < 20:
        return 0
    # If the ADXR is between 20 and 25, assign a score of 50
    else:
        return 50

def calculate_stochRSI_score(stoch_fastK, stoch_fastD):
    # If the %K line is above the %D line, assign a score of 100
    if stoch_fastK.iloc[-1] > stoch_fastD.iloc[-1]:
        return 100
    # If the %K line is below the %D line, assign a score of 0
    elif stoch_fastK.iloc[-1] < stoch_fastD.iloc[-1]:
        return 0
    # If the %K line and the %D line are close, assign a score of 50
    else:
        return 50

def calculate_obv_score(obv):
    # If the OBV line is trending up, assign a score of 100
    if obv.iloc[-1] > obv.iloc[-2]:
        return 100
    # If the OBV line is trending down, assign a score of 0
    elif obv.iloc[-1] < obv.iloc[-2]:
        return 0
    # If the OBV line is flat, assign a score of 50
    else:
        return 50

with DAG(
    dag_id="gcs_to_pg",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    gcs_to_local_fundamental = GoogleCloudStorageDownloadOperator(
        
        task_id = "gcs_to_local_fundamental",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        object_name ='fundamental_data_to_pg',
        filename = '/tmp/fundamental_data_to_pg.csv'
    )

    gcs_to_local_technical = GoogleCloudStorageDownloadOperator(
        
        task_id = "gcs_to_local_technical",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        object_name ='technical_data_to_pg',
        filename = '/tmp/technical_data_to_pg.csv'
    )

    gcs_to_local_sentiment = GoogleCloudStorageDownloadOperator(
        
        task_id = "gcs_to_local_sentiment",
        gcp_conn_id = 'gcp_3107_official',
        bucket = 'is3107',
        object_name ='sentiment_analysis_to_pg',
        filename = '/tmp/sentiment_analysis_to_pg.csv'
    )

    

    create_table_fundamental = PostgresOperator(
        task_id = 'create_table_fundamental',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''CREATE TABLE IF NOT EXISTS fundamental_data(
            Symbol VARCHAR(100),
            fiscalDateEnding DATE,
            totalAssets FLOAT,
            totalCurrentAssets FLOAT,
            cashAndCashEquivalentsAtCarryingValue FLOAT,
            cashAndShortTermInvestments FLOAT,
            inventory FLOAT,
            currentNetReceivables FLOAT, 
            totalNonCurrentAssets FLOAT,
            totalLiabilities FLOAT,
            totalCurrentLiabilities FLOAT,
            currentAccountsPayable FLOAT,
            currentDebt FLOAT,
            totalNonCurrentLiabilities FLOAT,
            retainedEarnings FLOAT,
            totalShareholderEquity FLOAT,
            operatingCashflow FLOAT,
            capitalExpenditures FLOAT,
            changeInReceivables FLOAT,
            changeInInventory FLOAT,
            profitLoss FLOAT,
            cashflowFromInvestment FLOAT,
            cashflowFromFinancing FLOAT,
            netIncome FLOAT,
            grossProfit FLOAT,
            totalRevenue FLOAT,
            costOfRevenue FLOAT,
            ebit FLOAT,
            ebitda FLOAT,
            debtToEquityRatio FLOAT,
            returnOnEquity FLOAT,
            currentRatio FLOAT,
            grossMargin FLOAT,
            quickRatio FLOAT,
            Description text,
            CIK INTEGER,
            MarketCapitalization FLOAT,
            PERatio FLOAT,
            PEGRatio FLOAT,
            BookValue FLOAT,
            DividendYield FLOAT,
            TrailingPE FLOAT,
            ForwardPE FLOAT,
            PriceToBookRatio FLOAT,
            EVToRevenue FLOAT,
            EVToEBITDA FLOAT,
            EPS FLOAT,
            ProfitMargin FLOAT,
            FundamentalScore FLOAT
        )    
        '''
        
    )

    create_table_technical = PostgresOperator(
        task_id = 'create_table_technical',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''CREATE TABLE IF NOT EXISTS technical_data(
            date DATE,
            symbol VARCHAR(100),
            SMA FLOAT,
            EMA50 FLOAT,
            EMA100 FLOAT,
            EMA200 FLOAT,
            WMA FLOAT,
            MACD FLOAT, 
            MACD_Signal FLOAT,
            MACD_Hist FLOAT,
            ADXR FLOAT,
            FastK FLOAT,
            FastD FLOAT,
            OBV FLOAT,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            adjustedClose FLOAT,
            volume FLOAT,
            dividendAmount FLOAT,
            splitCoefficient FLOAT,
            TechnicalScore FLOAT
        )    
        '''
        
    )

    create_table_sentiment = PostgresOperator(
        task_id = 'create_table_sentiment',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''CREATE TABLE IF NOT EXISTS sentiment_analysis(
            date DATE,
            symbol VARCHAR(100),
            Sentiment_ticker_score FLOAT,
            Sentiment_ticker_label VARCHAR(100)
        
        )    
        '''
        
    )

    delete_table_fundamental = PostgresOperator(
        task_id = 'delete_table_fundamental',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''DELETE FROM fundamental_data '''

    )

    delete_table_technical = PostgresOperator(
        task_id = 'delete_table_technical',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''DELETE FROM technical_data '''

    )

    delete_table_sentiment = PostgresOperator(
        task_id = 'delete_table_sentiment',
        postgres_conn_id = 'postgres_is3107_official',
        sql = '''DELETE FROM sentiment_analysis '''

    )

    def transform_fundamental_data():
        df = pd.read_csv('/tmp/fundamental_data_to_pg.csv')
        df = calculate_fundamental_score(df)
        df.to_csv('/tmp/transformed_fundamental_data.csv',index=False)
    
    def transform_technical_data():
        df = pd.read_csv('/tmp/technical_data_to_pg.csv')
        df = calculate_technical_score(df)
        df.to_csv('/tmp/transformed_technical_data.csv',index=False)

    def load_fundamental_data():
        pg_hook = PostgresHook(
            postgres_conn_id = 'postgres_is3107_official',
            schema = 'is3107'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        with open('/tmp/transformed_fundamental_data.csv','r') as file:
            
            reader=csv.reader(file)
            next(reader)
            for row in reader:
                cursor.execute("INSERT INTO fundamental_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                               ,row)
            #next(file)
            # cursor.copy_from(file, 'fundamental_data4' ,sep='|')
        pg_conn.commit()
        cursor.close()
        pg_conn.close()

    def load_technical_data():
        pg_hook = PostgresHook(
            postgres_conn_id = 'postgres_is3107_official',
            schema = 'is3107'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        with open('/tmp/transformed_technical_data.csv','r') as file:
            
            next(file)
            cursor.copy_from(file, 'technical_data' ,sep=',')
        pg_conn.commit()
        cursor.close()
        pg_conn.close()

    def load_sentiment():
        pg_hook = PostgresHook(
            postgres_conn_id = 'postgres_is3107_official',
            schema = 'is3107'
        )
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        with open('/tmp/sentiment_analysis_to_pg.csv','r') as file:
            
            next(file)
            cursor.copy_from(file, 'sentiment_analysis' ,sep=',')
        pg_conn.commit()
        cursor.close()
        pg_conn.close()
    
    task_transform_fundamental_data = PythonOperator(
        task_id = 'transform_fundamental_data',
        python_callable=transform_fundamental_data,
    )

    task_transform_technical_data = PythonOperator(
        task_id = 'transform_technical_data',
        python_callable=transform_technical_data,
    )

    task_load_fundamental_data = PythonOperator(
        task_id = 'load_fundamental_data',
        python_callable=load_fundamental_data,
    )

    task_load_technical_data = PythonOperator(
        task_id = 'load_technical_data',
        python_callable=load_technical_data,
    )

    task_load_sentiment = PythonOperator(
        task_id = 'load_sentiment',
        python_callable=load_sentiment
    )

gcs_to_local_fundamental >> create_table_fundamental >> delete_table_fundamental>>task_transform_fundamental_data>>task_load_fundamental_data
gcs_to_local_technical >> create_table_technical >> delete_table_technical>> task_transform_technical_data >>task_load_technical_data
gcs_to_local_sentiment >> create_table_sentiment>> delete_table_sentiment>> task_load_sentiment