from requests import get
import pandas as pd
import yfinance as yf
import talib as tb
from airflow.decorators import task
from google.cloud import storage
import os
from params import google_cloud_path, gs_bucket
from datetime import datetime
from dateutil.relativedelta import relativedelta

from check_gcs_conn import blob_exists, blob_download_to_df
from bs4 import BeautifulSoup
import random
import requests
import io

from airflow.operators.python import PythonOperator

from fomc import FomcStatement, FomcMinutes
from news import News

def get_adj_close(tickers, start_date, end_date):
    """
    Returns Stock Price Information from Yahoo Finance
    """
    returns_df = pd.DataFrame(columns=['Ticker', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close'])
    cannot_find = []
    for ticker in tickers:
        # retrieve stock data (includes Date, OHLC, Volume, Adjusted Close)
        try:
          # ADd interval = w / m to change timeframe
          s = yf.download(ticker, start_date, end_date)
          s['Ticker'] = ticker
          returns_df = returns_df.append(s)
        except Exception as e:
          print(e)
          cannot_find.append(ticker)
    print(cannot_find)
    return returns_df

def array_to_df(values, ticker, ta, subta=None, blob_exists=True):
    """
    Helper function to convert price array to DataFrame
    """
    df = pd.DataFrame(values, columns=['Value'])
    df = df.dropna()
    df['Ticker'] = ticker
    df['TA'] = ta
    df['TA_2'] = subta
    df = df.rename_axis('Date').reset_index()
    return df

def _get_user_agent() -> str:
    """
    Get a random User-Agent strings from a list of some recent real browsers
    """
    user_agent_strings = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:82.1) Gecko/20100101 Firefox/82.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:83.0) Gecko/20100101 Firefox/83.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:84.0) Gecko/20100101 Firefox/84.0",
    ]

    return random.choice(user_agent_strings)

def _get_html(url: str):
    """
    Wraps HTTP requests.get for testibility.
    Fake the user agent by changing the User-Agent header of the request
    and bypass such User-Agent based blocking scripts used by websites.
    """
    return requests.get(url, headers={"User-Agent": _get_user_agent()}).text

def split_fear_gread_web_text_to_dataframe(str1):
    tmp1 = str1.split(")")
    list1 = []
    for i in tmp1:
        list1.extend(i.split(":"))
    list2 = []
    for j in list1:
        list2.extend(j.split("("))
    list2 = list2[:-1]
    for j in range(len(list2)):
        list2[j] = list2[j].replace("Fear & Greed", "").strip()
    time_list = []
    value_list = []
    text_list = []
    for i in range(len(list2)):
        if i % 3 == 0: time_list.append(list2[i])
        elif i % 3 == 1: value_list.append(list2[i])
        else: text_list.append(list2[i])

    df = pd.DataFrame({"Time":time_list, "FG_Value":value_list, "FG_Textvalue":text_list})
    return df

def get_most_recent_date_of_prices(bucket_name):
    """
    Returns the most recent date of prices from previous runs
    """
    #check if the bucket exists
    try:
        client = storage.Client()
        gcs_available = True
    except:
        gcs_available = False

    if gcs_available:
        if client.bucket(bucket_name).exists():
        #if there is the bucket, check if prices.csv has been extracted before
            if blob_exists(bucket_name, 'prices.csv'):
                prices = blob_download_to_df(bucket_name, 'prices.csv')
                #if yes, get the most recent date of last run
                most_recent_date = prices['Date'].max()
                recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
            else: 
                #if no, then get historical data which is 1 year ago
                recent_date = datetime.today() - relativedelta(months=12)
        else:
            recent_date = datetime.today() - relativedelta(months=12)
    else:
        #if no, then get historical data which is 1 year ago
        recent_date = datetime.today() - relativedelta(months=12)
    return recent_date

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_cloud_path
recent_date = get_most_recent_date_of_prices(gs_bucket)

def extract_data_task_group():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    def get_gcs_availability(**kwargs):
        return kwargs['ti'].xcom_pull(task_ids=f"check_conn")

    @task()
    def scrape_sti_tickers():
        """
        Scrape latest STI Tickers from Wikipedia
        """
        sti = pd.read_html('https://en.wikipedia.org/wiki/Straits_Times_Index', match='List of STI constituents')[0]
        sti['Stock Symbol'] = sti['Stock Symbol'].apply(lambda x: x.split(" ")[1] + ".SI" )
        return sti.set_index('Stock Symbol').to_dict()['Company']
    
    @task(task_id='extract_all_prices')
    def extract_all_prices(sti_tickers, bucket_name):
        """
        Extract all prices for stock market analysis
        """
        fixed_tickers = {
            'CL=F': 'Crude Oil',
            'BZ=F': 'Brent Oil',
            'ZC=F': 'Corn',
            'ZS=F': 'Soybean',
            'ZR=F': 'Rice',
            'KC=F': 'Coffee',
            'GC=F': 'Gold',
            'SI=F': 'Silver',
            'HG=F': 'Copper',
            'SB=F': 'Sugar',
            '^TNX': 'US Treasury Yield 10 Years',
            '^TYX': 'US Treasury Yield 30 Years',
            '^FVX': 'US Treasury Yield 5 Years'
        }
        full_tickers = {**sti_tickers, **fixed_tickers}
        tickers_list = [*full_tickers.keys()]
        #check if prices data is extracted before
        if gcs_available and blob_exists(bucket_name, 'prices.csv'):
            #if yes, get the most recent date and extract past one month data from the recent date till current date for TA analysis
            prices_df = get_adj_close(tickers_list, start_date=(recent_date - relativedelta(months=1)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            prices_df = get_adj_close(tickers_list, start_date=(recent_date).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        prices_df['Name'] = prices_df['Ticker'].map(full_tickers)
        return prices_df.rename_axis('Date').reset_index()

    @task(task_id='extract_stock_info')
    def extract_stock_info(sti_tickers, bucket_name):
        """
        Extract the latest stock information (Reference Data) from Yahoo Finance
        """
        if (gcs_available is False) or (blob_exists(bucket_name, 'stock_info.csv') is False) or (datetime.today().day == 1 and datetime.today().month in [1, 4, 7, 10]):
            tickers_list = [*sti_tickers.keys()]
            info_df = None
            for i in tickers_list:
                # Stock Info
                try:
                    stock = yf.Ticker(i)
                    stock_info = stock.info
                    stock_info_df = pd.DataFrame([[sti_tickers[i], i, 'SG', stock_info['industry'], stock_info['longBusinessSummary']]], columns=['Stock', 'Ticker', 'Market', 'Industry', 'Summary'])
                    if info_df is None:
                        info_df = stock_info_df
                    else:
                        info_df = pd.concat([info_df, stock_info_df])
                except Exception as e:
                    continue
            return info_df.reset_index(drop=True)
        else:
            return None

    @task(task_id='extract_stock_fundamentals')
    def extract_stock_fundamentals(sti_tickers, bucket_name):
        """
        Extract latest stock fundamentals (Accounting Data) from Yahoo Finance
        """
        if (gcs_available is False) or (blob_exists(bucket_name, 'stock_fundamentals.csv') is False) or (datetime.today().day == 1 and datetime.today().month in [1, 4, 7, 10]):
            tickers_list = [*sti_tickers.keys()]
            fun_df = None
            fund_list = ['Total Revenue', 'Total Assets', 'Cash', 'Total Current Liabilities', 'Total Liab', 'Net Income', 'Total Stockholder Equity', 'Common Stock', 'Treasury Stock']
            for i in tickers_list:
                # Stock Fundamentals (Balance Sheet + Financials)
                stock = yf.Ticker(i)
                fundamentals_df = pd.concat([stock.quarterly_balance_sheet.T, stock.quarterly_financials.T], axis=1)
                for fund in fund_list:
                    if fund not in fundamentals_df.columns:
                        fundamentals_df[fund] = None
                final_fundamentals_df = fundamentals_df[['Total Revenue', 'Total Assets', 'Cash', 'Total Current Liabilities', 'Total Liab', 'Net Income', 'Total Stockholder Equity', 'Common Stock', 'Treasury Stock']]
                final_fundamentals_df['Ticker'] = i
                final_fundamentals_df['Stock'] = sti_tickers[i]
                if fun_df is None:
                    fun_df = final_fundamentals_df
                else:
                    fun_df = pd.concat([fun_df, final_fundamentals_df])
            return fun_df.rename_axis('Date').reset_index()
        else:
            return None

    @task(task_id='extract_stock_dividends')
    def extract_stock_dividends(sti_tickers):
        """
        Extract latest stock dividends from Yahoo Finance
        """
        tickers_list = [*sti_tickers.keys()]
        div_df = None
        for i in tickers_list:
            # Dividends 
            stock = yf.Ticker(i)
            dividend_df = pd.DataFrame(stock.dividends)
            dividend_df['Ticker'] = i
            dividend_df['Stock'] = sti_tickers[i]
            if div_df is None:
                div_df = dividend_df
            else:
                dividend_df = pd.concat([div_df, dividend_df])
        return dividend_df.rename_axis('Date').reset_index()

    @task(task_id='extract_exchange_rates')
    def extract_exchange_rates(bucket_name):
        """
        Extract Latest Exchange Rates from Yahoo Finance
        """
        exchange_rate_mapping = {
            'SGDUSD=X': 'SGD/USD',
            'SGDCNY=X': 'SGD/CNY',
            'SGDEUR=X': 'SGD/EUR',
            'SGDJPY=X': 'SGD/JPY',
            'SGDGBP=X': 'SGD/GBP'
        }
        exchange_rate_list = [*exchange_rate_mapping.keys()]
        if gcs_available and blob_exists(bucket_name, 'exchange_rates.csv'):
            exchange_rate_df = get_adj_close(exchange_rate_list, start_date=(datetime.today() - relativedelta(months=2)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        else:    
            exchange_rate_df = get_adj_close(exchange_rate_list, start_date=(datetime.today() - relativedelta(months=12)).strftime("%Y-%m-%d"), end_date=(datetime.today()).strftime("%Y-%m-%d"))
        exchange_rate_df['Exchange_rate'] = exchange_rate_df['Ticker'].map(exchange_rate_mapping)
        #sgd_sgd = pd.DataFrame(columns=['Ticker', 'High', 'Low', 'Open', 'Close', 'Volume', 'Adj Close'])
        return exchange_rate_df.rename_axis('Date').reset_index()

    @task(task_id='extract_sg_ir')
    def extract_sg_ir(bucket_name):
        """
        Extract latest SORA data from MAS API
        """
        if gcs_available and blob_exists(bucket_name, 'sg_ir.csv'):
            start_date = (datetime.today()- relativedelta(months=6)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d") 
        else:    
            start_date = (datetime.today()- relativedelta(months=24)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d") 
        url = f'https://eservices.mas.gov.sg/api/action/datastore/search.json?resource_id=9a0bf149-308c-4bd2-832d-76c8e6cb47ed&between[end_of_day]={start_date},{end_date}'
        headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'}
        response = get(url, headers=headers)
        sg_interest_rate = pd.DataFrame.from_dict(response.json()['result']['records'])
        sg_interest_rate = sg_interest_rate.rename(columns={'end_of_day': 'Date'})
        sg_interest_rate['Date']= pd.to_datetime(sg_interest_rate['Date'])
        return sg_interest_rate

    @task(task_id='extract_fear_greed_index')
    def extract_fear_greed_index(bucket_name):
        """
        Scrapes CNN Fear and Greed Index HTML page
        """

        #daily extract when there is no historical value or today's data is not updated 
        #Scrape data from webpage
        text_soup_cnn = BeautifulSoup(
            _get_html("https://money.cnn.com/data/fear-and-greed/"),
            "lxml",
        )

        # Extract in fear and greed index
        index_data = (
            text_soup_cnn.findAll("div", {"class": "modContent feargreed"})[0]
            .contents[0]
            .text
        )
        
        #Format web data into dataframe
        transformed_df = split_fear_gread_web_text_to_dataframe(index_data)
        daily_data_df = transformed_df.iloc[0:1]
        data_date = (datetime.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        daily_data_df.insert(0, "Date", data_date, True)
        prev_close = transformed_df._get_value(1, 'FG_Value')
        daily_data_df['FG_Close'] = [prev_close]
        prev_close_text = transformed_df._get_value(1, 'FG_Textvalue')
        daily_data_df['FG_Closetext'] = [prev_close_text]
        result = daily_data_df
        return result

    @task(task_id='extract_esg_score')
    def extract_esg_score(sti_tickers, bucket_name):
        """
        Scrapes latest ESG Score from Yahoo Finance
        """

        #daily extract 
        tickers_list = [*sti_tickers.keys()]
        main_df = pd.DataFrame()
        esg_data = pd.DataFrame()
        for ticker in tickers_list:
            try:
                ticker_name = yf.Ticker(ticker)
                ticker_info = ticker_name.info
                ticker_df = pd.DataFrame.from_dict(ticker_info.items()).T
                #the above line will parse the dict response into a DataFrame
                ticker_df.columns = ticker_df.iloc[0]
                #above line will rename all columns to first row of dataframe
                #as all the headers come up in the 1st row, next line will drop the 1st line
                ticker_df = ticker_df.drop(ticker_df.index[0])
                main_df = main_df.append(ticker_df)
                #if no response from Yahoo received, it will pass to next ticker
                if ticker_name.sustainability is not None: 
                    #response dataframe
                    ticker_df_esg = ticker_name.sustainability.T 
                    #adding new column 'symbol' in response df
                    ticker_df_esg['symbol'] = ticker 
                    #attaching the response df to esg_data
                    esg_data = esg_data.append(ticker_df_esg) 
            #in case yfinance API misbehaves
            except (IndexError, ValueError) as e: 
                print(e)
        main_df = main_df[['symbol', 'sector', 'previousClose', 'sharesOutstanding']]
        esg_data = esg_data[['symbol', 'socialScore', 'governanceScore', 'totalEsg', 'environmentScore']]
        final_df = main_df.merge(esg_data, how='left', on='symbol')
        data_date = (datetime.today() - relativedelta(days=1)).strftime("%Y-%m-%d")
        final_df.insert(0, "Date", data_date, True)

        return final_df 

    @task(task_id='extract_fomc_statement')
    def extract_fomc_statement(bucket_name):
        """
        Return FOMC Statement Data
        """
        if gcs_available and blob_exists(bucket_name, 'fomc_statement.csv'):
            from_year = int((datetime.today()- relativedelta(months=3)).strftime("%Y"))
        else:    
            from_year = int((datetime.today()- relativedelta(months=6)).strftime("%Y"))

        fomc = FomcStatement()
        df = fomc.get_contents(from_year)

        return df
        
    @task(task_id='extract_fomc_minutes')
    def extract_fomc_minutes(bucket_name):
        """
        Return FOMC Minutes Data
        """
        if gcs_available and blob_exists(bucket_name, 'fomc_minutes.csv'):
            from_year = int((datetime.today()- relativedelta(months=3)).strftime("%Y"))
        else:    
            from_year = int((datetime.today()- relativedelta(months=6)).strftime("%Y"))
        
        fomc = FomcMinutes()
        df = fomc.get_contents(from_year)

        return df

    @task(task_id='extract_news_sources')
    def extract_news_sources(bucket_name):
        """
        Return RavenPack News Headline + Sentiment Data
        """
        if gcs_available and blob_exists(bucket_name, 'news_sources.csv'):
            news_df = blob_download_to_df(bucket_name, 'news_sources.csv')
            #if yes, get the most recent date
            news_df['timestamp_tz'] = news_df['timestamp_tz'].apply(lambda x : x[:-4]) # remove decimals
            most_recent_date = news_df['timestamp_tz'].max()
            start_date = datetime.strptime(most_recent_date, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d") 

        else:    
            # else take 3 months historical data
            start_date = (datetime.today()- relativedelta(months=3)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d")

        news = News(False)
        news_sources_df = news.get_contents(start_date, end_date)

        return news_sources_df
    
    @task(task_id='extract_news_volume_spikes')
    def extract_news_volume_spikes(bucket_name):
        """
        Return RavenPack News Volume Spikes
        """
        if gcs_available and blob_exists(bucket_name, 'news_volume_spikes.csv'):
            news_vol_df = blob_download_to_df(bucket_name, 'news_sources.csv')
            news_vol_df['timestamp_tz'] = news_vol_df['timestamp_tz'].apply(lambda x : x[:-4]) # remove decimals
            #if yes, get the most recent date
            most_recent_date = news_vol_df['timestamp_tz'].max()
            start_date = datetime.strptime(most_recent_date, '%Y-%m-%d %H:%M:%S').strftime("%Y-%m-%d")
            end_date = datetime.today().strftime("%Y-%m-%d")

        else:    
            # else take 3 months historical data
            start_date = (datetime.today()- relativedelta(months=3)).strftime("%Y-%m-%d") 
            end_date = datetime.today().strftime("%Y-%m-%d")

        news = News(True)
        news_volume_spikes_df = news.get_contents(start_date, end_date)

        return news_volume_spikes_df


    # Run Tasks 
    bucket = gs_bucket
    gcs_available = PythonOperator(task_id=f'check_gcs_available', python_callable=get_gcs_availability, provide_context=True)
    
    # Scrape STI Tickers
    tickers = scrape_sti_tickers()
    # Stock Prices 
    price_df = extract_all_prices(tickers, bucket_name=bucket)
    # Stock Info, Fundamentals and Div
    stock_info_df = extract_stock_info(tickers, bucket_name=bucket)
    stock_fundamentals_df = extract_stock_fundamentals(tickers, bucket_name=bucket)
    stock_dividends_df = extract_stock_dividends(tickers)
    # SG Exchange Rates
    sg_exchange_rates = extract_exchange_rates(bucket_name=bucket)
    # SG Interest Rates
    sg_ir_df = extract_sg_ir(bucket_name=bucket)
    # Fear and Greed Index 
    fear_greed_index_df = extract_fear_greed_index(bucket_name=bucket)
    esg_score_df = extract_esg_score(tickers,  bucket_name=bucket)
    # FOMC Minutes + Statements
    fomc_st_df = extract_fomc_statement(bucket_name=bucket)
    fomc_min_df = extract_fomc_minutes(bucket_name=bucket)
    # News
    news_sources_df = extract_news_sources(bucket_name=bucket)
    news_volume_spikes_df = extract_news_volume_spikes(bucket_name=bucket)