import datetime
import pandas as pd
import yfinance as yf
import numpy as np
import cvxpy as cvx
from pypfopt import EfficientFrontier
from pypfopt import risk_models
from pypfopt import expected_returns
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

    #Cumulative returns of assets
    #returns_df.expanding(0).apply(lambda x: sum(x) * 100, raw = True).plot(figsize = (10, 5))
    #plt.title("Asset Log Returns")
    #plt.ylabel("Log Returns (%)")
    #plt.axhline(y=0, color='black', linestyle='--')

    #Optimize portfolio
    #def get_optimized_portfolio(returns_df, returns_scale = .0001, max_holding = 0.5):
    
    #    # convert returns dataframe to numpy array
    #    returns = returns_df.T.to_numpy()
    #    # m is the number of assets
    #    m = returns.shape[0]
  
    #    # covariance matrix of returns
    #    cov = np.cov(returns)

    #    # creating variable of weights to optimize
    #    x = cvx.Variable(m)
    
        # portfolio variance, in quadratic form
    #    portfolio_variance = cvx.quad_form(x, cov)
    #    print("return in simple returns")
    #    print(returns_df)
    #    log_returns_df = np.log(returns_df+1)
    #    print("return in log returns")
    #    print(log_returns_df)
    #    total_return_log = log_returns_df.sum().to_numpy() #this is in log space, change to simple return
    #    print("total return in log")
    #    print(total_return_log)
    #    print("total simple return")

    #    total_simple_return = np.exp(total_return_log) -1
    #    print(total_simple_return)
    #    frequency = 252 #assume daily compounding, we are going to take geometric average
    #    #this is the standard basic mean for optimization (to assume daily compounding)
    
    #    horizon_length = returns.shape[1]
    #    expected_mean = (1 + total_simple_return) ** (1 / horizon_length) - 1
    #    print("geometric return")
    #    print(expected_mean)
    #    #let's assume 
    #    # element wise multiplication, followed up by sum of weights
    #    portfolio_return = sum(cvx.multiply(expected_mean, x))
        
    #    # Objective Function
    #    # We want to minimize variance and maximize returns. We can also minimize the negative of returns.
    #    # Therefore, variance has to be a positive and returns have to be a negative.
    #    objective = cvx.Minimize(portfolio_variance - returns_scale * portfolio_return)
        
    #    # Constraints
    #    # long only, sum of weights equal to 1, no allocation to a single stock great than 50% of portfolio
    #    constraints = [x >= 0, sum(x) == 1, x <= max_holding]

    #    # use cvxpy to solve the objective
    #    problem = cvx.Problem(objective, constraints)
    #    # retrieve the weights of the optimized portfolio
    #    result = problem.solve()
        
    #    return x.value


def load_ticker_data():
    pg_hook = PostgresHook(
        postgres_conn_id = 'postgres_db',
        schema = 'db_test'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    with open('/private/tmp/logreturns.csv','r') as file:
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

