from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta



with DAG(
    dag_id="technical_table_join",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    create_technical_table_query = """
    CREATE OR REPLACE TABLE `able-brace-379917.project_dataset.technical_data`
    AS
    WITH sma_data as (
        select * from able-brace-379917.project_dataset.sma 
    )
    , ema50_data as (
        select * from able-brace-379917.project_dataset.ema50 
    )
    , ema100_data as (
        select * from able-brace-379917.project_dataset.ema100 
    )
    , ema200_data as (
        select * from able-brace-379917.project_dataset.ema200
    )
    , wma_data as (
        select * from able-brace-379917.project_dataset.wma 
    )
    , macd_data as (
        select * from able-brace-379917.project_dataset.macd 
    )
    , adxr_data as (
        select * from able-brace-379917.project_dataset.adxr 
    )
    , stochRSI_data as (
        select * from able-brace-379917.project_dataset.stochRSI
    )
    , obv_data as (
        select * from able-brace-379917.project_dataset.obv
    ) 
    , ts_daily_adjusted_data as (
        select * from able-brace-379917.project_dataset.ts_daily_adjusted 
    )
    SELECT 
        sma_data.*
        , ema50_data.* except (symbol, date)
        , ema100_data.* except (symbol, date)
        , ema200_data.* except (symbol, date)
        , wma_data.* except (symbol, date)
        , macd_data.* except (symbol, date)
        , adxr_data.* except (symbol, date)
        , stochRSI_data.* except (symbol, date)
        , obv_data.* except (symbol, date)
        , ts_daily_adjusted_data.* except (Symbol, date)
    FROM
        sma_data
    LEFT JOIN ema50_data ON sma_data.symbol = ema50_data.symbol
        AND sma_data.date = ema50_data.date
    LEFT JOIN ema100_data ON sma_data.symbol = ema100_data.symbol
        AND sma_data.date = ema100_data.date
    LEFT JOIN ema200_data ON sma_data.symbol = ema200_data.symbol
        AND sma_data.date = ema200_data.date
    LEFT JOIN wma_data ON sma_data.symbol = wma_data.symbol
        AND sma_data.date = wma_data.date
    LEFT JOIN macd_data ON sma_data.symbol = macd_data.symbol
        AND sma_data.date = macd_data.date
    LEFT JOIN adxr_data ON sma_data.symbol = adxr_data.symbol
        AND sma_data.date = adxr_data.date
    LEFT JOIN stochRSI_data ON sma_data.symbol = stochRSI_data.symbol
        AND sma_data.date = stochRSI_data.date
    LEFT JOIN obv_data ON sma_data.symbol = obv_data.symbol
        AND sma_data.date = obv_data.date
    LEFT JOIN ts_daily_adjusted_data ON sma_data.symbol = ts_daily_adjusted_data.symbol
        AND sma_data.date = ts_daily_adjusted_data.date   
    """

    create_table_operator = BigQueryExecuteQueryOperator(
        task_id='create_table_from_query',
        sql=create_technical_table_query,
        gcp_conn_id = 'gcp_3107_official',
        use_legacy_sql=False,
        dag=dag,
    )
