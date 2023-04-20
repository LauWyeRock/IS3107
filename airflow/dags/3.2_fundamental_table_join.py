from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.bigquery  import BigQueryExecuteQueryOperator
# from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# dataset_name = "project_dataset"

# fundamental_data_join = f"with balance_sheet_data as (select * from project_dataset.balance_sheet), cash_flow_data as (select * from project_dataset.cash_flow), income_statement_data as (select * from project_dataset.income_statement), overview_data as (select * from project_dataset.overview) select balance_sheet_data.*, cash_flow_data.* except (Symbol), income_statement_data.* except (Symbol), overview_data.* except (Symbol) from balance_sheet_data LEFT JOIN cash_flow_data on balance_sheet_data.Symbol = cash_flow_data.Symbol LEFT JOIN income_statement_data on balance_sheet_data.Symbol = income_statement.Symbol LEFT JOIN overview_data on balance_sheet_data.Symbol = overview_data.Symbol"

# fundamental_join_query = BigQueryInsertJobOperator(
#     task_id='fundamental_join_query',
#     configuration = {
#         "query" : {
#             "query" : fundamental_data_join,
#             "useLegacySql": False,
#         }
#     },
#     location = LOCATION,
#     deferrable = True
#     )

with DAG(
    dag_id="fundamental_table_join",
    start_date=datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:



#To join all tables as a new table using query
  create_fundamental_table_query = """
  CREATE OR REPLACE TABLE `able-brace-379917.project_dataset.fundamental_data`
AS
WITH balance_sheet_data_new AS 
  (
    WITH balance_sheet_current AS (
      SELECT * 
      FROM able-brace-379917.project_dataset.balance_sheet
      )
    , balance_sheet_lagged AS (
      SELECT 
        Symbol
        ,fiscalDateEnding
        ,LAG(totalShareholderEquity, 1, 0) OVER (PARTITION BY Symbol ORDER BY fiscalDateEnding) AS laggedShareholderEquity
      FROM able-brace-379917.project_dataset.balance_sheet
    )
    SELECT 
      balance_sheet_current.*
      , balance_sheet_lagged.* EXCEPT (Symbol,fiscalDateEnding)

    FROM
      balance_sheet_current LEFT JOIN balance_sheet_lagged ON balance_sheet_current.Symbol = balance_sheet_lagged.Symbol
      AND balance_sheet_current.fiscalDateEnding = balance_sheet_lagged.fiscalDateEnding    

  )
, cash_flow_data AS (SELECT * from able-brace-379917.project_dataset.cash_flow)
, income_statement_data AS (SELECT * from able-brace-379917.project_dataset.income_statement)
, overview_data AS (SELECT * from able-brace-379917.project_dataset.overview) 
SELECT 
    balance_sheet_data_new.* EXCEPT (laggedShareholderEquity)
    , cash_flow_data.* EXCEPT (Symbol,fiscalDateEnding)
    , income_statement_data.* EXCEPT (Symbol,fiscalDateEnding)
    , balance_sheet_data_new.totalLiabilities / balance_sheet_data_new.totalShareholderEquity AS debtToEquityRatio
    , CASE WHEN
      balance_sheet_data_new.laggedShareholderEquity > 0  then cash_flow_data.netIncome / (0.5 * (balance_sheet_data_new.totalShareholderEquity + balance_sheet_data_new.laggedShareholderEquity))
      ELSE cash_flow_data.netIncome / balance_sheet_data_new.totalShareholderEquity END AS returnOnEquity
    , balance_sheet_data_new.totalCurrentAssets / balance_sheet_data_new.totalCurrentLiabilities AS currentRatio
    , income_statement_data.grossProfit / income_statement_data.totalRevenue AS grossMargin
    , (balance_sheet_data_new.totalCurrentAssets - balance_sheet_data_new.inventory) / balance_sheet_data_new.totalLiabilities AS quickRatio
    , overview_data.* EXCEPT (Symbol,ebitda) 
FROM balance_sheet_data_new 
LEFT JOIN cash_flow_data ON balance_sheet_data_new.Symbol = cash_flow_data.Symbol 
  AND balance_sheet_data_new.fiscalDateEnding = cash_flow_data.fiscalDateEnding
LEFT JOIN income_statement_data ON balance_sheet_data_new.Symbol = income_statement_data.Symbol 
  AND balance_sheet_data_new.fiscalDateEnding = income_statement_data.fiscalDateEnding
LEFT JOIN overview_data ON balance_sheet_data_new.Symbol = overview_data.Symbol
  """

  create_table_operator = BigQueryExecuteQueryOperator(
      task_id='create_table_from_query',
      sql=create_fundamental_table_query,
      gcp_conn_id = 'gcp_3107_official',
      use_legacy_sql=False,
      dag=dag,
  )