from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator



with DAG(
    dag_id="top10_volume_closing_prices",
    start_date=datetime(2022, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:

    create_technical_table_query = """
    CREATE OR REPLACE TABLE  `able-brace-379917.project_dataset.top10_volume_closing_prices` AS
    SELECT `able-brace-379917.project_dataset.snp500_ticker_data`.ticker, `able-brace-379917.project_dataset.snp500_ticker_data`.date, `able-brace-379917.project_dataset.snp500_ticker_data`.close
    FROM `able-brace-379917.project_dataset.snp500_ticker_data`
    INNER JOIN `able-brace-379917.project_dataset.top_10_volume`
    ON `able-brace-379917.project_dataset.snp500_ticker_data`.ticker = `able-brace-379917.project_dataset.top_10_volume`.ticker; 

    """

    create_table_operator = BigQueryExecuteQueryOperator(
        task_id='create_table_from_query',
        sql=create_technical_table_query,
        gcp_conn_id = 'gcp_3107_official',
        use_legacy_sql=False,
        dag=dag,
    )

    bq_to_gcs = BigQueryToGCSOperator(
        task_id = "bq_to_gcs_top10",
        gcp_conn_id = 'gcp_3107_official',
        source_project_dataset_table = 'able-brace-379917.project_dataset.top10_volume_closing_prices',
        destination_cloud_storage_uris = ["gs://is3107/top10_volume_closing_prices"],
        export_format = 'CSV'
    )

create_table_operator >> bq_to_gcs