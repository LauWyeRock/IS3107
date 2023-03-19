from datetime import datetime, timedelta
import requests
import json
import pendulum
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator

API_1_URL = "https://api.etherscan.io/api?module=proxy&action=eth_blockNumber&apikey=72EUK8FPKYZUXNV6Z516JCD23H2VM1BC9Y"
API_2_URL = "https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag=0xff8a53&boolean=true&apikey=72EUK8FPKYZUXNV6Z516JCD23H2VM1BC9Y"

with DAG(
        "ethereum_pipeline",
        default_args={"retries": 2}, 
        description="DAG to get newest block number and total number of transactions",
        schedule_interval=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=['example'],
) as dag:

    def get_block_num(ti):
        response = requests.get(API_1_URL)
        block_num = response.json()["result"] 
        ti.xcom_push(key="block_num",value=block_num)
        print(f"Latest block number: {block_num}")

    def get_transaction_num(ti):
        block_num = ti.xcom_pull(key="block_num", task_ids=["get_block_num"])[0]
        API_2_URL = f"https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag={block_num}&boolean=true&apikey=72EUK8FPKYZUXNV6Z516JCD23H2VM1BC9Y"
        response = requests.get(API_2_URL)
        transaction_num = len(response.json()['result']["transactions"])
       
        print(f"Total number of transactions: {transaction_num}")

    task_1 = PythonOperator(
        task_id="get_block_num",
        python_callable=get_block_num,
        dag=dag,
    )

    task_2 = PythonOperator(
        task_id="get_transaction_num",
        python_callable=get_transaction_num,
        dag=dag,
    )

    task_1 >> task_2
