from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.tableau.operators.tableau import TableauOperator
from airflow.providers.tableau.hooks.tableau import TableauHook
import datetime

# Define your TableauHook connection
tableau_conn = TableauHook(tableau_conn_id='tableau_default')

# Define a Python function to publish a Tableau workbook
def publish_workbook():
    # Specify the path to your Tableau workbook file
    workbook_file = 'C:/Users/wyero/OneDrive/Documents/My Tableau Repository/Workbooks/~Book1_18836.twbr'


    TableauOperator(
        resource="workbooks",
        method="refresh",
        find="Book1",
        match_with="Book1",
        task_id="refresh_tableau_workbook"
    )

with DAG(
    dag_id="example_tableau_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    publish_workbook_operator = PythonOperator(
    task_id='publish_workbook',
    python_callable=publish_workbook,
    dag=dag
    )


