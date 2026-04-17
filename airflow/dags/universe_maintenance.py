from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict

default_args={
    'owner': 'Anh',
    'depends_on_past': False
}

@dag('Financial_Data_ETL', schedule='once', default_args=default_args,
     catchup=False, tags=['financial_data_lake', 'ETL'], description='Extracting Data from Schwab')
def universe_maintenance():

    @task
    def pull_symbol_from_csv():
        pass
    
    @task
    def insert_to_membership_table():
        pass

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    my_universe = pull_symbol_from_csv()
    insert_to_table = insert_to_membership_table()

    start >> my_universe >> insert_to_table >> end

universe_maintenance()