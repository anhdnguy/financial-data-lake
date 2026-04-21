from airflow.sdk import dag, task, TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict

import uuid

default_args={
    'owner': 'Anh',
    'depends_on_past': False
}

@dag('Financial_Data_ETL', schedule='once', default_args=default_args,
     catchup=False, tags=['financial_data_lake', 'ETL'], description='Extracting Data from Schwab',
     params={'pipeline_run_id': str(uuid.uuid4())})
def universe_maintenance():

    @task
    def log_pipeline_start():
        pass

    @task
    def pull_symbol_from_csv():
        pass

    @task
    def sanitize():
        pass

    @task
    def query_active_symbols():
        pass

    @task
    def diff_symbols():
        pass

    @task
    def upsert_membership():
        pass

    @task
    def upsert_universe_membership():
        pass

    @task
    def update_exit_date():
        pass

    @task
    def log_delisted():
        pass

    @task
    def log_pipeline_end():
        pass

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    my_universe = pull_symbol_from_csv()

    start >> my_universe >> end

universe_maintenance()