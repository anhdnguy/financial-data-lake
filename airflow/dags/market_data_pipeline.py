from airflow.sdk import dag, task, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict

default_args={
    'owner': 'Anh',
    'depends_on_past': False
}

@dag('Financial_Data_ETL', schedule='once', default_args=default_args,
     catchup=False, tags=['financial_data_lake', 'ETL'], description='Extracting Data from Schwab')
def financial_data_pipeline():

    @task
    def top_5000_us_equities():
        pass

    @task
    def fetch_ohlcv():
        pass

    @task
    def load_to_s3():
        pass

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Get top 5000 US equities
    top_5000 = top_5000_us_equities()

    # Fetch OHLCV data
    with TaskGroup(group_id='equitites_fetching') as equitities_group:
        equitity_ohlcv = fetch_ohlcv.expand(chunk=top_5000)

    # Load OHLCV data to S3
    with TaskGroup(group_id='equitities_loading') as load_group:
        load_ohlcv = load_to_s3()

    start >> top_5000 >> equitities_group >> load_group >> end

financial_data_pipeline()
