from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict

# Import client
from src.db_client.db_client import DBClient

# Import services
from src.services.universe_service import UniverseService

# Import config
from src.config import AppConfig

# Import bootstrap helper
from src.utilities.bootstrap import get_service

import uuid

default_args={
    'owner': 'Anh',
    'depends_on_past': False
}

@dag('universe_maintenance', schedule='once', default_args=default_args,
     catchup=False, tags=['financial_data_lake', 'ETL'], description='Extracting Data from Schwab')
def universe_maintenance():

    @task
    def log_pipeline_start() -> None:
        pipeline_run_id = str(uuid.uuid4())
        Variable.set("pipeline_run_id", pipeline_run_id)
        service = get_service("universe_maintenance", pipeline_run_id)

        with service:
            service.pipeline_start()

    @task
    def pull_symbol_from_csv() -> Dict[str, List]:
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        return service.pull_symbol()

    @task
    def sanitize(symbol_lists: Dict[str, List]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        return service.sanitize_symbol(symbol_lists)

    @task
    def query_active_symbols(symbol_lists: Dict[str, List]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)

        with service:
            symbols = service.query_symbols(symbol_lists)
        return symbols

    @task
    def diff_symbols(dict_symbols: Dict[Dict[str, List]]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        return service.diff_query_import(dict_symbols)

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

    log_pipeline_start_task = log_pipeline_start()

    pull_symbol_from_csv_task = pull_symbol_from_csv()

    sanitize_task = sanitize(pull_symbol_from_csv_task)

    query_active_symbols_task = query_active_symbols(sanitize_task)

    diff_symbols_task = diff_symbols(query_active_symbols_task)

    upsert_membership_task = upsert_membership()

    upsert_universe_membership_task = upsert_universe_membership()

    update_exit_date_task = update_exit_date()

    log_delisted_task = log_delisted()

    log_pipeline_end_task = log_pipeline_end()

    # Extract tickers
    start >> log_pipeline_start_task >> pull_symbol_from_csv_task
    pull_symbol_from_csv_task >> sanitize_task >> query_active_symbols_task
    query_active_symbols_task >> diff_symbols_task

    # Extract new or delisted tickers
    diff_symbols_task >> [upsert_membership_task, update_exit_date_task]

    # Process new tickers
    upsert_membership_task >> upsert_universe_membership_task

    # Process delisted tickers
    update_exit_date_task >> log_delisted_task

    # Final chain
    [upsert_universe_membership_task, log_delisted_task] >> log_pipeline_end_task

    log_pipeline_end_task >> end


universe_maintenance()