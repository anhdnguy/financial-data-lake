import logging
import uuid

from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict

from src.clients.db_client import DBClient
from src.services.universe_service import UniverseService
from src.services.pipeline_exception import PipelineError
from src.transform.universe_transform import _sanitize_symbol, _sort_delisted_from_active
from src.utilities.bootstrap import get_service

log = logging.getLogger(__name__)

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
        try:
            with service:
                service.pipeline_start()
        except PipelineError as e:
            log.error("log_pipeline_start failed: %s", e)
            raise

    @task
    def pull_symbol_from_csv() -> Dict[str, List]:
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        try:
            return service.pull_symbol()
        except PipelineError as e:
            log.error("pull_symbol_from_csv failed: %s", e)
            raise

    @task
    def sanitize(symbol_lists: Dict[str, List]):
        return _sanitize_symbol(symbol_lists)

    @task
    def query_active_symbols(symbol_lists: Dict[str, List]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        try:
            with service:
                symbols = service.query_symbols(symbol_lists)
            return symbols
        except PipelineError as e:
            log.error("query_active_symbols failed: %s", e)
            raise

    @task
    def diff_symbols(dict_symbols: Dict[Dict[str, List]]):
        return _sort_delisted_from_active(dict_symbols)

    @task
    def upsert_membership(current_and_delisted: Dict[str, Dict[str, List]]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        try:
            with service:
                service.upsert_membership_universe(current_and_delisted)
        except PipelineError as e:
            log.error("upsert_membership failed: %s", e)
            raise

    @task
    def update_exit_date(current_and_delisted: Dict[str, Dict[str, List]]):
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        try:
            with service:
                return service.update_exit_date(current_and_delisted)
        except PipelineError as e:
            log.error("update_exit_date failed: %s", e)
            raise

    @task
    def log_delisted(current_and_delisted: Dict[str, Dict[str, List]]):
        for universe, data in current_and_delisted.items():
            delisted = data["delisted_symbols"]
            if delisted:
                log.info("Delisted from %s (%d symbols): %s", universe, len(delisted), delisted)
            else:
                log.info("No delistings for universe %s", universe)

    @task
    def log_pipeline_end():
        pipeline_run_id = Variable.get("pipeline_run_id")
        service = get_service("universe_maintenance", pipeline_run_id)
        try:
            with service:
                service.pipeline_end()
        except PipelineError as e:
            log.error("log_pipeline_end failed: %s", e)
            raise
        finally:
            Variable.delete("pipeline_run_id")

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    log_pipeline_start_task = log_pipeline_start()

    pull_symbol_from_csv_task = pull_symbol_from_csv()

    sanitize_task = sanitize(pull_symbol_from_csv_task)

    query_active_symbols_task = query_active_symbols(sanitize_task)

    diff_symbols_task = diff_symbols(query_active_symbols_task)

    upsert_membership_task = upsert_membership(diff_symbols_task)

    update_exit_date_task = update_exit_date(diff_symbols_task)

    log_delisted_task = log_delisted(update_exit_date_task)

    log_pipeline_end_task = log_pipeline_end()

    # Extract tickers
    start >> log_pipeline_start_task >> pull_symbol_from_csv_task
    pull_symbol_from_csv_task >> sanitize_task >> query_active_symbols_task
    query_active_symbols_task >> diff_symbols_task

    # Final chain
    [upsert_membership_task, log_delisted_task] >> log_pipeline_end_task

    log_pipeline_end_task >> end


universe_maintenance()