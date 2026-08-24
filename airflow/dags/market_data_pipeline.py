from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict
import uuid

from src.utilities.bootstrap import get_market_data_service
from src.transform.ohlcv_transform import chunk_symbols

default_args = {
    "owner": "Anh",
    "depends_on_past": False,
}

_VAR_KEY = "market_data_pipeline_run_id"


def on_failure_callback(context):
    try:
        pipeline_run_id = Variable.get(_VAR_KEY, default_var=None)
    except Exception:
        pipeline_run_id = None

    exception = context.get("exception")
    run_id = pipeline_run_id or ""

    with get_market_data_service("market_data_pipeline", run_id) as service:
        service.pipeline_failed(
            pipeline_run_id=pipeline_run_id,
            run_date=context["logical_date"].date(),
            error=str(exception),
        )


@dag(
    "market_data_pipeline",
    schedule="30 6 * * 2-6",  # 5:30 AM UTC = 11:30 PM PST, weekdays only
    default_args=default_args,
    catchup=False,
    tags=["financial_data_lake", "ETL", "ohlcv"],
    description="Daily EOD OHLCV ingestion for Russell 3000 via Schwab API",
    on_failure_callback=on_failure_callback,
)
def market_data_pipeline():

    @task
    def log_pipeline_start() -> None:
        pipeline_run_id = str(uuid.uuid4())
        Variable.set(_VAR_KEY, pipeline_run_id)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            service.pipeline_start()

    @task
    def query_active_symbols() -> Dict[str, List]:
        pipeline_run_id = Variable.get(_VAR_KEY)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            return service.query_active_and_retry_symbols()

    @task
    def build_chunks(symbol_payload: Dict[str, List]) -> List[List[str]]:
        all_symbols = list(set(symbol_payload["symbols"] + symbol_payload["retries"]))
        return chunk_symbols(all_symbols)

    @task
    def fetch_ohlcv(chunk: List[str]) -> List[Dict]:
        pipeline_run_id = Variable.get(_VAR_KEY)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            return service.fetch_and_validate_chunk(chunk)

    @task
    def aggregate_results(chunks_results: List[List[Dict]]) -> List[Dict]:
        return [row for chunk in chunks_results for row in chunk]

    @task
    def statistical_validation(raw_records: Dict) -> Dict:
        pipeline_run_id = Variable.get(_VAR_KEY)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            return service.run_statistical_validation(raw_records)

    @task
    def write_to_delta_lake(validated_payload: Dict) -> int:
        pipeline_run_id = Variable.get(_VAR_KEY)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            return service.write_delta(validated_payload["clean"])

    @task
    def update_volatility(rows_written: int) -> None:
        if rows_written == 0:
            return
        pipeline_run_id = Variable.get(_VAR_KEY)
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            service.update_rolling_volatility()

    @task(trigger_rule="all_done")
    def log_pipeline_end(rows_written: int) -> None:
        pipeline_run_id = Variable.get(_VAR_KEY)
        if rows_written is None:
            rows_written = 0
        with get_market_data_service("market_data_pipeline", pipeline_run_id) as service:
            service.pipeline_end(rows_written)

    # ------------------------------------------------------------------ #
    # Task wiring                                                          #
    # ------------------------------------------------------------------ #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    start_task = log_pipeline_start()
    start >> start_task

    # Explicit dep: Variable must be set before query_active_symbols reads it
    active = query_active_symbols()
    start_task >> active

    chunks = build_chunks(active)
    ohlcv_results = fetch_ohlcv.expand(chunk=chunks)

    aggregated = aggregate_results(ohlcv_results)
    validated = statistical_validation(aggregated)
    rows_written = write_to_delta_lake(validated)

    volatility_task = update_volatility(rows_written)
    pipeline_end_task = log_pipeline_end(rows_written)
    volatility_task >> pipeline_end_task >> end


market_data_pipeline()
