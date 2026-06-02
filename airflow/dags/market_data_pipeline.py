from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator

from typing import List, Dict, Any
import uuid

from src.config import AppConfig
from src.db_client.db_client import DBClient
from src.services.market_data_service import MarketDataService
from src.utilities.bootstrap import get_market_data_service
from src.transform.ohlcv_transform import (
    chunk_symbols,
    build_dataframe,
    deduplicate,
)

default_args = {
    "owner": "Anh",
    "depends_on_past": False,
}


def on_failure_callback(context):
    """
    Triggered by Airflow on any task failure.
    Updates pipeline_run to FAILED.
    Falls back to querying by dag_id + run_date if XCom is unavailable.
    """
    ti = context["task_instance"]
    pipeline_run_id = ti.xcom_pull(
        dag_id="market_data_pipeline",
        task_ids="log_pipeline_start",
        key="pipeline_run_id",
    )

    exception = context.get("exception")
    config = AppConfig()
    client = DBClient(config)

    with get_market_data_service(
        client, "market_data_pipeline", pipeline_run_id
    ) as service:
        service.pipeline_failed(
            pipeline_run_id=pipeline_run_id,
            run_date=context["logical_date"].date(),
            error=str(exception),
        )


@dag(
    "market_data_pipeline",
    schedule="0 3 * * 1-5",  # 11 PM PST = 3 AM UTC, weekdays only
    default_args=default_args,
    catchup=False,
    tags=["financial_data_lake", "ETL", "ohlcv"],
    description="Daily EOD OHLCV ingestion for Russell 3000 via Schwab API",
    on_failure_callback=on_failure_callback,
)
def market_data_pipeline():

    @task
    def log_pipeline_start() -> str:
        """
        Generates pipeline_run_id, inserts pipeline_run record with
        status=RUNNING. Returns pipeline_run_id for downstream tasks.
        """
        pipeline_run_id = str(uuid.uuid4())
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            service.pipeline_start()

        return pipeline_run_id

    @task
    def query_active_symbols(pipeline_run_id: str) -> Dict[str, List]:
        """
        Queries universe_membership for active symbols (exit_date IS NULL).
        Also queries failed_ingestion for retry candidates
        (retry_after <= now, attempts < max_attempts).
        Returns {"symbols": [...], "retries": [...]}
        """
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            return service.query_active_and_retry_symbols()

    @task
    def build_chunks(symbol_payload: Dict[str, List]) -> List[List[str]]:
        """
        Pure transform. Merges active symbols and retry symbols.
        Splits into chunks respecting Schwab rate limit.
        Returns list of symbol chunks.
        """
        all_symbols = symbol_payload["symbols"] + symbol_payload["retries"]
        return chunk_symbols(all_symbols)

    @task
    def fetch_ohlcv(chunk: List[str], pipeline_run_id: str) -> List[Dict]:
        """
        Dynamic task mapping — one task per chunk.
        For each symbol in chunk:
            - Check Redis for valid token (distributed lock, SET NX)
            - Call Schwab /pricehistory
            - Layer 1: HTTP status validation
            - Layer 2: Field presence validation
            - Layer 3: Internal consistency (High >= Open/Close/Low)
            - Layer 4: Date validation (returned date == today UTC)
        Logs failures to failed_ingestion.
        Returns list of valid raw OHLCV dicts.
        """
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            return service.fetch_and_validate_chunk(chunk)

    @task
    def aggregate_results(
        chunks_results: List[List[Dict]], pipeline_run_id: str
    ) -> Dict:
        """
        Collects results from all dynamic task chunks.
        Flattens into single list.
        Builds Pandas DataFrame.
        Deduplicates on (symbol, datetime).
        Logs duplicates if found.
        Returns serializable dict representation of clean DataFrame.
        """
        flat = [row for chunk in chunks_results for row in chunk]
        df = build_dataframe(flat)
        df = deduplicate(df)
        return df.to_dict(orient="records")

    @task
    def statistical_validation(
        raw_records: Dict, pipeline_run_id: str
    ) -> Dict:
        """
        Queries volatility_rolling from PostgreSQL for each symbol.
        For each row:
            - Log return vs previous close > 5 * rolling_sd → flag
            - Volume > 10 * rolling_avg_volume → flag
            - abs(open - prev_close) > 3 * rolling_sd → flag
        Logs flagged rows to failed_ingestion (VALIDATION).
        Returns {"clean": [...], "flagged_count": int}
        """
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            return service.run_statistical_validation(raw_records)

    @task
    def write_to_delta_lake(validated_payload: Dict, pipeline_run_id: str) -> int:
        """
        Converts clean records to PyArrow table.
        Writes to Delta Lake via deltalake Python library:
            mode: append
            partition: year
            path: s3://ohlcv/prices/
            storage_options: LocalStack endpoint
        Verifies write by reading Delta transaction log.
        Returns row count written.
        """
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            return service.write_delta(validated_payload["clean"])

    @task
    def update_volatility(rows_written: int, pipeline_run_id: str) -> None:
        """
        Queries last 20 trading days from Delta Lake for each active symbol.
        Computes log returns.
        Computes rolling standard deviation.
        Upserts into volatility_rolling (rolling_sd, updated_at).
        Skips if rows_written == 0.
        """
        if rows_written == 0:
            return

        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            service.update_rolling_volatility()

    @task(trigger_rule="all_done")
    def log_pipeline_end(rows_written: int, pipeline_run_id: str) -> None:
        """
        Determines final status:
            rows_written > 0, no failures → SUCCESS
            rows_written > 0, some failures → PARTIAL
            rows_written == 0 → FAILED
        Updates pipeline_run: status, completed_at, records_processed,
        records_failed.
        Clears pipeline_run_id XCom.
        """
        config = AppConfig()
        client = DBClient(config)

        with get_market_data_service(
            client, "market_data_pipeline", pipeline_run_id
        ) as service:
            service.pipeline_end(rows_written)

    # ------------------------------------------------------------------ #
    # Task wiring                                                          #
    # ------------------------------------------------------------------ #

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Startup
    pipeline_run_id = log_pipeline_start()
    start >> pipeline_run_id

    # Symbol retrieval
    symbol_payload = query_active_symbols(pipeline_run_id)
    chunks = build_chunks(symbol_payload)

    # Dynamic task mapping — one fetch_ohlcv task per chunk
    ohlcv_results = fetch_ohlcv.expand_kwargs(
        [{"chunk": c, "pipeline_run_id": pipeline_run_id} for c in chunks]
    )

    # Aggregation and validation
    aggregated = aggregate_results(ohlcv_results, pipeline_run_id)
    validated = statistical_validation(aggregated, pipeline_run_id)

    # Delta Lake write
    rows_written = write_to_delta_lake(validated, pipeline_run_id)

    # Post-write
    update_volatility(rows_written, pipeline_run_id)

    # Shutdown
    log_pipeline_end(rows_written, pipeline_run_id) >> end


market_data_pipeline()