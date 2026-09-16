import logging
import math
import uuid
from datetime import date
from typing import List, Dict, Optional

from src.clients.db_client import DBClient
from src.clients.db_exception import DBError
from src.clients.schwab_client import SchwabClient
from src.clients.schwab_exception import (
    SchwabTokenError, SchwabAuthExpiredError, SchwabRateLimitError,
    SchwabHTTPError, SchwabValidationError,
)
from src.clients.s3_client import S3DeltaClient
from src.clients.s3_exception import S3Error, S3ReadError
from src.services.pipeline_exception import (
    PipelineDBError, PipelineAPIError, PipelineStorageError,
)
from src.transform.ohlcv_transform import (
    build_dataframe,
    compute_rolling_volatility,
    construct_dict_from_df
)
from src.transform.universe_transform import _get_today

logger = logging.getLogger(__name__)


class OHLCVService:
    def __init__(self, client: DBClient, schwab: SchwabClient, s3: S3DeltaClient,
                 dag_id: str, pipeline_run_id: str):
        self.client = client
        self.schwab = schwab
        self.s3 = s3
        self.pipeline_run_id = pipeline_run_id
        self.dag_id = dag_id

    def __enter__(self):
        self.client.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.client.rollback()
        else:
            self.client.commit()
        self.client.__exit__(exc_type, exc, tb)
        return False

    def pipeline_start(self) -> None:
        query = """
            INSERT INTO pipeline_run (id, dag_id, run_date, status)
            VALUES (%s, %s, %s, 'RUNNING')
        """
        try:
            self.client._insert(query, (self.pipeline_run_id, self.dag_id, _get_today()))
        except DBError as e:
            logger.error("pipeline_start failed: %s", e)
            raise PipelineDBError("Pipeline start insert failed") from e

    def query_active_and_retry_symbols(self) -> Dict[str, List]:
        query_active = """
            SELECT DISTINCT m.symbol
            FROM universe_membership um
            JOIN membership m ON um.membership_id = m.id
            WHERE um.exit_date IS NULL
        """
        query_retries = """
            SELECT DISTINCT m.symbol
            FROM failed_ingestion fi
            JOIN membership m ON fi.symbol_id = m.id
            WHERE fi.retry_after <= NOW()
              AND fi.review_required = FALSE
              AND fi.attempts < 3
        """
        try:
            active_rows = self.client._select(query_active)
            retry_rows = self.client._select(query_retries)
        except DBError as e:
            logger.error("query_active_and_retry_symbols failed: %s", e)
            raise PipelineDBError("Symbol query failed") from e

        return {
            "symbols": [row[0] for row in active_rows],
            "retries": [row[0] for row in retry_rows],
        }

    def run_statistical_validation(self, raw_records: List[Dict]) -> Dict:
        if not raw_records:
            return {"clean": [], "flagged_count": 0}

        symbols = list({r["symbol"] for r in raw_records})
        session_date = min(r["date"] for r in raw_records)
        try:
            vol_rows = self.client._select(
                """
                SELECT m.symbol, vr.rolling_sd
                FROM volatility_rolling vr
                JOIN membership m ON vr.symbol_id = m.id
                WHERE m.symbol = ANY(%s)
                """,
                (symbols,),
            )
        except DBError as e:
            logger.error("run_statistical_validation: volatility query failed: %s", e)
            raise PipelineDBError("Volatility rolling query failed") from e

        rolling_sd_map = {row[0]: row[1] for row in vol_rows}

        sorted_records = sorted(raw_records, key=lambda r: (r["symbol"], str(r["date"])))

        try:
            recent_closes = self.s3.read_recent_closes(symbols, lookback=1, before=session_date)
            prev_close_map: Dict[str, float] = construct_dict_from_df(recent_closes, "symbol", "close")
        except S3ReadError as e:
            # No price history yet (e.g. first ever run) — nothing to compute, not a failure.
            logger.warning("run_statistical_validation: no Delta history yet: %s", e)
            prev_close_map = {}

        clean = []
        flagged_count = 0

        for record in sorted_records:
            symbol = record["symbol"]
            close = float(record["close"])
            open_price = float(record["open"])
            rolling_sd = rolling_sd_map.get(symbol)
            prev_close = prev_close_map.get(symbol)

            flag_reasons = []

            if rolling_sd is not None and prev_close is not None and prev_close > 0 and open_price > 0:
                log_return = abs(math.log(close / prev_close))
                if log_return > 5 * rolling_sd:
                    flag_reasons.append(
                        f"log_return {log_return:.4f} > 5*sd {5 * rolling_sd:.4f}"
                    )

                open_gap = abs(math.log(open_price / prev_close))
                if open_gap > 3 * rolling_sd:
                    flag_reasons.append(
                        f"open_gap {open_gap:.4f} > 3*sd {3 * rolling_sd:.4f}"
                    )

                # TODO: add rolling_avg_volume to volatility_rolling for volume spike check

            if flag_reasons:
                flagged_count += 1
                error_msg = "; ".join(flag_reasons)
                try:
                    self.client._insert(
                        """
                        INSERT INTO failed_ingestion
                            (id, symbol_id, raw_symbol, failure, failure_error,
                             retry_after, attempts)
                        SELECT %s, m.id, %s, 'VALIDATION', %s,
                               NOW() + INTERVAL '1 hour', 1
                        FROM membership m WHERE m.symbol = %s
                        ON CONFLICT (symbol_id) DO UPDATE SET
                            failure = EXCLUDED.failure,
                            failure_error = EXCLUDED.failure_error,
                            retry_after = NOW() + INTERVAL '1 hour',
                            attempts = failed_ingestion.attempts + 1,
                            review_required = (failed_ingestion.attempts + 1) >= 3,
                            created_at = NOW()
                        """,
                        (str(uuid.uuid4()), symbol, error_msg, symbol),
                    )
                except DBError as e:
                    logger.error("Failed to log validation failure for %s: %s", symbol, e)
            else:
                clean.append(record)

        return {"clean": clean, "flagged_count": flagged_count}

    def pipeline_end(self, rows_written: int, attempted: Optional[int] = None) -> None:
        """
        Close out the run. `attempted` is how many symbols the run set out to fetch;
        None means that number was unavailable (log_pipeline_end runs under
        trigger_rule="all_done", so upstream may have handed it nothing).

        Status is derived from the run's OWN numbers, never from the DLQ count. A DLQ
        that silently refuses its writes reads as zero failures — which is how five
        consecutive runs reported SUCCESS while the write shrank from 3010 to 614.
        """
        try:
            rows = self.client._select(
                "SELECT started_at FROM pipeline_run WHERE id = %s",
                (self.pipeline_run_id,),
            )
            started_at = rows[0][0] if rows else None

            failure_count = 0
            if started_at:
                count_rows = self.client._select(
                    "SELECT COUNT(*) FROM failed_ingestion WHERE created_at >= %s",
                    (started_at,),
                )
                failure_count = count_rows[0][0] if count_rows else 0

            # The DLQ count is now a cross-check, not a status input.
            if attempted is not None:
                records_failed = max(attempted - rows_written, 0)
                if records_failed != failure_count:
                    logger.warning(
                        "pipeline_end: DLQ recorded %d failures but %d symbols are "
                        "missing from the write (attempted=%d, written=%d) — the DLQ "
                        "is not capturing every failure",
                        failure_count, records_failed, attempted, rows_written,
                    )
            else:
                logger.warning(
                    "pipeline_end: attempted count unavailable — falling back to the "
                    "DLQ count, which may under-report"
                )
                records_failed = failure_count

            if rows_written == 0:
                status = "FAILED"
            elif attempted is None:
                # No honest denominator; best effort on the DLQ count alone.
                status = "SUCCESS" if failure_count == 0 else "PARTIAL"
            elif rows_written < attempted or failure_count > 0:
                status = "PARTIAL"
            else:
                status = "SUCCESS"

            self.client._update(
                """
                UPDATE pipeline_run
                SET status = %s, completed_at = NOW(),
                    records_processed = %s, records_failed = %s
                WHERE id = %s
                """,
                (status, rows_written, records_failed, self.pipeline_run_id),
            )
        except DBError as e:
            logger.error("pipeline_end failed: %s", e)
            raise PipelineDBError("Pipeline end update failed") from e

    def pipeline_failed(
        self,
        pipeline_run_id: Optional[str],
        run_date: date,
        error: str,
    ) -> None:
        try:
            if pipeline_run_id is None:
                rows = self.client._select(
                    "SELECT id FROM pipeline_run WHERE dag_id = %s AND run_date = %s",
                    (self.dag_id, run_date),
                )
                if not rows:
                    logger.error(
                        "pipeline_failed: no run found for dag_id=%s run_date=%s",
                        self.dag_id,
                        run_date,
                    )
                    return
                pipeline_run_id = rows[0][0]

            self.client._update(
                """
                UPDATE pipeline_run
                SET status = 'FAILED', completed_at = NOW(), notes = %s
                WHERE id = %s
                """,
                (error, pipeline_run_id),
            )
        except DBError as e:
            logger.error("pipeline_failed update failed: %s", e)
            raise PipelineDBError("Pipeline failed update failed") from e

    def _log_ingestion_failure(self, symbol: str, failure_mode: str, error: str) -> None:
        try:
            self.client._insert(
                """
                INSERT INTO failed_ingestion
                    (id, symbol_id, raw_symbol, failure, failure_error, retry_after, attempts)
                SELECT %s, m.id, %s, %s, %s, NOW() + INTERVAL '1 hour', 1
                FROM membership m WHERE m.symbol = %s
                ON CONFLICT (symbol_id) DO UPDATE SET
                    failure = EXCLUDED.failure,
                    failure_error = EXCLUDED.failure_error,
                    retry_after = NOW() + INTERVAL '1 hour',
                    attempts = failed_ingestion.attempts + 1,
                    review_required = (failed_ingestion.attempts + 1) >= 3,
                    created_at = NOW()
                """,
                (str(uuid.uuid4()), symbol, failure_mode, error, symbol),
            )
        except DBError as e:
            logger.error("Failed to log ingestion failure for %s: %s", symbol, e)

    def fetch_and_validate_chunk(self, chunk: List[str]) -> List[Dict]:
        results = []
        for symbol in chunk:
            try:
                record = self.schwab.fetch_ohlcv(symbol)
                results.append(record)
            except SchwabAuthExpiredError as e:
                # Terminal: the refresh token is dead. No retry fixes this —
                # a human must re-authenticate. Abort loud; the message lands
                # in pipeline_run.notes via the on_failure_callback.
                logger.error("Schwab re-authentication required — aborting: %s", e)
                raise PipelineAPIError(
                    f"Schwab re-authentication required (review_required): {e}"
                ) from e
            except SchwabTokenError as e:
                logger.error("Token refresh failed — aborting chunk: %s", e)
                raise PipelineAPIError("Schwab token refresh failed") from e
            except SchwabRateLimitError as e:
                # The limiter itself is broken (Redis down / acquire timeout),
                # not this symbol. Fail CLOSED and abort the chunk — pressing on
                # unthrottled is what escalates 429s into a 403 ban.
                logger.error("Rate limiter unavailable — aborting chunk: %s", e)
                raise PipelineAPIError("Schwab rate limiter unavailable") from e
            except SchwabHTTPError as e:
                logger.warning("HTTP error for %s: %s", symbol, e)
                self._log_ingestion_failure(symbol, "HTTP_ERROR", str(e))
            except SchwabValidationError as e:
                logger.warning("Validation failed for %s: %s", symbol, e)
                self._log_ingestion_failure(symbol, "VALIDATION", str(e))
        return results

    def write_delta(self, records: List[Dict]) -> int:
        if not records:
            return 0
        try:
            df = build_dataframe(records)
            return self.s3.write(df)
        except S3Error as e:
            logger.error("write_delta failed: %s", e)
            raise PipelineStorageError("Delta Lake write failed") from e

    def update_rolling_volatility(self) -> None:
        symbols = self.query_active_and_retry_symbols()["symbols"]
        if not symbols:
            return

        rolling_window = 21
        try:
            closes = self.s3.read_recent_closes(symbols, lookback=rolling_window)
        except S3ReadError as e:
            # No price history yet (e.g. first ever run) — nothing to compute, not a failure.
            logger.warning("update_rolling_volatility: no Delta history yet: %s", e)
            return

        vol_map = compute_rolling_volatility(closes, rolling_window - 1)
        if not vol_map:
            logger.info("update_rolling_volatility: no symbols had sufficient history")
            return

        rows = [(symbol, sd) for symbol, sd in vol_map.items()]
        try:
            self.client._upsert(
                """
                INSERT INTO volatility_rolling (symbol_id, rolling_sd)
                SELECT m.id, v.sd::double precision
                FROM (VALUES %s) AS v(symbol, sd)
                JOIN membership m ON m.symbol = v.symbol
                ON CONFLICT (symbol_id) DO UPDATE SET rolling_sd = EXCLUDED.rolling_sd
                """,
                rows,
            )
        except DBError as e:
            logger.error("update_rolling_volatility upsert failed: %s", e)
            raise PipelineDBError("Volatility upsert failed") from e
