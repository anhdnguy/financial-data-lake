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
    construct_dict_from_df,
    deduplicate,
)
from src.transform.universe_transform import _get_today

logger = logging.getLogger(__name__)

# Layer-4 volume spike: today's volume against the 20-day average. Loose on purpose —
# this is a bad-tick filter, not a signal. Earnings and index rebalances routinely move
# 3-5x; 10x is the range where a data error is more likely than a real session.
_VOLUME_SPIKE_MULTIPLE = 10.0


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

    def query_active_symbols(self) -> List[str]:
        """
        Every active universe member — the full daily fetch list.

        There is deliberately no separate DLQ "retry" list. This pipeline fetches the
        WHOLE active universe every run, so a symbol that failed today is refetched
        tomorrow as a matter of course: today's run IS the retry. A retry query could
        only ever contribute symbols the active list does not already contain — which
        means symbols no longer in the universe, exactly the delisted tickers that were
        being resurrected and refetched forever.

        failed_ingestion therefore feeds nothing back into the fetch list. It is a
        monitoring surface: which symbols are broken right now (rows are deleted once a
        symbol reaches the price store) and how many runs they have been failing.
        A retry list would only earn its keep if the daily fetch were ever narrowed to
        a subset of the universe.
        """
        query_active = """
            SELECT DISTINCT m.symbol
            FROM universe_membership um
            JOIN membership m ON um.membership_id = m.id
            WHERE um.exit_date IS NULL
        """
        try:
            active_rows = self.client._select(query_active)
        except DBError as e:
            logger.error("query_active_symbols failed: %s", e)
            raise PipelineDBError("Symbol query failed") from e

        return [row[0] for row in active_rows]

    def run_statistical_validation(self, raw_records: List[Dict]) -> Dict:
        if not raw_records:
            return {"clean": [], "flagged_count": 0, "unvalidated_count": 0}

        symbols = list({r["symbol"] for r in raw_records})
        session_date = min(r["date"] for r in raw_records)
        try:
            vol_rows = self.client._select(
                """
                SELECT m.symbol, vr.rolling_sd, vr.rolling_avg_volume
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
        rolling_volume_map = {row[0]: row[2] for row in vol_rows}

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
        unvalidated_count = 0

        for record in sorted_records:
            symbol = record["symbol"]
            close = float(record["close"])
            open_price = float(record["open"])
            rolling_sd = rolling_sd_map.get(symbol)
            rolling_avg_volume = rolling_volume_map.get(symbol)
            prev_close = prev_close_map.get(symbol)

            flag_reasons = []
            checks_run = 0

            if rolling_sd is not None and prev_close is not None and prev_close > 0 and open_price > 0:
                checks_run += 1
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

            # Volume spike is independent of prev_close: it needs only today's volume
            # and the rolling baseline, so it still runs for a symbol with no usable
            # previous close. The multiple is deliberately loose — a 10x day is a
            # suspected bad tick, whereas 2-3x is an ordinary earnings session, and an
            # over-tight threshold here is exactly how the open-gap bug silently
            # rejected 80% of the universe.
            if rolling_avg_volume is not None and rolling_avg_volume > 0:
                checks_run += 1
                volume = float(record["volume"])
                if volume > _VOLUME_SPIKE_MULTIPLE * rolling_avg_volume:
                    flag_reasons.append(
                        f"volume {volume:.0f} > {_VOLUME_SPIKE_MULTIPLE:.0f}x "
                        f"avg {rolling_avg_volume:.0f}"
                    )

            if checks_run == 0:
                # No baseline yet (new listing, or first run for this symbol). The record
                # passes, but it passed UNCHECKED — counted separately so a run cannot
                # report a clean bill of health it never actually earned.
                unvalidated_count += 1

            if flag_reasons:
                flagged_count += 1
                error_msg = "; ".join(flag_reasons)
                try:
                    # Savepoint: one symbol's failed DLQ write must not abort the
                    # transaction and take every other flagged symbol down with it.
                    with self.client.savepoint("dlq_validation"):
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

        if unvalidated_count:
            logger.warning(
                "run_statistical_validation: %d of %d records passed with NO statistical "
                "check (missing rolling baseline or previous close)",
                unvalidated_count, len(sorted_records),
            )

        return {
            "clean": clean,
            "flagged_count": flagged_count,
            "unvalidated_count": unvalidated_count,
        }

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
            # Savepoint: this runs per-symbol inside a chunk loop. Without it, one bad
            # DLQ write aborts the transaction and every remaining symbol in the chunk
            # fails too — the failure mode that hid thousands of losses behind a
            # swallowed exception.
            with self.client.savepoint("dlq_ingestion"):
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

    def _clear_resolved_failures(self, symbols: List[str]) -> None:
        """
        Drop DLQ rows for symbols that just landed in the price store.

        failed_ingestion is current-state ("what needs retrying now"), not history, so a
        resolved failure is deleted outright rather than marked. Without this, `attempts`
        only ever climbs: three unrelated transient failures months apart would latch
        review_required and drop a healthy symbol from the retry list permanently.
        """
        if not symbols:
            return
        try:
            self.client._update(
                """
                DELETE FROM failed_ingestion
                WHERE symbol_id IN (SELECT id FROM membership WHERE symbol = ANY(%s))
                """,
                (symbols,),
            )
        except DBError as e:
            logger.error("_clear_resolved_failures failed: %s", e)
            raise PipelineDBError("Clearing resolved DLQ rows failed") from e

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
            # s3.write MERGEs on (symbol, date) and delta-rs raises if two source rows
            # match one target row. That uniqueness was previously incidental — it held
            # only because build_chunks happens to de-dupe its symbol list. Enforce it
            # here instead of relying on an upstream accident.
            df = deduplicate(df)
            rows_written = self.s3.write(df)
        except S3Error as e:
            logger.error("write_delta failed: %s", e)
            raise PipelineStorageError("Delta Lake write failed") from e

        # Only now are these symbols genuinely resolved: the data is in the price store.
        # Clearing at fetch time would be wrong — a record can pass fetch and still be
        # flagged by Layer 4 afterwards. Safe to retry if this raises: s3.write is an
        # idempotent MERGE, so a re-run rewrites the same rows.
        self._clear_resolved_failures(df["symbol"].tolist())
        return rows_written

    def update_rolling_volatility(self) -> None:
        symbols = self.query_active_symbols()
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

        rows = [
            (symbol, stats["sd"], stats["avg_volume"])
            for symbol, stats in vol_map.items()
        ]
        try:
            self.client._upsert(
                """
                INSERT INTO volatility_rolling (symbol_id, rolling_sd, rolling_avg_volume)
                SELECT m.id, v.sd::double precision, v.avg_volume::double precision
                FROM (VALUES %s) AS v(symbol, sd, avg_volume)
                JOIN membership m ON m.symbol = v.symbol
                ON CONFLICT (symbol_id) DO UPDATE SET
                    rolling_sd = EXCLUDED.rolling_sd,
                    rolling_avg_volume = EXCLUDED.rolling_avg_volume
                """,
                rows,
            )
        except DBError as e:
            logger.error("update_rolling_volatility upsert failed: %s", e)
            raise PipelineDBError("Volatility upsert failed") from e
