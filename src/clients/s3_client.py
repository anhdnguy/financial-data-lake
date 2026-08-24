import logging
from datetime import date
from typing import Dict, List, Optional

import pandas as pd
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from src.config import AppConfig
from src.clients.s3_exception import S3WriteError, S3ReadError

logger = logging.getLogger(__name__)


class S3DeltaClient:
    """
    Owns the Delta-Lake-on-S3 boundary (LocalStack in dev, AWS S3 in prod).

    Mirrors db_client / schwab_client: the service calls write()/read_recent_closes()
    and never sees storage_options or delta-rs internals.
    """

    def __init__(self, config: AppConfig):
        self._uri = config.delta_table_uri
        # delta-rs (object_store) reads these env-style keys. AWS_ALLOW_HTTP is required
        # for LocalStack (http, not https); AWS_S3_ALLOW_UNSAFE_RENAME is acceptable
        # because the pipeline is a single writer (no concurrent commits to coordinate).
        self._storage_options = {
            "AWS_ENDPOINT_URL": config.s3_endpoint_url,
            "AWS_ACCESS_KEY_ID": config.aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": config.aws_secret_access_key,
            "AWS_REGION": config.aws_region,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def write(self, df: pd.DataFrame) -> int:
        """
        Upsert a typed OHLCV frame into the Delta table, keyed on (symbol, date),
        partitioned by year. Idempotent: re-running the same day overwrites the matching
        rows instead of appending duplicates, so a re-trigger / task retry after a partial
        failure cannot silently double-write a day into the price store.

        First write to a non-existent path bootstraps the table (MERGE needs an existing
        target). Returns the number of rows upserted (the input frame length).

        The source frame MUST be unique on (symbol, date) — delta-rs raises if multiple
        source rows match one target row. The pipeline guarantees this via one row per
        symbol per day (SchwabClient collapses the response to the latest candle).
        """
        if df.empty:
            return 0
        try:
            df = df.copy()
            # Partition layout is a storage concern, derived here rather than in the
            # pure transforms. Year partitioning keeps files large (no small-file problem).
            df["year"] = df["date"].dt.year.astype("int32")

            try:
                dt = DeltaTable(self._uri, storage_options=self._storage_options)
            except TableNotFoundError:
                # Bootstrap: no table yet, so MERGE has no target. The first write creates
                # it; subsequent writes upsert. Append (not overwrite) so a concurrent/empty
                # path is created cleanly with the year partitioning in place.
                write_deltalake(
                    self._uri,
                    df,
                    mode="append",
                    partition_by=["year"],
                    storage_options=self._storage_options,
                )
                return len(df)

            # Key on (symbol, date). `target.year = source.year` is logically redundant
            # (date determines year) but lets delta-rs prune to the touched year partitions
            # instead of scanning the whole table for the join.
            (
                dt.merge(
                    source=df,
                    predicate=(
                        "target.symbol = source.symbol "
                        "AND target.date = source.date "
                        "AND target.year = source.year"
                    ),
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
            return len(df)
        except Exception as e:
            raise S3WriteError(f"Delta write failed: {str(e)}") from e

    def read_recent_closes(
        self, symbols: List[str],
        lookback: int = 21,
        before: date | None = None
    ) -> pd.DataFrame:
        """
        Read recent (symbol, date, close) rows for the given symbols. Prunes to the
        current + previous year partitions, filter the subset of symbols, sort by symbol
        and date, and trim based on the lookback days.

        Raises S3ReadError if the table does not exist yet (first ever run).
        """
        if not symbols:
            return pd.DataFrame(columns=["symbol", "date", "close"])

        current_year = date.today().year
        years = [str(current_year), str(current_year - 1)]

        try:
            dt = DeltaTable(self._uri, storage_options=self._storage_options)
            df = dt.to_pandas(
                columns=["symbol", "date", "close", "year"],
                partitions=[("year", "in", years)],
            )
        except TableNotFoundError as e:
            raise S3ReadError(f"Delta table not found at {self._uri}") from e
        except Exception as e:
            raise S3ReadError(f"Delta read failed: {str(e)}") from e

        df = df[df["symbol"].isin(symbols)]
        df = df.sort_values(by=["symbol", "date"])
        if before:
            df = df[df["date"] < pd.Timestamp(before)]
        df = df.groupby("symbol").tail(lookback)
        return df[["symbol", "date", "close"]]

    def optimize(self, target_size: Optional[int] = None) -> Dict:
        """
        Compact small Parquet files into fewer large ones (bin-packing) to fix the
        small-file problem the daily appends create.

        OPTIMIZE rewrites bytes, NOT rows: every (symbol, date, close, ...) value is
        identical afterwards — the data is just repacked toward the target file size.
        The original small files are NOT deleted here; they become *tombstones* (files
        no longer referenced by the current table version) and stay on S3 until vacuum()
        removes them. So OPTIMIZE temporarily INCREASES storage (old small files + new
        big files coexist) until the next vacuum past the retention window.

        target_size: bytes per output file. None lets delta-rs use the table default
        (~256MB). Returns delta-rs compaction metrics (files added/removed, bytes, etc.).
        """
        try:
            dt = DeltaTable(self._uri, storage_options=self._storage_options)
            if target_size is not None:
                return dt.optimize.compact(target_size=target_size)
            return dt.optimize.compact()
        except TableNotFoundError as e:
            raise S3ReadError(f"Delta table not found at {self._uri}") from e
        except Exception as e:
            raise S3WriteError(f"Delta optimize failed: {str(e)}") from e

    def vacuum(self, retention_hours: int = 168, dry_run: bool = True) -> List[str]:
        """
        Physically delete tombstoned data files older than retention_hours — the files
        left behind by OPTIMIZE compactions and MERGE rewrites that the current version
        no longer references.

        VACUUM never deletes rows from the current table version. retention_hours is a
        time-travel / in-flight-reader safety window, NOT a business data-retention knob:
        the 20-day volatility lookback reads the *current* version, which vacuum cannot
        touch regardless of how it is tuned. There is therefore no need to size retention
        to the volatility window.

        168h (7 days) is delta-rs's safety floor. Going below it requires
        enforce_retention_duration=False and risks deleting files that a concurrent
        reader or a recent time-travel query still needs — we only relax the guard when
        the caller deliberately asks for a shorter window. dry_run=True (default) returns
        the files that WOULD be removed without deleting anything.
        """
        # Only relax delta-rs's guard when the caller intentionally goes below the floor.
        enforce = retention_hours >= 168
        try:
            dt = DeltaTable(self._uri, storage_options=self._storage_options)
            return dt.vacuum(
                retention_hours=retention_hours,
                dry_run=dry_run,
                enforce_retention_duration=enforce,
            )
        except TableNotFoundError as e:
            raise S3ReadError(f"Delta table not found at {self._uri}") from e
        except Exception as e:
            raise S3WriteError(f"Delta vacuum failed: {str(e)}") from e
