import logging
from datetime import date
from typing import List

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
        Append a typed OHLCV frame to the Delta table, partitioned by year.
        First write to a non-existent path creates the table. Returns rows written.
        """
        if df.empty:
            return 0
        try:
            df = df.copy()
            # Partition layout is a storage concern, derived here rather than in the
            # pure transforms. Year partitioning keeps files large (no small-file problem).
            df["year"] = df["date"].dt.year.astype("int32")
            write_deltalake(
                self._uri,
                df,
                mode="append",
                partition_by=["year"],
                storage_options=self._storage_options,
            )
            return len(df)
        except Exception as e:
            raise S3WriteError(f"Delta write failed: {str(e)}") from e

    def read_recent_closes(self, symbols: List[str], lookback: int = 21) -> pd.DataFrame:
        """
        Read recent (symbol, date, close) rows for the given symbols. Prunes to the
        current + previous year partitions so the lookback window is covered across a
        year boundary; the caller trims to the exact window per symbol.

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
        return df[["symbol", "date", "close"]]
