import logging
from typing import List, Dict

import pandas as pd

logger = logging.getLogger(__name__)


def chunk_symbols(symbols: List[str], chunk_size: int = 100) -> List[List[str]]:
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]


def build_dataframe(records: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["date"] = pd.to_datetime(df["date"])
    for col in ("open", "high", "low", "close"):
        df[col] = df[col].astype("float64")
    df["volume"] = df["volume"].astype("int64")
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")
    dropped = before - len(df)
    if dropped > 0:
        logger.warning("Dropped %d duplicate OHLCV rows on (symbol, date)", dropped)
    return df
