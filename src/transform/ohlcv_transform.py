import logging
from typing import List, Dict

import numpy as np
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


def compute_rolling_volatility(closes: pd.DataFrame, window: int = 20) -> Dict[str, float]:
    """
    Per-symbol rolling volatility = sample std (ddof=1) of the last `window` DAILY log
    returns. Daily units on purpose — run_statistical_validation compares a single-day
    log return to 5 * rolling_sd, so the unit must match (do NOT annualize).

    A `window`-day std needs `window + 1` closes (to form `window` returns). Symbols with
    insufficient history (e.g. recent IPOs) are SKIPPED — never emitted as 0.0, which
    would zero tomorrow's 5-sigma threshold and flag every tick.
    """
    result: Dict[str, float] = {}
    if closes.empty:
        return result

    for symbol, group in closes.groupby("symbol"):
        group = group.sort_values("date")
        if len(group) < window + 1:
            logger.info(
                "Skipping %s: %d closes < %d required for %d-day volatility",
                symbol, len(group), window + 1, window,
            )
            continue
        log_returns = np.log(group["close"].astype("float64")).diff().dropna()
        sd = log_returns.tail(window).std(ddof=1)
        if pd.isna(sd):
            continue
        result[symbol] = float(sd)

    return result
