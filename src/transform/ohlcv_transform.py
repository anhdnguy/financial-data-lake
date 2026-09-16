import logging
from datetime import date, timedelta
from typing import List, Dict

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)


def chunk_symbols(symbols: List[str], chunk_size: int = 100) -> List[List[str]]:
    return [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]


def last_trading_date(as_of: date) -> date:
    """
    Most recent NYSE trading session strictly before `as_of`. "Yesterday" isn't
    always a trading day (weekends, holidays), but Schwab's /pricehistory last
    candle always is — this is what schwab_client's Layer-1 date check compares
    the candle date against, instead of a naive as_of - 1 day.
    """
    nyse = mcal.get_calendar("NYSE")
    sessions = nyse.valid_days(start_date=as_of - timedelta(days=10), end_date=as_of)
    prior_sessions = [d.date() for d in sessions if d.date() < as_of]
    return max(prior_sessions)


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


def construct_dict_from_df(df: pd.DataFrame, key: str, value: str) -> Dict[str, float]:
    df_to_dict = df.to_dict("records")
    result = {item[key] : item[value] for item in df_to_dict}
    return result


def compute_rolling_volatility(closes: pd.DataFrame, window: int = 20) -> Dict[str, Dict[str, float]]:
    """
    Per-symbol rolling stats over the last `window` sessions, returned as
    {symbol: {"sd": float, "avg_volume": float}}.

    `sd` is the sample std (ddof=1) of the last `window` DAILY log returns. Daily units
    on purpose — run_statistical_validation compares a single-day log return to
    5 * rolling_sd, so the unit must match (do NOT annualize).

    `avg_volume` is the mean of the last `window` volumes, the baseline the Layer-4
    volume-spike check measures today's volume against.

    A `window`-day std needs `window + 1` closes (to form `window` returns). Symbols with
    insufficient history (e.g. recent IPOs) are SKIPPED — never emitted as 0.0, which
    would zero tomorrow's 5-sigma threshold and flag every tick.

    Requires a `volume` column: a symbol is written to volatility_rolling as one row
    carrying both stats, so computing one without the other would leave a half-populated
    row that the volume check then cannot use.
    """
    result: Dict[str, Dict[str, float]] = {}
    if closes.empty:
        return result

    if "volume" not in closes.columns:
        raise ValueError(
            "compute_rolling_volatility requires a 'volume' column "
            f"(got {list(closes.columns)})"
        )

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
        avg_volume = group["volume"].astype("float64").tail(window).mean()
        if pd.isna(sd) or pd.isna(avg_volume):
            logger.info(
                "Skipping %s: non-finite stats (sd=%s, avg_volume=%s)",
                symbol, sd, avg_volume,
            )
            continue
        result[symbol] = {"sd": float(sd), "avg_volume": float(avg_volume)}

    return result
