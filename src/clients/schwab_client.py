import logging
from datetime import datetime, timezone
from typing import Dict

import requests

from src.config import AppConfig
from src.clients.schwab_token import SchwabTokenProvider
from src.clients.schwab_exception import (
    SchwabHTTPError, SchwabValidationError
)

logger = logging.getLogger(__name__)


class SchwabClient:
    def __init__(self, config: AppConfig, token_provider: SchwabTokenProvider):
        self.config = config
        self.token_provider = token_provider

    def _headers(self) -> Dict[str, str]:
        token = self.token_provider.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def fetch_ohlcv(self, symbol: str) -> Dict:
        headers = self._headers()
        today_timestamp_utc = datetime.now(timezone.utc).timestamp()

        response = requests.get(
            f"{self.config.schwab_base_url}/pricehistory",
            headers=headers,
            params={
                "symbol": symbol,
                "periodType": "year",
                "frequencyType": "daily",
                "startDate": today_timestamp_utc
            },
            timeout=10,
        )

        # Layer 1 — HTTP status
        if response.status_code != 200:
            raise SchwabHTTPError(
                f"{symbol}: HTTP {response.status_code} — {response.text[:200]}"
            )

        data = response.json()
        if data.get("empty") or not data.get("candles"):
            raise SchwabHTTPError(f"{symbol}: empty response from Schwab")

        candle = data["candles"][-1]

        # Layer 1 — Date matches today UTC
        candle_timestamp = candle["datetime"]
        if candle_timestamp != today_timestamp_utc:
            candle_date = datetime.fromtimestamp(
                candle_timestamp / 1000, tz=timezone.utc
            ).date()
            today_utc = datetime.fromtimestamp(
                today_timestamp_utc / 1000, tz=timezone.utc
            ).date()
            raise SchwabValidationError(
                f"{symbol}: candle date {candle_date} != today {today_utc}"
            )

        # Layer 2 — All fields present and positive
        for field in ("open", "high", "low", "close", "volume"):
            value = candle.get(field)
            if value is None or value <= 0:
                raise SchwabValidationError(
                    f"{symbol}: field '{field}' missing or non-positive (got {value})"
                )

        open_ = float(candle["open"])
        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle["close"])
        volume = int(candle["volume"])

        # Layer 3 — Internal consistency
        if not (high >= open_ and high >= close and high >= low):
            raise SchwabValidationError(
                f"{symbol}: high {high} does not dominate open={open_} close={close} low={low}"
            )
        if not (low <= open_ and low <= close):
            raise SchwabValidationError(
                f"{symbol}: low {low} does not undercut open={open_} close={close}"
            )

        return {
            "symbol": symbol,
            "date": candle_date.isoformat(),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
