import base64
import logging
import time
from datetime import date, datetime, timezone
from typing import Dict

import redis
import requests
from redis.exceptions import RedisError

from src.config import AppConfig
from src.clients.schwab_token import SchwabTokenProvider
from src.clients.schwab_exception import (
    SchwabTokenError, SchwabHTTPError, SchwabValidationError
)

logger = logging.getLogger(__name__)

_TOKEN_KEY = "schwab:access_token"
_LOCK_KEY = "schwab:token_refresh_lock"
_TOKEN_TTL = 1500   # 25 minutes — 5-minute buffer before 30-minute expiry
_LOCK_TTL = 30      # seconds — max time to hold the refresh lock
_MAX_LOCK_RETRIES = 5
_LOCK_RETRY_SLEEP = 0.5


class SchwabClient:
    def __init__(self, config: AppConfig, token_provider: SchwabTokenProvider):
        self.config = config
        self.token_provider = token_provider

    def _headers(self) -> Dict[str, str]:
        token = self.token_provider.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def fetch_ohlcv(self, symbol: str) -> Dict:
        headers = self._headers()
        today_utc = datetime.now(timezone.utc).date()

        response = requests.get(
            f"{self.config.schwab_base_url}/marketdata/v1/pricehistory",
            headers=headers,
            params={
                "symbol": symbol,
                "periodType": "day",
                "period": 1,
                "frequencyType": "daily",
                "frequency": 1,
                "needExtendedHoursData": "false",
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
        candle_date = datetime.fromtimestamp(
            candle["datetime"] / 1000, tz=timezone.utc
        ).date()
        if candle_date != today_utc:
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
