import logging
import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict

import requests

from src.config import AppConfig
from src.clients.schwab_token import SchwabTokenProvider
from src.clients.schwab_limiter import RedisTokenBucket
from src.clients.schwab_exception import (
    SchwabHTTPError, SchwabValidationError
)

logger = logging.getLogger(__name__)


class SchwabClient:
    # 1 initial attempt + 3 backoff retries per symbol. A symbol still throttled
    # after that goes to the DLQ (SchwabHTTPError) with retry_after NOW()+1h —
    # persistent 429s mean back off for real, not hammer harder.
    _MAX_ATTEMPTS = 4
    _BACKOFF_BASE_SECONDS = 2.0
    _BACKOFF_CAP_SECONDS = 60.0

    def __init__(self, config: AppConfig, token_provider: SchwabTokenProvider,
                 limiter: RedisTokenBucket):
        self.config = config
        self.token_provider = token_provider
        self.limiter = limiter

    def _headers(self) -> Dict[str, str]:
        token = self.token_provider.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    def _backoff_delay(self, attempt: int, retry_after_header: str) -> float:
        # Schwab knows exactly when the throttle lifts — its word beats our
        # guess. Only fall back to computed backoff when the header is absent
        # (or the HTTP-date form we don't parse).
        if retry_after_header:
            try:
                return max(float(retry_after_header), 0.0) + random.uniform(0.0, 1.0)
            except ValueError:
                pass
        delay = min(
            self._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)),
            self._BACKOFF_CAP_SECONDS,
        )
        # Jitter de-synchronizes the herd: without it, every worker throttled at
        # the same instant retries at the same instant — the same spike again.
        return random.uniform(delay / 2, delay)

    def _request_pricehistory(self, symbol: str) -> requests.Response:
        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            # Every attempt pays one token — a retry is still an API call, and
            # the bucket is what keeps ALL calls (retries included) on the drip.
            self.limiter.acquire()

            response = requests.get(
                f"{self.config.schwab_base_url}/marketdata/v1/pricehistory",
                headers=self._headers(),
                params={
                    "symbol": symbol,
                    "periodType": "year",
                    "frequencyType": "daily",
                    "period": 1,
                    "frequency": 1
                },
                timeout=10,
            )
            if response.status_code != 429:
                return response

            if attempt == self._MAX_ATTEMPTS:
                break

            delay = self._backoff_delay(attempt, response.headers.get("Retry-After"))
            logger.warning(
                "%s: HTTP 429 (attempt %d/%d) — backing off %.1fs",
                symbol, attempt, self._MAX_ATTEMPTS, delay,
            )
            time.sleep(delay)

        raise SchwabHTTPError(
            f"{symbol}: still throttled (HTTP 429) after {self._MAX_ATTEMPTS} attempts"
        )

    def fetch_ohlcv(self, symbol: str) -> Dict:
        yesterday_utc = datetime.now(timezone.utc).date() - timedelta(days = 1)

        response = self._request_pricehistory(symbol)

        # Layer 1 — HTTP status (429 already handled by the backoff loop)
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
        if candle_date != yesterday_utc:
            raise SchwabValidationError(
                f"{symbol}: candle date {candle_date} != yesterday {yesterday_utc}"
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
