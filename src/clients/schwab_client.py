import base64
import logging
import time
from datetime import date, datetime, timezone
from typing import Dict

import redis
import requests
from redis.exceptions import RedisError

from src.config import AppConfig
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
    def __init__(self, config: AppConfig):
        self.config = config
        self._redis = redis.Redis(
            host=config.redis_host, port=config.redis_port, db=0
        )

    def _get_token(self) -> str:
        try:
            token = self._redis.get(_TOKEN_KEY)
        except RedisError as e:
            raise SchwabTokenError(f"Redis unavailable on token read: {str(e)}") from e
        if token:
            return token.decode()
        return self._refresh_token()

    def _refresh_token(self) -> str:
        try:
            for _ in range(_MAX_LOCK_RETRIES):
                lock_acquired = self._redis.set(_LOCK_KEY, "1", nx=True, ex=_LOCK_TTL)
                if lock_acquired:
                    try:
                        token = self._do_refresh()
                        self._redis.setex(_TOKEN_KEY, _TOKEN_TTL, token)
                        return token
                    finally:
                        # Best-effort release; the 30s TTL guarantees eventual cleanup,
                        # so a release failure must not mask the real error.
                        try:
                            self._redis.delete(_LOCK_KEY)
                        except RedisError:
                            logger.warning("Failed to release token refresh lock")

                # Another worker holds the lock — wait then check if token appeared
                time.sleep(_LOCK_RETRY_SLEEP)
                token = self._redis.get(_TOKEN_KEY)
                if token:
                    return token.decode()
        except RedisError as e:
            raise SchwabTokenError(f"Redis unavailable on token refresh: {str(e)}") from e

        raise SchwabTokenError(
            "Failed to acquire token refresh lock after retries"
        )

    def _do_refresh(self) -> str:
        credentials = base64.b64encode(
            f"{self.config.schwab_app_key}:{self.config.schwab_app_secret}".encode()
        ).decode()

        response = requests.post(
            f"{self.config.schwab_base_url}/v1/oauth/token",
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.config.schwab_refresh_token,
            },
            timeout=10,
        )

        if response.status_code != 200:
            raise SchwabTokenError(
                f"Token refresh failed: HTTP {response.status_code} — {response.text[:200]}"
            )

        token = response.json().get("access_token")
        if not token:
            raise SchwabTokenError("Token refresh response missing access_token")

        return token

    def fetch_ohlcv(self, symbol: str) -> Dict:
        token = self._get_token()
        today_utc = datetime.now(timezone.utc).date()

        response = requests.get(
            f"{self.config.schwab_base_url}/marketdata/v1/pricehistory",
            headers={"Authorization": f"Bearer {token}"},
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
