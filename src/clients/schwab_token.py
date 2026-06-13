import json
import base64
import time
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis
import requests

from src.clients.db_client import DBClient
from src.clients.schwab_exception import (
    SchwabTokenError, SchwabAuthExpiredError
)


@dataclass(frozen=True)
class TokenPayload:
    """A short-lived access-token cache entry. Lives in Redis ONLY.

    The refresh token is deliberately NOT here — it has a different lifetime
    (7 days vs 30 min) and a different home (durable Postgres). Caching it
    under the access token's TTL is the exact bug this redesign removes.
    """
    access_token: str
    expires_at: int     # unix epoch seconds


class RedisTokenStore:
    """Caches the ~30-minute access token in Redis so 3000 fetch tasks don't
    each re-mint one. The refresh token is not stored here."""

    def __init__(self, redis_client: redis.Redis, key: str):
        self.r = redis_client
        self.key = key

    def get(self) -> Optional[TokenPayload]:
        raw = self.r.get(self.key)
        if not raw:
            return None
        data = json.loads(raw)
        return TokenPayload(
            access_token=data["access_token"],
            expires_at=int(data["expires_at"]),
        )

    def set(self, payload: TokenPayload) -> None:
        # TTL = remaining access-token life, so Redis self-clears at expiry.
        ttl = max(payload.expires_at - int(time.time()), 1)
        self.r.set(
            self.key,
            json.dumps({
                "access_token": payload.access_token,
                "expires_at": payload.expires_at,
            }),
            ex=ttl,
        )


class RedisErrorMarker:
    """Transient broadcast of a TERMINAL refresh failure.

    The lock winner writes it; the waiters read it so they fail fast and loud
    (re-auth required) instead of spinning to a meaningless lock-timeout. This
    is coordination only, scoped to a single refresh cycle, so the TTL is
    short — bounded BELOW by the waiters' wait window (or a waiter could miss
    it) and ABOVE by the inter-run gap (or a stale marker poisons the next run
    after a human re-auths). The DURABLE 'needs re-auth' record is the pipeline
    failure / DLQ, never this key.
    """

    def __init__(self, redis_client: redis.Redis, key: str, ttl_seconds: int = 45):
        self.r = redis_client
        self.key = key
        self.ttl = ttl_seconds

    def set(self, reason: str) -> None:
        self.r.set(self.key, reason, ex=self.ttl)

    def get(self) -> Optional[str]:
        raw = self.r.get(self.key)
        if raw is None:
            return None
        return raw.decode() if isinstance(raw, bytes) else raw


class RedisLock:
    def __init__(self, redis_client: redis.Redis, lock_key: str, ttl_seconds: int = 30):
        self.r = redis_client
        self.lock_key = lock_key
        self.ttl = ttl_seconds
        self._token = None

    def acquire(self) -> bool:
        token = secrets.token_urlsafe(16)
        ok = self.r.set(self.lock_key, token, nx=True, ex=self.ttl)
        if ok:
            self._token = token
            return True
        return False

    def release(self) -> None:
        if not self._token:
            return
        # Only delete the lock if WE still own it (compare-and-delete), so a
        # slow holder whose TTL already expired can't delete someone else's lock.
        lua = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
            else
            return 0
            end
        """
        self.r.eval(lua, 1, self.lock_key, self._token)
        self._token = None


class SchwabTokenProvider:
    """Hands out a usable Schwab access token.

    Read path: Redis cache → on miss, single-flight refresh under a SET NX
    lock. The lock winner reads the refresh token + issued_at from Postgres
    (borrowed, READ-ONLY DBClient), checks the 7-day wall, calls Schwab, and
    caches the new access token in Redis. Losers wait, polling Redis for either
    the fresh token (success) or an error marker (terminal failure).
    """

    def __init__(
        self,
        store: RedisTokenStore,
        lock: RedisLock,
        error_marker: RedisErrorMarker,
        db: DBClient,
        *,
        client_id: str,
        client_secret: str,
        base_url: str,
        provider_name: str = "schwab",
        refresh_ttl_days: int = 7,
        skew_seconds: int = 60,
        timeout_seconds: int = 15,
        wait_seconds: float = 3.0,
    ):
        self.store = store
        self.lock = lock
        self.error_marker = error_marker
        self.db = db                      # borrowed from the service; read-only
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.provider_name = provider_name
        self.refresh_ttl = timedelta(days=refresh_ttl_days)
        self.skew = skew_seconds
        self.timeout = timeout_seconds
        self.wait_seconds = wait_seconds

    def get_access_token(self) -> str:
        cached = self.store.get()
        if cached and int(time.time()) < (cached.expires_at - self.skew):
            return cached.access_token

        # Token unusable. Exactly one worker refreshes; the rest wait.
        if not self.lock.acquire():
            return self._wait_for_token()

        try:
            # Double-check: between our cache miss and winning the lock, a
            # previous holder may have just refreshed (serial lock hand-off).
            # Without this we'd fire a second, redundant refresh.
            cached = self.store.get()
            if cached and int(time.time()) < (cached.expires_at - self.skew):
                return cached.access_token

            payload = self._refresh()
            self.store.set(payload)
            return payload.access_token
        except SchwabAuthExpiredError as e:
            # Terminal: broadcast so the waiters fail fast instead of timing
            # out with a misleading generic error.
            self.error_marker.set(str(e))
            raise
        finally:
            self.lock.release()

    def _wait_for_token(self) -> str:
        deadline = time.time() + self.wait_seconds
        while time.time() < deadline:
            time.sleep(0.25)

            cached = self.store.get()
            if cached and int(time.time()) < (cached.expires_at - self.skew):
                return cached.access_token

            marker = self.error_marker.get()
            if marker:
                raise SchwabAuthExpiredError(
                    f"Refresh failed by lock holder: {marker}"
                )

        # No token and no marker — the holder is just slow, or died without
        # broadcasting. Transient; let the run retry.
        raise SchwabTokenError("Timed out waiting for token refresh")

    def _read_seed(self):
        """Read the durable refresh token + issued_at from Postgres through the
        borrowed connection. READ ONLY — never writes (the only writer is the
        human re-auth path)."""
        rows = self.db._select(
            "SELECT refresh_token, issued_at FROM schwab_token WHERE provider = %s",
            (self.provider_name,),
        )
        if not rows:
            raise SchwabAuthExpiredError(
                f"No refresh token provisioned for provider '{self.provider_name}' "
                "— manual re-authentication required"
            )
        refresh_token, issued_at = rows[0]
        return refresh_token, issued_at

    def _refresh(self) -> TokenPayload:
        refresh_token, issued_at = self._read_seed()

        # Cheap wall pre-check — issued_at is TIMESTAMPTZ, so tz-aware. If the
        # token is already past its 7-day life, don't even bother calling Schwab.
        if datetime.now(timezone.utc) >= issued_at + self.refresh_ttl:
            raise SchwabAuthExpiredError(
                f"Refresh token past its {self.refresh_ttl.days}-day life "
                f"(issued {issued_at.isoformat()}) — re-authentication required"
            )

        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        try:
            response = requests.post(
                f"{self.base_url}/v1/oauth/token",
                headers={
                    "Authorization": f"Basic {credentials}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            # Network blip — transient, retry next run. No marker.
            raise SchwabTokenError(f"Token refresh request failed: {e}") from e

        if response.status_code == 200:
            data = response.json()
            return TokenPayload(
                access_token=data["access_token"],
                expires_at=int(time.time()) + int(data["expires_in"]),
            )

        # Non-200: classify terminal vs transient.
        error_code = ""
        try:
            error_code = response.json().get("error", "")
        except ValueError:
            pass

        if response.status_code in (400, 401) or error_code == "invalid_grant":
            # The credential itself is bad — refresh token revoked/expired, or
            # client_id/secret wrong. No retry fixes this.
            raise SchwabAuthExpiredError(
                f"Refresh rejected (HTTP {response.status_code}, error='{error_code}') "
                "— re-authentication required"
            )

        # 5xx / 429 / anything else — Schwab-side and transient. No marker.
        raise SchwabTokenError(
            f"Token refresh failed: HTTP {response.status_code} — {response.text[:200]}"
        )
