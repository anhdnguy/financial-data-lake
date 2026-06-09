import json
import base64
import time
import secrets
from dataclasses import dataclass
from typing import Optional

import redis
import requests

from src.clients.schwab_exception import (
    SchwabTokenError, SchwabHTTPError, SchwabValidationError
)

@dataclass(frozen=True)
class TokenPayload:
    access_token: str
    refresh_token: str
    expires_at: int     # unix epoch seconds

class RedisTokenStore:
    def __init__(self, redis_client: redis.Redis, key: str):
        self.r = redis_client
        self.key = key

    def get(self) -> Optional[TokenPayload]:
        raw = self.r.get(self.key)
        if not raw:
            return None
        data = json.loads(raw)
        return TokenPayload(
            access_token=data["access_token"]
            refresh_token=data["refresh_token"]
            expires_at=int(data["expires_at"])
        )

    def set(self, payload: TokenPayload) -> None:
        # Store with TTL so Redis automatically clears it when expired.
        ttl = max(payload.expires_in - int(time.time()), 1)
        self.r.set(
            self.key,
            json.dumps({
                "access_token": payload.access_token,
                "refresh_token": payload.refresh_token
                "expires_at": payload.expires_at
            }),
            ex=ttl,
        )

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
    def __init__(
        self,
        store: RedisTokenStore,
        lock: RedisLock,
        *,
        client_id: str,
        client_secret: str,
        base_url: str,
        skew_seconds: int = 60,
        timeout_seconds: int = 15,
        wait_seconds: float = 3.0,
    ):
        self.store = store
        self.lock = lock
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.skew = skew_seconds
        self.timeout = timeout_seconds
        self.wait_seconds = wait_seconds

    def get_access_token(self) -> str:
        now = int(time.time())

        cached = self.store.get()
        if cached and now < (cached.expires_at - self.skew):
            # Access token is found and is still usable
            return cached.access_token

        # Access token is unusable, attempt to aquire the lock
        if not self.lock.acquire():
            # Lock cannot be acquired, someone else might be refreshing the token
            deadline = time.time() + self.wait_seconds

            while time.time() < deadline:
                time.sleep(0.25)
                cached = self.store.get()
                if cached and now < (cached.expires_at - self.skew):
                    return cached.access_token
            
            raise SchwabTokenError("Failed to acquire token refresh lock after retries")

        # Lock is acquired and attempt to refresh the token
        try:
            # Double check after acquiring lock to avoid duplicate refresh
            cached = self.store.get()
            if cached and now < (cached.expires_at - self.skew):
                return cached.access_token

            payload = self._refresh(cached.refresh_token)
            self.store.set(payload)
            return payload.access_token
        finally:
            # Lock is released
            self.lock.release()

    def _refresh(self, refresh_token) - TokenPayload:
        # Attempt to refresh token
        credentials = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

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
            timeout=10
        )

        if response.status_code != 200:
            raise SchwabTokenError(
                f"Token refresh failed: HTTP {response.status_code} — {response.text[:200]}"
            )
        
        data = response.json()
        access_token = data["access_token"]
        refresh_token = data["refresh_token"]
        expires_in = int(data["expires_in"])
        expires_at = int(time.time()) + expires_in
        return TokenPayload(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_at=expires_at
        )