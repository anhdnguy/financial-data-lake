import json
import time
import secrets
from dataclasses import dataclass
from typing import Optional

import redis
import requests

@dataclass(frozen=True)
class TokenPayload:
    access_token: str
    refresh_token: str
    expires_in: int     # unix epoch seconds

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
            expires_in=int(data["expires_in"])
        )

    def set(self, payload: TokenPayload) -> None:
        # Store with TTL so Redis automatically clears it when expired.
        ttl = max(payload.expires_in - int(time.time()), 1)
        self.r.set(
            self.key,
            json.dumps({
                "access_token": payload.access_token,
                "refresh_token": payload.refresh_token
                "expires_in": payload.expires_in
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
        skew_seconds: int = 60,
        timeout_seconds: int = 15,
        wait_seconds: float = 3.0,
    ):
        self.store = store
        self.lock = lock
        self.client_id = client_id
        self.client_secret = client_secret
        self.skew = skew_seconds
        self.timeout = timeout_seconds
        self.wait_seconds = wait_seconds