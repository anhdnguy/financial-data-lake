import random
import time

import redis

from src.clients.schwab_exception import SchwabRateLimitError


# Atomic token bucket. The whole read → refill → check → spend sequence runs as
# ONE indivisible server-side step (same reason RedisLock.release is Lua): two
# workers polling the same near-empty bucket can never both spend the last token.
#
# The clock is Redis's own (TIME), never the caller's: 16 workers on 16 machines
# carry 16 skewed clocks, and a fast clock stamping shared state would mint
# phantom tokens. The bucket state and the time it is measured against must live
# in the same place.
#
# Two independent dials, per the design discussion:
#   - refill period (ARGV[2]) controls the long-run AVERAGE rate;
#   - capacity     (ARGV[1]) controls the maximum BURST (how many tokens may sit
#     banked and be spent at once). Schwab throttles on the spike, not the
#     minute-long average, so for this EOD batch capacity stays at 1: perfectly
#     smooth, one call per drip, a burst is impossible by construction.
_TOKEN_BUCKET_LUA = """
    -- KEYS[1]  bucket state hash: tokens, last_refill_us
    -- ARGV[1]  capacity (max banked tokens = max burst)
    -- ARGV[2]  refill period in microseconds (one token drips in per period)
    -- ARGV[3]  state TTL seconds (idle bucket self-cleans; a rebuilt bucket
    --          starts full, which is what an idle-past-TTL bucket would be)
    -- Returns  {1, 0} when a token was granted,
    --          {0, wait_us} when empty — wait_us until the next token drips.

    redis.replicate_commands()

    local capacity  = tonumber(ARGV[1])
    local period_us = tonumber(ARGV[2])
    local ttl_s     = tonumber(ARGV[3])

    local t = redis.call('TIME')
    local now_us = t[1] * 1000000 + t[2]

    local state   = redis.call('HMGET', KEYS[1], 'tokens', 'last_refill_us')
    local tokens  = tonumber(state[1])
    local last_us = tonumber(state[2])

    if tokens == nil or last_us == nil then
        tokens = capacity
        last_us = now_us
    else
        local minted = math.floor((now_us - last_us) / period_us)
        if minted > 0 then
            if tokens + minted >= capacity then
                -- Bucket full: surplus drip overflows (an idle period can never
                -- bank more than capacity). Re-anchor the faucet to now.
                tokens = capacity
                last_us = now_us
            else
                tokens = tokens + minted
                -- Advance the anchor by exactly the minted amount so fractional
                -- progress toward the next token is not thrown away.
                last_us = last_us + minted * period_us
            end
        end
    end

    if tokens >= 1 then
        redis.call('HSET', KEYS[1], 'tokens', tokens - 1, 'last_refill_us', last_us)
        redis.call('EXPIRE', KEYS[1], ttl_s)
        return {1, 0}
    end

    redis.call('HSET', KEYS[1], 'tokens', tokens, 'last_refill_us', last_us)
    redis.call('EXPIRE', KEYS[1], ttl_s)
    return {0, (last_us + period_us) - now_us}
"""


class RedisTokenBucket:
    """Distributed token-bucket rate limiter for the Schwab API boundary.

    The object holds NO state — every fetch task news one up (its own process,
    per Airflow), but they all point at the same Redis key, so N workers drain
    ONE shared bucket. Same pattern as RedisTokenStore/RedisLock: fresh plumbing
    per task, one truth in Redis.

    acquire() blocks until a token is granted: each miss returns how long until
    the next token drips, and the caller sleeps exactly that long plus a little
    random jitter so blocked workers don't wake in lockstep and stampede the
    single new token together.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        key: str,
        *,
        capacity: int = 1,
        refill_period_ms: float = 550.0,
        acquire_timeout_seconds: float = 120.0,
        jitter_ms: float = 50.0,
        state_ttl_seconds: int = 3600,
    ):
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if refill_period_ms <= 0:
            raise ValueError(f"refill_period_ms must be > 0, got {refill_period_ms}")
        self.r = redis_client
        self.key = key
        self.capacity = capacity
        self.refill_period_us = int(refill_period_ms * 1000)
        self.acquire_timeout = acquire_timeout_seconds
        self.jitter_seconds = jitter_ms / 1000.0
        self.state_ttl = state_ttl_seconds
        # Registered once: subsequent calls go out as EVALSHA, not the full script.
        self._take = redis_client.register_script(_TOKEN_BUCKET_LUA)

    def acquire(self) -> None:
        """Block until a token is granted. Raises SchwabRateLimitError if Redis
        is unreachable or the wait exceeds acquire_timeout (fail CLOSED — see
        the exception's docstring)."""
        # monotonic() is only our LOCAL patience budget; the shared truth
        # (tokens, elapsed time) is measured on the Redis server clock.
        deadline = time.monotonic() + self.acquire_timeout
        while True:
            try:
                granted, wait_us = self._take(
                    keys=[self.key],
                    args=[self.capacity, self.refill_period_us, self.state_ttl],
                )
            except redis.RedisError as e:
                raise SchwabRateLimitError(
                    f"Rate limiter Redis unavailable: {e}"
                ) from e

            if granted == 1:
                return

            wait = wait_us / 1_000_000 + random.uniform(0, self.jitter_seconds)
            if time.monotonic() + wait > deadline:
                raise SchwabRateLimitError(
                    f"No rate-limit token within {self.acquire_timeout:.0f}s "
                    f"(capacity={self.capacity}, period={self.refill_period_us}us)"
                )
            time.sleep(wait)
