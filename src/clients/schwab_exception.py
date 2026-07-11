class SchwabError(Exception):
    """Base exception for Schwab client errors."""

class SchwabTokenError(SchwabError):
    """Raised when token acquisition or refresh fails for a TRANSIENT reason
    (network blip, Schwab 5xx/429, lock wait timeout). Safe to retry."""

class SchwabAuthExpiredError(SchwabError):
    """Raised when the refresh token itself is dead — past its 7-day wall, or
    Schwab returns invalid_grant / 401, or no token has ever been provisioned.
    TERMINAL: no retry can fix it; a human must re-authenticate in the browser.
    Surfaces as a review_required signal."""

class SchwabRateLimitError(SchwabError):
    """Raised when a rate-limiter token cannot be acquired — Redis is down or
    the wait exceeded the acquire timeout. TRANSIENT infrastructure failure.
    The limiter fails CLOSED (no token, no call): an unthrottled retry herd is
    exactly what escalates Schwab's 429s into a 403 ban, so when the throttle
    is broken we stop calling, never call unmetered."""

class SchwabHTTPError(SchwabError):
    """Raised when the API returns a non-200 response."""

class SchwabValidationError(SchwabError):
    """Raised when OHLCV data fails structural validation (layers 1–3)."""
