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

class SchwabHTTPError(SchwabError):
    """Raised when the API returns a non-200 response."""

class SchwabValidationError(SchwabError):
    """Raised when OHLCV data fails structural validation (layers 1–3)."""
