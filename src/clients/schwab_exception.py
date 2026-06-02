class SchwabError(Exception):
    """Base exception for Schwab client errors."""

class SchwabTokenError(SchwabError):
    """Raised when token acquisition or refresh fails."""

class SchwabHTTPError(SchwabError):
    """Raised when the API returns a non-200 response."""

class SchwabValidationError(SchwabError):
    """Raised when OHLCV data fails structural validation (layers 1–3)."""
