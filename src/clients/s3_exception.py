class S3Error(Exception):
    """Base exception for S3 / Delta Lake client errors."""
    pass

class S3WriteError(S3Error):
    """Raised when a Delta Lake write fails."""
    pass

class S3ReadError(S3Error):
    """Raised when a Delta Lake read fails (incl. table not found)."""
    pass
