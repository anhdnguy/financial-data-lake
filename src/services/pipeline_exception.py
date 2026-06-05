class PipelineError(Exception):
    """Base exception for all pipeline service errors."""
    pass

class PipelineIOError(PipelineError):
    """Raised when file I/O fails (missing CSV, bad format)."""
    pass

class PipelineDBError(PipelineError):
    """Raised when a DB operation fails at the service boundary."""
    pass

class PipelineAPIError(PipelineError):
    """Raised when an external API call fails unrecoverably (e.g., token refresh)."""
    pass

class PipelineStorageError(PipelineError):
    """Raised when a Delta Lake / object-storage operation fails at the service boundary."""
    pass
