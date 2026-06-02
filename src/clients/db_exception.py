class DBError(Exception):
    """Base exception for all DB client errors."""
    pass

class DBConnectionError(DBError):
    """Raised when the database connection cannot be established."""
    pass

class DBQueryError(DBError):
    """Raised when a SQL query fails during execution."""
    pass