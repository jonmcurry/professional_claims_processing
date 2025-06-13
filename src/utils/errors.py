class DatabaseError(Exception):
    """Base class for database related errors."""

class DatabaseConnectionError(DatabaseError):
    """Raised when a database connection cannot be established."""

class QueryError(DatabaseError):
    """Raised when a query execution fails."""

