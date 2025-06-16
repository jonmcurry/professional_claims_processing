class DatabaseError(Exception):
    """Base class for database related errors."""


class DatabaseConnectionError(DatabaseError):
    """Raised when a database connection cannot be established."""


class QueryError(DatabaseError):
    """Raised when a query execution fails."""


class CircuitBreakerOpenError(DatabaseError):
    """Raised when the circuit breaker is open and operations are blocked."""


from enum import Enum


class ErrorCategory(str, Enum):
    VALIDATION = "validation"
    DATABASE = "database"
    RULE = "rule"
    PROCESSING = "processing"
    UNKNOWN = "unknown"


def categorize_exception(exc: Exception) -> ErrorCategory:
    if isinstance(exc, DatabaseError):
        return ErrorCategory.DATABASE
    return ErrorCategory.UNKNOWN
