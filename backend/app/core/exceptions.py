

class WisdomError(Exception):
    """Base class for all application-level errors."""


class LLMError(WisdomError):
    """Raised when the LLM provider returns an error or times out."""


class SQLExecutionError(WisdomError):
    """Raised when the SQL tool fails to execute a query against the DB."""

    def __init__(self, message: str, sql: str = "") -> None:
        super().__init__(message)
        self.sql = sql


class SessionNotFoundError(WisdomError):
    """Raised when a session_id does not exist in the SessionManager."""


class EmptyMessageError(WisdomError):
    """Raised when the user sends a blank or whitespace-only message."""


class EnhancementError(WisdomError):
    """
    Raised when the query enhancer fails.
    This is intentionally non-fatal — callers should fall back to the raw query.
    """


class DataIngestionError(WisdomError):
    """Raised when a file cannot be parsed or imported during data ingestion."""

    def __init__(self, message: str, filename: str = "") -> None:
        super().__init__(message)
        self.filename = filename