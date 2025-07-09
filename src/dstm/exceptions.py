class Error(Exception):
    """Base type used by all exceptions in this library."""


class ConnectionError(Error):
    """Exception raised when connection fails."""


class PublishError(Error):
    """Exception raised when message publishing fails."""


class ConsumeError(Error):
    """Exception raised when message consumption fails."""


class WiringError(Error):
    """Incorrect wiring of task names <-> task implementations"""
