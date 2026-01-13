"""Custom exceptions for the platform."""


class PlatformException(Exception):
    """Base exception for all platform errors."""
    
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class FormatDetectionError(PlatformException):
    """Raised when table format cannot be detected."""
    pass


class MetadataReadError(PlatformException):
    """Raised when metadata cannot be read from a table."""
    pass


class NormalizationError(PlatformException):
    """Raised when metadata normalization fails."""
    pass


class StorageError(PlatformException):
    """Raised when metadata storage operations fail."""
    pass
