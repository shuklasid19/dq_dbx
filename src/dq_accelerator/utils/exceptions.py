"""Custom exceptions for DQ Accelerator."""


class DQAcceleratorError(Exception):
    """Base exception for DQ Accelerator."""

    pass


class ConfigError(DQAcceleratorError):
    """Exception raised for configuration errors."""

    pass


class CheckExecutionError(DQAcceleratorError):
    """Exception raised during check execution."""

    pass


class StorageError(DQAcceleratorError):
    """Exception raised for storage-related errors."""

    pass


class ValidationError(DQAcceleratorError):
    """Exception raised for validation errors."""

    pass
