"""DQ Accelerator - Production-grade Data Quality Framework for Databricks."""

from dq_accelerator.engine.dq_engine import DQAccelerator
from dq_accelerator.engine.custom_checks import CustomCheckRegistry
from dq_accelerator.reporting.summary import ValidationSummary
from dq_accelerator.utils.exceptions import (
    DQAcceleratorError,
    ConfigError,
    CheckExecutionError,
)

__version__ = "1.0.0"
__all__ = [
    "DQAccelerator",
    "CustomCheckRegistry",
    "ValidationSummary",
    "DQAcceleratorError",
    "ConfigError",
    "CheckExecutionError",
]
