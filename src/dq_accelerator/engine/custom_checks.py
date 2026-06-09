"""Custom check registry for dynamically loading user-defined check functions.

Custom checks are loaded at runtime from a user-specified Python module
(configured via `custom_checks_module` in the table YAML).
No checks are hardcoded here - users manage their own custom_checks_library.py.
"""

import importlib.util
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from pyspark.sql import Column

from dq_accelerator.utils.exceptions import CheckExecutionError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class CustomCheckRegistry:
    """Registry for managing custom DQ check functions.
    
    Custom checks are loaded dynamically from a user-provided Python module.
    Users can add/modify/remove checks without rebuilding the wheel.
    """

    def __init__(self) -> None:
        """Initialize an empty custom check registry."""
        self._checks: Dict[str, Callable[..., Column]] = {}
        logger.debug("Initialized empty CustomCheckRegistry")

    def register(self, name: str, func: Callable[..., Column]) -> None:
        """
        Register a custom check function.

        Args:
            name: Function name
            func: Check function that returns a Column
        """
        if name in self._checks:
            logger.warning(f"Overwriting existing custom check: {name}")
        self._checks[name] = func
        logger.debug(f"Registered custom check: {name}")

    def get(self, name: str) -> Optional[Callable[..., Column]]:
        """
        Get a registered custom check function.

        Args:
            name: Function name

        Returns:
            Check function or None if not found
        """
        return self._checks.get(name)

    def load_from_module(self, module_path: str, function_name: Optional[str] = None) -> None:
        """
        Load custom check function(s) from a Python module.

        Args:
            module_path: Path to Python module file or module name
            function_name: Specific function to load (if None, loads all valid functions)

        Raises:
            CheckExecutionError: If module cannot be loaded
        """
        try:
            # Check if it's a file path or module name
            if Path(module_path).exists():
                spec = importlib.util.spec_from_file_location("custom_checks_module", module_path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules["custom_checks_module"] = module
                    spec.loader.exec_module(module)
                else:
                    raise CheckExecutionError(f"Failed to load module from: {module_path}")
            else:
                module = importlib.import_module(module_path)

            if function_name:
                if hasattr(module, function_name):
                    func = getattr(module, function_name)
                    self.register(function_name, func)
                else:
                    raise CheckExecutionError(f"Function '{function_name}' not found in module: {module_path}")
            else:
                # Load all callable functions from module
                for attr_name in dir(module):
                    if not attr_name.startswith("_"):
                        attr = getattr(module, attr_name)
                        if callable(attr):
                            self.register(attr_name, attr)

            logger.info(f"Successfully loaded custom checks from: {module_path}")
        except Exception as e:
            raise CheckExecutionError(f"Error loading custom checks from {module_path}: {str(e)}") from e

    def get_all_checks(self) -> Dict[str, Callable[..., Column]]:
        """
        Get all registered custom checks.

        Returns:
            Dictionary of all registered check functions
        """
        return self._checks.copy()

    def clear(self) -> None:
        """Clear all registered custom checks."""
        self._checks.clear()
        logger.info("Cleared all custom checks")
