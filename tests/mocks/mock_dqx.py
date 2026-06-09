"""Mock implementation of DQX components for testing."""

from typing import Any, Callable, Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from dataclasses import dataclass


@dataclass
class ValidationStatus:
    """Mock validation status."""
    
    has_errors: bool = False
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class MockDQEngine:
    """
    Mock DQEngine for testing that doesn't require Databricks API calls.
    
    This mock implementation provides the same interface as databricks.labs.dqx.engine.DQEngine
    but executes checks locally without making API calls.
    """
    
    def __init__(self, workspace_client: Optional[Any] = None):
        """Initialize mock DQ engine."""
        self.workspace_client = workspace_client
    
    def validate_checks(
        self, 
        checks: List[Dict[str, Any]], 
        custom_functions: Optional[Dict[str, Callable]] = None
    ) -> ValidationStatus:
        """
        Validate check configurations.
        
        Args:
            checks: List of check configurations
            custom_functions: Dictionary of custom check functions
            
        Returns:
            ValidationStatus object
        """
        errors = []
        custom_functions = custom_functions or {}
        
        for idx, check in enumerate(checks):
            if "check" not in check:
                errors.append(f"Check {idx}: missing 'check' field")
                continue
                
            check_config = check["check"]
            if "function" not in check_config:
                errors.append(f"Check {idx}: missing 'function' field")
                continue
            
            function_name = check_config["function"]
            
            # Validate that function exists (either built-in or custom)
            builtin_functions = [
                "is_not_null",
                "is_not_null_and_not_empty",
                "is_unique",
                "is_in_range",
                "is_in",
                "sql_expression",
            ]
            
            if function_name not in builtin_functions and function_name not in custom_functions:
                errors.append(f"Check {idx}: unknown function '{function_name}'")
        
        return ValidationStatus(has_errors=len(errors) > 0, errors=errors)
    
    def apply_checks_by_metadata(
        self,
        df: DataFrame,
        checks: List[Dict[str, Any]],
        custom_functions: Optional[Dict[str, Callable]] = None,
    ) -> DataFrame:
        """
        Apply checks to DataFrame and add metadata columns.
        
        Args:
            df: Input DataFrame
            checks: List of check configurations
            custom_functions: Dictionary of custom check functions
            
        Returns:
            DataFrame with _warnings and _errors columns added
        """
        custom_functions = custom_functions or {}
        
        # Process each check and build conditions
        error_conditions = []
        warning_conditions = []
        
        for check in checks:
            criticality = check.get("criticality", "error")
            check_config = check["check"]
            function_name = check_config["function"]
            arguments = check_config.get("arguments", {})
            
            # Apply the check
            try:
                condition = self._apply_single_check(
                    df, function_name, arguments, custom_functions
                )
                
                # Invert condition for failures (condition = True means pass)
                failure_condition = ~condition
                
                # Create error/warning message
                message = self._create_check_message(function_name, arguments)
                
                # Add to appropriate list
                if criticality == "error":
                    error_conditions.append((failure_condition, message))
                else:
                    warning_conditions.append((failure_condition, message))
                    
            except Exception as e:
                # If check fails to execute, add as error
                error_conditions.append((F.lit(True), f"Check execution failed: {str(e)}"))
        
        # Build _errors column
        if error_conditions:
            error_arrays = [
                F.when(cond, F.array(F.lit(msg))).otherwise(F.array())
                for cond, msg in error_conditions
            ]
            errors_col = F.flatten(F.array(*error_arrays))
        else:
            errors_col = F.array().cast("array<string>")
        
        # Build _warnings column
        if warning_conditions:
            warning_arrays = [
                F.when(cond, F.array(F.lit(msg))).otherwise(F.array())
                for cond, msg in warning_conditions
            ]
            warnings_col = F.flatten(F.array(*warning_arrays))
        else:
            warnings_col = F.array().cast("array<string>")
        
        # Add metadata columns
        result_df = df.withColumn("_errors", errors_col).withColumn("_warnings", warnings_col)
        
        return result_df
    
    def apply_checks_by_metadata_and_split(
        self,
        df: DataFrame,
        checks: List[Dict[str, Any]],
        custom_functions: Optional[Dict[str, Callable]] = None,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Apply checks and split into valid and quarantined DataFrames.
        
        Args:
            df: Input DataFrame
            checks: List of check configurations
            custom_functions: Dictionary of custom check functions
            
        Returns:
            Tuple of (valid_df, quarantined_df)
        """
        # Apply checks first
        df_with_checks = self.apply_checks_by_metadata(df, checks, custom_functions)
        
        # Split based on _errors column (warnings don't cause quarantine)
        valid_df = df_with_checks.filter(F.size(F.col("_errors")) == 0).drop("_errors", "_warnings")
        quarantined_df = df_with_checks.filter((F.size(F.col("_errors")) > 0) | (F.size(F.col("_warnings")) > 0))
        
        return valid_df, quarantined_df
    
    def _apply_single_check(
        self,
        df: DataFrame,
        function_name: str,
        arguments: Dict[str, Any],
        custom_functions: Dict[str, Callable],
    ) -> Any:
        """
        Apply a single check function.
        
        Args:
            df: DataFrame being checked
            function_name: Name of check function
            arguments: Arguments for the function
            custom_functions: Custom check functions
            
        Returns:
            Column expression representing check result (True = pass, False = fail)
        """
        # Handle custom functions
        if function_name in custom_functions:
            func = custom_functions[function_name]
            return func(**arguments)
        
        # Handle built-in functions
        if function_name == "is_not_null":
            column = arguments["column"]
            return F.col(column).isNotNull()
        
        elif function_name == "is_not_null_and_not_empty":
            column = arguments["column"]
            return F.col(column).isNotNull() & (F.trim(F.col(column)) != "")
        
        elif function_name == "is_unique":
            # For is_unique, we need to use window functions
            from pyspark.sql import Window
            columns = arguments["columns"]
            
            # Create window partitioned by the columns to check uniqueness
            window = Window.partitionBy(*columns)
            count_col = F.count("*").over(window)
            
            # Unique if count = 1
            return count_col == 1
        
        elif function_name == "is_in_range":
            column = arguments["column"]
            min_value = arguments.get("min_value")
            max_value = arguments.get("max_value")
            
            col_expr = F.col(column)
            condition = F.lit(True)
            
            if min_value is not None:
                condition = condition & (col_expr >= min_value)
            if max_value is not None:
                condition = condition & (col_expr <= max_value)
            
            return col_expr.isNull() | condition
        
        elif function_name == "is_in":
            column = arguments["column"]
            values = arguments["values"]
            return F.col(column).isNull() | F.col(column).isin(values)
        
        elif function_name == "sql_expression":
            expression = arguments["expression"]
            return F.expr(expression)
        
        else:
            raise ValueError(f"Unknown check function: {function_name}")
    
    def _create_check_message(self, function_name: str, arguments: Dict[str, Any]) -> str:
        """Create a human-readable check failure message."""
        if function_name == "is_not_null":
            return f"Column '{arguments['column']}' is null"
        elif function_name == "is_unique":
            cols = ", ".join(arguments["columns"])
            return f"Duplicate values found in columns: {cols}"
        elif function_name == "is_in_range":
            column = arguments["column"]
            min_val = arguments.get("min_value", "any")
            max_val = arguments.get("max_value", "any")
            return f"Column '{column}' outside range [{min_val}, {max_val}]"
        elif function_name in arguments:
            return f"Check '{function_name}' failed"
        else:
            return f"Check '{function_name}' failed for {arguments.get('column', 'unknown column')}"
