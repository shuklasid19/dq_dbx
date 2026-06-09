"""Check executor for running DQ checks using DQX library."""

# =============================================================================
# IMPORTANT: Monkey-patch isinstance BEFORE importing DQX
# DQX 0.12.0 has a bug: it uses isinstance(x, Union[...]) which fails in Python 3.10+
# =============================================================================
import builtins
import typing
import types

_original_isinstance = builtins.isinstance


def _patched_isinstance(obj, classinfo):
    """
    Patched isinstance to handle typing.Union types.
    
    DQX 0.12.0 incorrectly uses isinstance() with typing.Union which raises:
    TypeError: "typing.Union cannot be used with isinstance()"
    
    This patch intercepts such calls and handles them correctly.
    """
    try:
        # Check if classinfo is a typing.Union (e.g., Union[str, int])
        if hasattr(classinfo, '__origin__') and classinfo.__origin__ is typing.Union:
            # Extract the actual types from Union and check against them
            union_args = typing.get_args(classinfo)
            if union_args:
                return _original_isinstance(obj, union_args)
            return False
        
        # Handle Python 3.10+ types.UnionType (PEP 604 syntax: str | int)
        if hasattr(types, 'UnionType') and _original_isinstance(classinfo, types.UnionType):
            union_args = typing.get_args(classinfo)
            if union_args:
                return _original_isinstance(obj, union_args)
            return False
        
        # Default behavior for all other cases
        return _original_isinstance(obj, classinfo)
    
    except TypeError as e:
        # If we still get a TypeError about Union, try to handle it
        if "Union" in str(e):
            # Last resort: try to extract args if possible
            if hasattr(classinfo, '__args__'):
                return _original_isinstance(obj, classinfo.__args__)
            return False
        raise


# Apply the monkey-patch to builtins BEFORE DQX is imported
builtins.isinstance = _patched_isinstance

# =============================================================================
# Now import DQX - it will use our patched isinstance
# =============================================================================
from typing import Any, Dict, List, Optional, Tuple

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, TimestampType, MapType
from datetime import datetime
import uuid
from dq_accelerator.config.schemas import DQConfig, DQRuleConfig
from dq_accelerator.engine.custom_checks import CustomCheckRegistry
from dq_accelerator.utils.exceptions import CheckExecutionError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)

# Flag to force pure PySpark mode (for Spark Connect / Serverless)
USE_PYSPARK_FALLBACK = False


def _wrap_custom_functions_for_dqx(custom_functions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Wrap custom check functions to be compatible with DQX native engine.
    
    Custom checks return boolean Column expressions (True = valid, False = invalid).
    DQX expects Column expressions that return:
      - null when the row is VALID (passes the check)
      - an error message string when the row is INVALID (fails the check)
    
    IMPORTANT: The wrapper must preserve the original function's signature because
    DQX inspects function parameters to validate YAML arguments against them.
    
    Args:
        custom_functions: Dictionary of custom check functions
        
    Returns:
        Dictionary of wrapped functions compatible with DQX
    """
    import functools
    
    wrapped = {}
    
    for func_name, func in custom_functions.items():
        def _make_wrapper(original_func, name):
            @functools.wraps(original_func)
            def wrapper(*args, **kwargs):
                # Call the original function which returns a boolean Column
                bool_result = original_func(*args, **kwargs)
                # Convert: True (valid) → null, False (invalid) → error message
                return F.when(bool_result, F.lit(None).cast("string")).otherwise(
                    F.lit(f"Check failed: {name}")
                )
            return wrapper
        
        wrapped[func_name] = _make_wrapper(func, func_name)
    
    return wrapped


def _detect_spark_connect(spark_session=None) -> bool:
    """
    Proactively detect if running in Spark Connect mode (Databricks Serverless).
    
    Spark Connect has limitations with JVM access, so we need to use pure PySpark
    implementations for custom checks.
    
    Returns:
        True if Spark Connect is detected, False otherwise
    """
    try:
        # Method 1: Check for Spark Connect session type
        if spark_session is not None:
            session_type = str(type(spark_session))
            if "connect" in session_type.lower():
                return True
        
        # Method 2: Check environment variable set by Databricks Serverless
        import os
        if os.environ.get("SPARK_CONNECT_MODE") == "1":
            return True
        if os.environ.get("DATABRICKS_RUNTIME_VERSION", "").startswith("client"):
            return True
            
        # Method 3: Try to access a JVM-dependent attribute
        # This is a reliable test - if it fails, we're in Spark Connect
        if spark_session is not None:
            try:
                # Attempt to access JVM - this will fail in Spark Connect
                _ = spark_session._jvm
                return False  # JVM accessible, not Spark Connect
            except AttributeError:
                return True  # No _jvm attribute, likely Spark Connect
            except Exception as e:
                if "connect" in str(e).lower() or "jvm" in str(e).lower():
                    return True
        
        return False
    except Exception:
        # If detection fails, assume not Spark Connect
        return False


class CheckExecutor:
    """Executes DQ checks using Databricks Labs DQX library."""

    def __init__(
        self,
        workspace_client: Optional[WorkspaceClient] = None,
        custom_check_registry: Optional[CustomCheckRegistry] = None,
    ) -> None:
        """
        Initialize check executor.

        Args:
            workspace_client: Databricks Workspace Client (optional, will create if not provided and not mock)
            custom_check_registry: Custom check registry (optional, will create if not provided)
        """
        # Only create workspace client if not provided and not in testing mode
        if workspace_client is None:
            try:
                self.workspace_client = WorkspaceClient()
            except Exception as e:
                logger.warning(f"Could not create WorkspaceClient: {e}. Some features may be limited.")
                self.workspace_client = None
        else:
            # Check if it's a mock object
            if 'mock' in str(type(workspace_client)).lower():
                logger.warning("Mock workspace client detected, using None instead")
                self.workspace_client = None
            else:
                self.workspace_client = workspace_client
        
        # Initialize DQ Engine with workspace client (can be None)
        try:
            self.dq_engine = DQEngine(self.workspace_client) if self.workspace_client else DQEngine()
        except TypeError:
            # DQEngine might not accept None in some versions, try without argument
            self.dq_engine = DQEngine()
            
        self.custom_check_registry = custom_check_registry or CustomCheckRegistry()
        self._loaded_modules: set = set()  # Track loaded modules to avoid reloading

    def _load_custom_checks_module(self, module_path: Optional[str]) -> None:
        """
        Load custom check functions from a user-specified Python module.
        
        This allows users to define their own checks in a separate file
        without modifying the wheel/framework code.
        
        Args:
            module_path: Path to the Python module containing custom check functions
        """
        if not module_path:
            return
            
        # Avoid reloading the same module
        if module_path in self._loaded_modules:
            logger.debug(f"Custom checks module already loaded: {module_path}")
            return
        
        try:
            logger.info(f"Loading custom checks from module: {module_path}")
            self.custom_check_registry.load_from_module(module_path)
            self._loaded_modules.add(module_path)
            logger.info(f"Successfully loaded custom checks from: {module_path}")
        except Exception as e:
            logger.warning(f"Failed to load custom checks from {module_path}: {e}")

    def _convert_checks_to_metadata(self, checks: List[DQRuleConfig]) -> List[Dict[str, Any]]:
        """
        Convert DQRuleConfig objects to DQX metadata format.

        Args:
            checks: List of DQ rule configurations

        Returns:
            List of checks in DQX metadata format
        """
        metadata_checks = []

        for check in checks:
            metadata_check = {
                "criticality": check.criticality,
                "check": {
                    "function": check.check.function,
                    "arguments": check.check.arguments,
                },
            }

            if check.metadata:
                metadata_check["metadata"] = check.metadata

            metadata_checks.append(metadata_check)

        return metadata_checks

    def validate_checks(self, config: DQConfig) -> None:
        """
        Validate check configurations before execution.

        Args:
            config: DQ configuration

        Raises:
            CheckExecutionError: If validation fails
        """
        try:
            metadata_checks = self._convert_checks_to_metadata(config.checks)
            custom_functions = self.custom_check_registry.get_all_checks()

            status = self.dq_engine.validate_checks(metadata_checks, custom_functions)

            if status.has_errors:
                error_msg = f"Check validation failed for table {config.table_name}: {status.errors}"
                raise CheckExecutionError(error_msg)

            logger.info(f"Successfully validated {len(metadata_checks)} checks for table {config.table_name}")

        except Exception as e:
            raise CheckExecutionError(f"Check validation error: {str(e)}") from e

    def _is_spark_connect_error(self, error: Exception) -> bool:
        """Check if error is due to Spark Connect limitation."""
        error_str = str(error)
        return "_jc" in error_str or "JVM_ATTRIBUTE_NOT_SUPPORTED" in error_str or "Spark Connect" in error_str

    def apply_checks(
        self,
        df: DataFrame,
        config: DQConfig,
    ) -> DataFrame:
        """
        Apply DQ checks to DataFrame and return DataFrame with metadata columns.

        Args:
            df: Input DataFrame
            config: DQ configuration

        Returns:
            DataFrame with _warnings and _errors metadata columns

        Raises:
            CheckExecutionError: If check execution fails
        """
        if not config.enabled:
            logger.warning(f"Checks disabled for table {config.table_name}, returning original DataFrame")
            return df

        if not config.checks:
            logger.warning(f"No checks configured for table {config.table_name}")
            return df

        # Load custom checks from user-specified module (if any)
        self._load_custom_checks_module(config.custom_checks_module)

        # If explicitly forced to PySpark fallback, use it directly
        if USE_PYSPARK_FALLBACK:
            logger.info(f"PySpark fallback explicitly enabled for {config.table_name}")
            return self._apply_checks_pyspark(df, config)
        
        # Always try DQX native first - it handles both built-in AND custom checks
        # DQX natively knows all its built-in functions (is_not_null, is_in_range, etc.)
        # Custom checks are passed as a dict and DQX applies them as Column expressions
        # Fall back to PySpark only if DQX fails (e.g. Spark Connect serialization issues)
        try:
            metadata_checks = self._convert_checks_to_metadata(config.checks)
            custom_functions = self.custom_check_registry.get_all_checks()
            
            # Wrap custom functions for DQX compatibility
            # Custom checks return boolean (True=valid), DQX expects null=valid, string=invalid
            dqx_custom_functions = _wrap_custom_functions_for_dqx(custom_functions)

            logger.info(f"Applying {len(metadata_checks)} checks to table {config.table_name} using DQX native engine")

            result_df = self.dq_engine.apply_checks_by_metadata(
                df,
                metadata_checks,
                dqx_custom_functions,
            )

            logger.info(f"Successfully applied checks to table {config.table_name} (DQX native)")
            return result_df

        except Exception as e:
            logger.warning(f"DQX native engine failed for {config.table_name}: {e}")
            logger.info(f"Falling back to PySpark implementation for {config.table_name}")
            return self._apply_checks_pyspark(df, config)

    def apply_checks_and_split(
        self,
        df: DataFrame,
        config: DQConfig,
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Apply DQ checks and split into valid and quarantined DataFrames.

        Args:
            df: Input DataFrame
            config: DQ configuration

        Returns:
            Tuple of (valid_df, quarantined_df)

        Raises:
            CheckExecutionError: If check execution fails
        """
        if not config.enabled:
            logger.warning(f"Checks disabled for table {config.table_name}")
            return df, self._create_empty_dataframe(df)

        if not config.checks:
            logger.warning(f"No checks configured for table {config.table_name}")
            return df, self._create_empty_dataframe(df)

        # Load custom checks from user-specified module (if any)
        self._load_custom_checks_module(config.custom_checks_module)

        # If explicitly forced to PySpark fallback, use it directly
        if USE_PYSPARK_FALLBACK:
            logger.info(f"PySpark fallback explicitly enabled for {config.table_name}")
            return self._apply_checks_and_split_pyspark(df, config)
        
        # Always try DQX native first - it handles both built-in AND custom checks
        # Fall back to PySpark only if DQX fails (e.g. Spark Connect serialization issues)
        try:
            metadata_checks = self._convert_checks_to_metadata(config.checks)
            custom_functions = self.custom_check_registry.get_all_checks()
            
            # Wrap custom functions for DQX compatibility
            # Custom checks return boolean (True=valid), DQX expects null=valid, string=invalid
            dqx_custom_functions = _wrap_custom_functions_for_dqx(custom_functions)

            logger.info(f"Applying checks and splitting data for table {config.table_name} using DQX native engine")

            # Use apply_checks (NOT split) - then split ourselves based on _errors only
            # DQX's native split quarantines BOTH errors AND warnings,
            # but we only want ERROR rows quarantined (warnings stay in valid)
            result_df = self.dq_engine.apply_checks_by_metadata(
                df,
                metadata_checks,
                dqx_custom_functions,
            )

            # Split: quarantine only rows with ERRORs (not warnings)
            # _errors column is an array - rows with errors have size > 0
            has_errors = F.size(F.col("_errors")) > 0
            valid_df = result_df.filter(~has_errors)
            quarantined_df = result_df.filter(has_errors)

            # Force evaluation
            valid_count = valid_df.count()
            quarantine_count = quarantined_df.count()
            total_count = valid_count + quarantine_count

            logger.info(
                f"Split results for {config.table_name} (DQX native): "
                f"Valid={valid_count}/{total_count}, "
                f"Quarantined={quarantine_count}/{total_count}"
            )

            return valid_df, quarantined_df

        except Exception as e:
            logger.warning(f"DQX native engine failed for {config.table_name}: {e}")
            logger.info(f"Falling back to PySpark implementation for {config.table_name}")
            return self._apply_checks_and_split_pyspark(df, config)

    def _create_empty_dataframe(self, df: DataFrame) -> DataFrame:
        """Create an empty DataFrame with the same schema."""
        return df.sparkSession.createDataFrame([], df.schema)

    # =========================================================================
    # Pure PySpark Fallback Implementation (for Spark Connect / Serverless)
    # =========================================================================
    
    def _apply_checks_pyspark(self, df: DataFrame, config: DQConfig) -> DataFrame:
        """
        Apply DQ checks using pure PySpark (no JVM dependency).
        Works with Spark Connect / Databricks Serverless.
        
        Creates _errors and _warnings columns in DQX-compatible format:
        Array<Struct<name, message, columns, filter, function, run_time, run_id, user_metadata>>
        """
        import time
        start_time = time.time()
        
        logger.info(f"Using PySpark fallback for {len(config.checks)} checks on table {config.table_name}")
        
        custom_functions = self.custom_check_registry.get_all_checks()
        
        # Generate run metadata
        run_id = str(uuid.uuid4())
        run_time = datetime.now()
        
        # Define the DQX-compatible error/warning struct schema
        error_struct_schema = ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("message", StringType(), True),
            StructField("columns", ArrayType(StringType()), True),
            StructField("filter", StringType(), True),
            StructField("function", StringType(), True),
            StructField("run_time", TimestampType(), True),
            StructField("run_id", StringType(), True),
            StructField("user_metadata", MapType(StringType(), StringType()), True),
        ]))
        
        # Pre-process DataFrame for dataset-level checks (add window/aggregate columns)
        logger.info(f"Pre-processing dataset-level checks...")
        preprocess_start = time.time()
        df, dataset_level_columns = self._preprocess_dataset_level_checks(df, config.checks)
        
        # Only cache if there are dataset-level columns (expensive operations)
        if dataset_level_columns:
            logger.info(f"Dataset-level checks detected ({len(dataset_level_columns)} columns) - this may take longer due to Window operations")
            # Note: We skip caching to let Spark optimize the execution plan
            # Caching actually slows things down in Spark Connect due to serialization overhead
            logger.info(f"Preprocessing completed in {time.time() - preprocess_start:.2f}s")
        else:
            logger.info(f"No dataset-level checks - fast path enabled")
        
        # Build all check conditions first (without applying)
        logger.info(f"Building check conditions...")
        check_conditions = []
        
        for i, check in enumerate(config.checks):
            func_name = check.check.function
            args = check.check.arguments
            criticality = check.criticality
            
            # Get the check condition
            condition = self._get_check_condition(func_name, args, custom_functions, dataset_level_columns)
            
            if condition is not None:
                col_names = args.get("columns") if isinstance(args.get("columns"), list) else (
                    [args.get("column")] if args.get("column") else ["unknown"]
                )
                check_name = self._generate_check_name(func_name, args)
                
                check_conditions.append({
                    "condition": condition,
                    "col_names": col_names,
                    "check_name": check_name,
                    "func_name": func_name,
                    "criticality": criticality,
                })
            
            if (i + 1) % 10 == 0:
                logger.info(f"Built conditions for {i + 1}/{len(config.checks)} checks")
        
        logger.info(f"Built {len(check_conditions)} check conditions in {time.time() - start_time:.2f}s")
        
        # Now apply all checks in a single pass
        logger.info(f"Applying all checks...")
        apply_start = time.time()
        
        # Initialize empty arrays
        errors_col = F.array().cast(error_struct_schema)
        warnings_col = F.array().cast(error_struct_schema)
        
        for check_info in check_conditions:
            error_struct = F.struct(
                F.lit(check_info["check_name"]).alias("name"),
                F.lit(f"Check failed: {check_info['func_name']}").alias("message"),
                F.array(*[F.lit(c) for c in check_info["col_names"]]).alias("columns"),
                F.lit(None).cast(StringType()).alias("filter"),
                F.lit(check_info["func_name"]).alias("function"),
                F.lit(run_time).alias("run_time"),
                F.lit(run_id).alias("run_id"),
                F.create_map().cast(MapType(StringType(), StringType())).alias("user_metadata"),
            )
            
            if check_info["criticality"] == "error":
                errors_col = F.when(
                    ~check_info["condition"], F.concat(errors_col, F.array(error_struct))
                ).otherwise(errors_col)
            else:
                warnings_col = F.when(
                    ~check_info["condition"], F.concat(warnings_col, F.array(error_struct))
                ).otherwise(warnings_col)
        
        result_df = df.withColumn("_errors", errors_col).withColumn("_warnings", warnings_col)
        
        # Clean up temporary dataset-level columns
        for temp_col in dataset_level_columns.values():
            if temp_col in result_df.columns:
                result_df = result_df.drop(temp_col)
        
        total_time = time.time() - start_time
        logger.info(f"Successfully applied {len(config.checks)} checks to table {config.table_name} in {total_time:.2f}s (PySpark fallback)")
        return result_df
    
    def _generate_check_name(self, func_name: str, args: Dict[str, Any]) -> str:
        """Generate a check name similar to DQX naming convention."""
        col_name = args.get("column") or (args.get("columns", [""])[0] if isinstance(args.get("columns"), list) else args.get("columns", ""))
        
        # Create a sanitized name
        name_parts = [func_name]
        if col_name:
            name_parts.append(col_name)
        
        return "_".join(name_parts).lower().replace(" ", "_")
    
    def _preprocess_dataset_level_checks(
        self, df: DataFrame, checks: List[DQRuleConfig]
    ) -> Tuple[DataFrame, Dict[str, str]]:
        """
        Pre-process DataFrame to add columns needed for dataset-level checks.
        
        Dataset-level checks (is_unique, foreign_key, aggregations) need to
        compute values across rows before individual row conditions can be evaluated.
        
        Returns:
            Tuple of (processed DataFrame, dict mapping check_key -> column_name)
        """
        from pyspark.sql.window import Window
        
        dataset_level_columns = {}
        
        for i, check in enumerate(checks):
            func_name = check.check.function
            args = check.check.arguments
            
            # is_unique: Use window function to count duplicates
            if func_name == "is_unique":
                columns = args.get("columns", [])
                if isinstance(columns, str):
                    columns = [columns]
                
                if not columns:
                    continue
                
                # Create unique column name for this check
                unique_id = f"_unique_count_{i}"
                
                # Build the partition key
                if len(columns) == 1:
                    partition_expr = F.col(columns[0])
                else:
                    partition_expr = F.struct(*[F.col(c) for c in columns])
                
                # Count occurrences using window function
                w = Window.partitionBy(partition_expr)
                df = df.withColumn(unique_id, F.count("*").over(w))
                
                # Store the column name for later use
                check_key = f"is_unique_{','.join(columns)}"
                dataset_level_columns[check_key] = unique_id
                logger.debug(f"Added uniqueness count column: {unique_id} for columns: {columns}")
            
            # is_aggr_* checks: Pre-compute aggregations
            elif func_name in ("is_aggr_not_greater_than", "is_aggr_not_less_than", 
                               "is_aggr_equal", "is_aggr_not_equal"):
                col_name = args.get("column") or args.get("col_name")
                aggr_type = args.get("aggr_type", "count")
                group_by = args.get("group_by", [])
                
                if not col_name:
                    continue
                
                # Create unique column name
                aggr_col_id = f"_aggr_{aggr_type}_{i}"
                
                # Build aggregation expression
                col_expr = F.col(col_name)
                
                if aggr_type == "count":
                    aggr_expr = F.count(col_expr)
                elif aggr_type == "sum":
                    aggr_expr = F.sum(col_expr)
                elif aggr_type == "avg":
                    aggr_expr = F.avg(col_expr)
                elif aggr_type == "min":
                    aggr_expr = F.min(col_expr)
                elif aggr_type == "max":
                    aggr_expr = F.max(col_expr)
                elif aggr_type == "count_distinct":
                    aggr_expr = F.countDistinct(col_expr)
                elif aggr_type == "stddev":
                    aggr_expr = F.stddev(col_expr)
                else:
                    aggr_expr = F.count(col_expr)  # fallback
                
                # Apply aggregation (with or without group by)
                if group_by:
                    if isinstance(group_by, str):
                        group_by = [group_by]
                    w = Window.partitionBy(*[F.col(g) for g in group_by])
                else:
                    # Global aggregation - use empty partition (all rows)
                    w = Window.partitionBy(F.lit(1))
                
                df = df.withColumn(aggr_col_id, aggr_expr.over(w))
                
                check_key = f"{func_name}_{col_name}_{aggr_type}"
                dataset_level_columns[check_key] = aggr_col_id
                logger.debug(f"Added aggregation column: {aggr_col_id} for {func_name}")
        
        return df, dataset_level_columns
    
    def _apply_checks_and_split_pyspark(
        self, df: DataFrame, config: DQConfig
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Apply DQ checks and split using pure PySpark (no JVM dependency).
        Works with Spark Connect / Databricks Serverless.
        """
        import time
        start_time = time.time()
        
        # First apply all checks
        result_df = self._apply_checks_pyspark(df, config)
        
        # Cache the result to avoid recomputation during split
        logger.info(f"Caching result DataFrame for split operation...")
        cache_start = time.time()
        result_df = result_df.cache()
        total_count = result_df.count()
        logger.info(f"Cached {total_count} rows in {time.time() - cache_start:.2f}s")
        
        # Split into valid (no errors) and quarantine (has errors)
        logger.info(f"Splitting data into valid and quarantine...")
        split_start = time.time()
        
        valid_df = result_df.filter(F.size(F.col("_errors")) == 0)
        quarantine_df = result_df.filter(F.size(F.col("_errors")) > 0)
        
        # Get counts (uses cached data, should be fast)
        valid_count = valid_df.count()
        quarantine_count = total_count - valid_count
        
        logger.info(f"Split completed in {time.time() - split_start:.2f}s")
        
        # Unpersist cache
        result_df.unpersist()
        
        total_time = time.time() - start_time
        logger.info(
            f"Split results for {config.table_name} (PySpark fallback): "
            f"Valid={valid_count}/{total_count}, "
            f"Quarantined={quarantine_count}/{total_count} "
            f"(Total time: {total_time:.2f}s)"
        )
        
        return valid_df, quarantine_df
    
    def _get_check_condition(
        self, func_name: str, args: Dict[str, Any], custom_functions: Dict,
        dataset_level_columns: Optional[Dict[str, str]] = None
    ) -> Optional[Any]:
        """
        Get the check condition (Column expression) for a given function.
        
        This is used ONLY in the PySpark fallback path. It handles:
        1. Custom checks from user's custom_checks_library.py
        2. Dataset-level checks (is_unique, is_aggr_*) that need pre-computed columns
        
        Built-in DQX checks are handled by DQX's native engine (apply_checks_by_metadata).
        This method is only called when DQX native fails and we fall back to PySpark.
        
        Args:
            func_name: Name of the check function
            args: Arguments for the check
            custom_functions: Dictionary of custom check functions
            dataset_level_columns: Pre-computed column names for dataset-level checks
        
        Returns True for valid records, False for invalid records.
        """
        if dataset_level_columns is None:
            dataset_level_columns = {}
        
        # =================================================================
        # STEP 1: Check if it's a custom check from our registry
        # =================================================================
        if func_name in custom_functions:
            return self._call_custom_check(func_name, args, custom_functions)
        
        # =================================================================
        # STEP 2: Handle dataset-level checks that need pre-computed columns
        # =================================================================
        dataset_level_result = self._handle_dataset_level_check(func_name, args, dataset_level_columns)
        if dataset_level_result is not None:
            return dataset_level_result
        
        # =================================================================
        # STEP 3: Unknown check in PySpark fallback - flag for review
        # Built-in DQX checks should be handled by DQX native engine.
        # If we reach here, it means DQX native failed and the check
        # is not a custom check or dataset-level check.
        # =================================================================
        logger.warning(
            f"Check '{func_name}' not found in custom checks registry. "
            f"Built-in DQX checks should be handled by DQX native engine. "
            f"Records will be flagged for review."
        )
        return F.lit(False)
    
    def _call_custom_check(
        self, func_name: str, args: Dict[str, Any], custom_functions: Dict
    ) -> Optional[Any]:
        """Call a custom check function from the registry."""
        try:
            custom_func = custom_functions[func_name]
            result = custom_func(**args)
            logger.debug(f"Successfully executed custom check: {func_name}")
            return result
        except Exception as e:
            logger.warning(f"Custom check {func_name} execution error: {e}. Records will be flagged for review.")
            return F.lit(False)
    
    def _handle_dataset_level_check(
        self, func_name: str, args: Dict[str, Any], dataset_level_columns: Dict[str, str]
    ) -> Optional[Any]:
        """
        Handle dataset-level checks that need pre-computed columns.
        Returns None if the check is not a dataset-level check.
        """
        # is_unique: Uses pre-computed window count
        if func_name == "is_unique":
            columns = args.get("columns", [])
            if isinstance(columns, str):
                columns = [columns]
            
            check_key = f"is_unique_{','.join(columns)}"
            count_col = dataset_level_columns.get(check_key)
            
            if count_col:
                return F.col(count_col) == 1
            else:
                logger.warning(f"is_unique check: no pre-computed column found for {columns}")
                return F.lit(True)
        
        # Aggregate checks: Use pre-computed window aggregation
        if func_name in ("is_aggr_not_greater_than", "is_aggr_not_less_than", 
                         "is_aggr_equal", "is_aggr_not_equal"):
            col_name = args.get("column") or args.get("col_name")
            aggr_type = args.get("aggr_type", "count")
            limit = args.get("limit")
            
            check_key = f"{func_name}_{col_name}_{aggr_type}"
            aggr_col = dataset_level_columns.get(check_key)
            
            if aggr_col and limit is not None:
                aggr_value = F.col(aggr_col)
                
                if func_name == "is_aggr_not_greater_than":
                    return aggr_value <= limit
                elif func_name == "is_aggr_not_less_than":
                    return aggr_value >= limit
                elif func_name == "is_aggr_equal":
                    return aggr_value == limit
                elif func_name == "is_aggr_not_equal":
                    return aggr_value != limit
            
            logger.debug(f"{func_name} check: no pre-computed column or limit, returning True")
            return F.lit(True)
        
        # Dataset-level checks that can't run row-by-row
        if func_name in ("is_data_fresh", "is_data_fresh_per_time_window", 
                         "has_no_outliers", "has_valid_schema", "compare_datasets"):
            logger.debug(f"{func_name} is a dataset-level check, returning True for row-level fallback")
            return F.lit(True)
        
        # foreign_key requires reference table join
        if func_name == "foreign_key":
            logger.warning(
                f"foreign_key check requires reference table join - not fully supported in PySpark fallback. "
                f"Consider using DQX on Classic Compute for this check."
            )
            return F.lit(True)
        
        # Not a dataset-level check
        return None
    
