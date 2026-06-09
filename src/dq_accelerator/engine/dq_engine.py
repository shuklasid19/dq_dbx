"""Main DQ Accelerator engine orchestrating the entire quality check workflow."""

import uuid
from typing import Dict, Optional, Tuple

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession

from dq_accelerator.config.loader import ConfigLoader
from dq_accelerator.config.merger import ConfigMerger
from dq_accelerator.config.schemas import DQConfig, LayerConfig, TableConfig
from dq_accelerator.engine.check_executor import CheckExecutor
from dq_accelerator.engine.custom_checks import CustomCheckRegistry
from dq_accelerator.reporting.metrics import MetricsGenerator
from dq_accelerator.reporting.summary import ValidationSummary
from dq_accelerator.storage.result_writer import ResultWriter
from dq_accelerator.utils.exceptions import DQAcceleratorError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class DQAccelerator:
    """
    Main orchestrator for Data Quality checks using Databricks Labs DQX.

    This class provides a high-level API for running DQ checks on DataFrames
    with configuration-driven validation, multi-cloud storage support, and
    comprehensive reporting.
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        workspace_client: Optional[WorkspaceClient] = None,
        enable_workspace_features: bool = True,
    ) -> None:
        """
        Initialize DQ Accelerator.

        Args:
            spark: SparkSession (optional, will create from active session if not provided)
            workspace_client: Databricks Workspace Client (optional)
            enable_workspace_features: If False, skip workspace-dependent features (useful for testing)
        """
        self.spark = spark or SparkSession.getActiveSession()
        if self.spark is None:
            raise DQAcceleratorError("No active SparkSession found. Please provide a SparkSession.")

        # Only create workspace client if features are enabled and not provided
        self.enable_workspace_features = enable_workspace_features
        self.workspace_client = None
        
        if enable_workspace_features:
            if workspace_client is not None:
                # Check if it's a real WorkspaceClient, not a mock
                if hasattr(workspace_client, '__class__') and \
                   'mock' not in str(type(workspace_client)).lower():
                    self.workspace_client = workspace_client
                else:
                    logger.warning("Mock workspace client detected, disabling workspace features")
                    self.enable_workspace_features = False
            else:
                try:
                    self.workspace_client = WorkspaceClient()
                except Exception as e:
                    logger.warning(f"Could not create WorkspaceClient: {e}. Disabling workspace features.")
                    self.enable_workspace_features = False

        self.custom_check_registry = CustomCheckRegistry()
        self.check_executor = CheckExecutor(
            self.workspace_client if self.enable_workspace_features else None,
            self.custom_check_registry
        )
        self.result_writer = ResultWriter(self.spark)
        self.metrics_generator = MetricsGenerator(self.spark)
        self.validation_summary = ValidationSummary(self.spark)

        logger.info(f"DQ Accelerator initialized successfully (workspace_features: {self.enable_workspace_features})")

    def run_checks(
        self,
        df: DataFrame,
        table_config: TableConfig,
        layer_config: Optional[LayerConfig] = None,
        split_valid_invalid: bool = True,
        write_results: bool = True,
        execution_id: Optional[str] = None,
    ) -> Dict[str, DataFrame]:
        """
        Run DQ checks on a DataFrame.

        Args:
            df: Input DataFrame to validate
            table_config: Table configuration
            layer_config: Layer configuration (optional)
            split_valid_invalid: If True, split into valid and quarantined DataFrames
            write_results: If True, write results to configured storage locations
            execution_id: Optional execution ID (generated if not provided)

        Returns:
            Dictionary containing:
                - 'result': DataFrame with metadata columns (or valid_df if split)
                - 'quarantine': Quarantined DataFrame (if split_valid_invalid=True)
                - 'metrics': Metrics DataFrame
                - 'summary': Summary DataFrame

        Raises:
            DQAcceleratorError: If validation or execution fails
        """
        exec_id = execution_id or str(uuid.uuid4())
        logger.info(f"Starting DQ checks for table: {table_config.table_name} (execution_id: {exec_id})")

        try:
            # Merge configurations
            config = ConfigMerger.merge_configs(layer_config, table_config)

            # Load custom checks if configured
            self._load_custom_checks(config)

            # Load custom checks from user-specified module (must happen BEFORE validation)
            if config.custom_checks_module:
                self.check_executor._load_custom_checks_module(config.custom_checks_module)

            # Validate check configurations
            self.check_executor.validate_checks(config)

            # Execute checks
            if split_valid_invalid:
                valid_df, quarantine_df = self.check_executor.apply_checks_and_split(df, config)
                result_df = valid_df
            else:
                result_df = self.check_executor.apply_checks(df, config)
                quarantine_df = None

            # Generate metrics from the DataFrame with DQ metadata
            df_with_metadata = result_df if not split_valid_invalid else self.check_executor.apply_checks(df, config)
            metrics_df = self.metrics_generator.generate_metrics(
                df=df_with_metadata,
                table_name=config.table_name,
                layer=config.layer,
                execution_id=exec_id,
            )

            # Generate summary
            summary_df = self.validation_summary.generate_summary(
                df=df_with_metadata,
                table_name=config.table_name,
                layer=config.layer,
                execution_id=exec_id,
            )

            # Print summary
            self.validation_summary.print_summary(summary_df)

            # Write results if configured
            if write_results:
                self._write_results(result_df, quarantine_df, metrics_df, summary_df, config)

            results = {
                "result": result_df,
                "metrics": metrics_df,
                "summary": summary_df,
            }

            if quarantine_df is not None:
                results["quarantine"] = quarantine_df

            logger.info(f"Successfully completed DQ checks for table: {config.table_name}")
            return results

        except Exception as e:
            logger.error(f"Error running DQ checks for table {table_config.table_name}: {str(e)}")
            raise DQAcceleratorError(f"Failed to run DQ checks: {str(e)}") from e

    def run_checks_from_config_files(
        self,
        df: DataFrame,
        table_config_path: str,
        layer_config_path: Optional[str] = None,
        split_valid_invalid: bool = True,
        write_results: bool = True,
        execution_id: Optional[str] = None,
    ) -> Dict[str, DataFrame]:
        """
        Run DQ checks using configuration files.

        Args:
            df: Input DataFrame to validate
            table_config_path: Path to table configuration YAML file
            layer_config_path: Path to layer configuration YAML file (optional)
            split_valid_invalid: If True, split into valid and quarantined DataFrames
            write_results: If True, write results to configured storage locations
            execution_id: Optional execution ID

        Returns:
            Dictionary containing result, quarantine, metrics, and summary DataFrames
        """
        # Load configurations
        table_config = ConfigLoader.load_table_config(table_config_path)
        layer_config = ConfigLoader.load_layer_config(layer_config_path) if layer_config_path else None

        return self.run_checks(
            df=df,
            table_config=table_config,
            layer_config=layer_config,
            split_valid_invalid=split_valid_invalid,
            write_results=write_results,
            execution_id=execution_id,
        )

    def _load_custom_checks(self, config: DQConfig) -> None:
        """Load custom checks from configuration."""
        for custom_check in config.custom_checks:
            if custom_check.module_path:
                self.custom_check_registry.load_from_module(
                    custom_check.module_path,
                    custom_check.function_name,
                )

    def _write_results(
        self,
        result_df: DataFrame,
        quarantine_df: Optional[DataFrame],
        metrics_df: DataFrame,
        summary_df: DataFrame,
        config: DQConfig,
    ) -> None:
        """Write results to configured storage locations with dynamic path resolution."""
        write_errors = []
        
        # Write valid data
        if config.output.valid_data:
            try:
                logger.info(f"Writing valid data to {config.output.valid_data.catalog}.{config.output.valid_data.schema_name}.{config.output.valid_data.table}")
                self.result_writer.write_dataframe(
                    df=result_df,
                    storage_config=config.output.valid_data,
                    mode="append",
                    source_table_name=config.table_name,
                    source_layer=config.layer,
                )
            except Exception as e:
                logger.error(f"Failed to write valid data: {str(e)}")
                write_errors.append(f"valid_data: {str(e)}")

        # Write quarantine data (per-table paths)
        if quarantine_df is not None and config.output.quarantine_data:
            try:
                logger.info(f"Writing quarantine data to {config.output.quarantine_data.catalog}.{config.output.quarantine_data.schema_name}.{config.output.quarantine_data.table}")
                self.result_writer.write_dataframe(
                    df=quarantine_df,
                    storage_config=config.output.quarantine_data,
                    mode="append",
                    partition_cols=None,  # Avoid issues with non-existent columns
                    source_table_name=config.table_name,
                    source_layer=config.layer,
                )
            except Exception as e:
                logger.error(f"Failed to write quarantine data: {str(e)}")
                write_errors.append(f"quarantine_data: {str(e)}")

        # Write metrics
        if config.output.metrics:
            try:
                logger.info(f"Writing metrics to {config.output.metrics.catalog}.{config.output.metrics.schema_name}.{config.output.metrics.table}")
                self.result_writer.write_metrics(
                    metrics_df=metrics_df,
                    storage_config=config.output.metrics,
                    source_table_name=config.table_name,
                    source_layer=config.layer,
                )
            except Exception as e:
                logger.error(f"Failed to write metrics: {str(e)}")
                write_errors.append(f"metrics: {str(e)}")

        # Write summary
        if config.output.summary:
            try:
                logger.info(f"Writing summary to {config.output.summary.catalog}.{config.output.summary.schema_name}.{config.output.summary.table}")
                self.result_writer.write_summary(
                    summary_df=summary_df,
                    storage_config=config.output.summary,
                    source_table_name=config.table_name,
                    source_layer=config.layer,
                )
            except Exception as e:
                logger.error(f"Failed to write summary: {str(e)}")
                write_errors.append(f"summary: {str(e)}")
        
        # Report all write errors at the end
        if write_errors:
            error_summary = "; ".join(write_errors)
            raise DQAcceleratorError(f"Write errors occurred: {error_summary}")
