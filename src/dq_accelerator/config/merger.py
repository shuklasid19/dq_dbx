"""Configuration merger for combining layer and table configs."""

from typing import List, Optional

from dq_accelerator.config.schemas import (
    CustomCheckConfig,
    DQConfig,
    LayerConfig,
    OutputConfig,
    TableConfig,
)
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class ConfigMerger:
    """Merges layer and table configurations with intelligent override logic."""

    @staticmethod
    def merge_output_config(
        layer_output: Optional[OutputConfig],
        table_output: Optional[OutputConfig],
    ) -> OutputConfig:
        """
        Merge output configurations with table config taking precedence.

        Args:
            layer_output: Layer-level output config
            table_output: Table-level output config

        Returns:
            Merged OutputConfig
        """
        if table_output and layer_output:
            return OutputConfig(
                valid_data=table_output.valid_data or layer_output.valid_data,
                quarantine_data=table_output.quarantine_data or layer_output.quarantine_data,
                metrics=table_output.metrics or layer_output.metrics,
                summary=table_output.summary or layer_output.summary,
            )
        return table_output or layer_output or OutputConfig()

    @staticmethod
    def merge_custom_checks(
        layer_checks: Optional[List[CustomCheckConfig]],
        table_checks: Optional[List[CustomCheckConfig]],
    ) -> List[CustomCheckConfig]:
        """
        Merge custom check configurations.

        Args:
            layer_checks: Layer-level custom checks
            table_checks: Table-level custom checks

        Returns:
            Combined list of custom checks
        """
        checks = []
        if layer_checks:
            checks.extend(layer_checks)
        if table_checks:
            checks.extend(table_checks)
        return checks

    @staticmethod
    def merge_configs(layer_config: Optional[LayerConfig], table_config: TableConfig) -> DQConfig:
        """
        Merge layer and table configurations into a single DQConfig.

        Args:
            layer_config: Layer-level configuration (optional)
            table_config: Table-level configuration

        Returns:
            Merged DQConfig object
        """
        logger.info(f"Merging configs for table: {table_config.table_name}")

        if layer_config:
            output = ConfigMerger.merge_output_config(layer_config.output, table_config.output)
            custom_checks = ConfigMerger.merge_custom_checks(
                layer_config.custom_checks,
                table_config.custom_checks,
            )
            execution_mode = table_config.execution_mode if hasattr(table_config, "execution_mode") else layer_config.execution_mode
            log_level = table_config.log_level if hasattr(table_config, "log_level") else layer_config.log_level
        else:
            output = table_config.output or OutputConfig()
            custom_checks = table_config.custom_checks or []
            execution_mode = getattr(table_config, "execution_mode", "fail_fast")
            log_level = getattr(table_config, "log_level", "INFO")

        merged_config = DQConfig(
            table_name=table_config.table_name,
            layer=table_config.layer,
            source=table_config.source,
            output=output,
            checks=table_config.checks,
            custom_checks=custom_checks,
            custom_checks_module=getattr(table_config, "custom_checks_module", None),
            execution_mode=execution_mode,
            log_level=log_level,
            enabled=table_config.enabled,
        )

        logger.info(f"Successfully merged config for table: {table_config.table_name}")
        return merged_config
