"""Result writer for persisting DQ results to storage."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from dq_accelerator.config.schemas import StorageConfig
from dq_accelerator.storage.path_resolver import PathResolver
from dq_accelerator.utils.exceptions import StorageError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class ResultWriter:
    """Writes DQ results to various storage locations."""

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialize result writer.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.path_resolver = PathResolver()

    def _table_exists(self, table_path: str) -> bool:
        """Check if a Unity Catalog table exists."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {table_path}")
            return True
        except Exception:
            return False

    def write_dataframe(
        self,
        df: DataFrame,
        storage_config: Optional[StorageConfig],
        mode: str = "overwrite",
        partition_cols: Optional[list[str]] = None,
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> None:
        """
        Write DataFrame to storage location with dynamic path resolution.

        Args:
            df: DataFrame to write
            storage_config: Storage configuration
            mode: Write mode (overwrite, append, etc.)
            partition_cols: Optional partition columns
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation

        Raises:
            StorageError: If write operation fails
        """
        if storage_config is None:
            logger.warning("No storage config provided, skipping write")
            return

        try:
            path = self.path_resolver.resolve_storage_path(
                storage_config, source_table_name, source_layer
            )

            if storage_config.table:
                # Writing to Unity Catalog table
                logger.info(f"Writing to Unity Catalog table: {path}")
                
                # Check if table exists - if appending to existing table, don't specify partitionBy
                # to avoid partition mismatch errors
                table_exists = self._table_exists(path)
                
                writer = df.write.mode(mode).format("delta")

                # Enable schema merge for flexibility
                writer = writer.option("mergeSchema", "true")

                # Only apply partitionBy if table doesn't exist yet (creating new table)
                # For existing tables, Delta will use the existing partition scheme
                if partition_cols and not table_exists:
                    logger.info(f"Creating new table with partitions: {partition_cols}")
                    writer = writer.partitionBy(*partition_cols)
                elif partition_cols and table_exists:
                    logger.debug(f"Appending to existing table, using existing partition scheme")

                writer.saveAsTable(path)
                logger.info(f"Successfully wrote to table: {path}")

            elif storage_config.external_location or storage_config.volume:
                # Writing to external location or volume
                logger.info(f"Writing to external storage: {path}")
                writer = df.write.mode(mode).format("delta")
                
                # Enable schema merge
                writer = writer.option("mergeSchema", "true")

                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)

                writer.save(path)
                logger.info(f"Successfully wrote to external storage: {path}")

            else:
                raise StorageError("Invalid storage configuration")

        except Exception as e:
            raise StorageError(f"Failed to write DataFrame to storage: {str(e)}") from e

    def write_metrics(
        self,
        metrics_df: DataFrame,
        storage_config: Optional[StorageConfig],
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> None:
        """
        Write metrics DataFrame to storage (typically in append mode).

        Args:
            metrics_df: Metrics DataFrame
            storage_config: Storage configuration for metrics
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation
        """
        if storage_config:
            partition_cols = ["execution_date", "layer", "table_name"]
            self.write_dataframe(
                df=metrics_df,
                storage_config=storage_config,
                mode="append",
                partition_cols=partition_cols,
                source_table_name=source_table_name,
                source_layer=source_layer,
            )

    def write_summary(
        self,
        summary_df: DataFrame,
        storage_config: Optional[StorageConfig],
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> None:
        """
        Write summary DataFrame to storage.

        Args:
            summary_df: Summary DataFrame
            storage_config: Storage configuration for summary
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation
        """
        if storage_config:
            partition_cols = ["execution_date"]
            self.write_dataframe(
                df=summary_df,
                storage_config=storage_config,
                mode="append",
                partition_cols=partition_cols,
                source_table_name=source_table_name,
                source_layer=source_layer,
            )
