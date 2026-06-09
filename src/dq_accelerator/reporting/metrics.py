"""Metrics generation for DQ checks."""

from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class MetricsGenerator:
    """Generate metrics from DQ check results."""

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialize metrics generator.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark

    def generate_metrics(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        execution_id: str,
        check_names: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Generate detailed metrics for each DQ check.

        Args:
            df: DataFrame with DQ metadata columns
            table_name: Table name
            layer: Layer name
            execution_id: Execution identifier
            check_names: Optional list of check names to extract metrics for

        Returns:
            Metrics DataFrame
        """
        logger.info(f"Generating metrics for table: {table_name}")

        metrics_data = []
        execution_time = datetime.now()

        # Extract error metrics
        if "_errors" in df.columns:
            error_metrics = self._extract_check_metrics(
                df, "_errors", "ERROR", table_name, layer, execution_id, execution_time
            )
            metrics_data.extend(error_metrics)

        # Extract warning metrics
        if "_warnings" in df.columns:
            warning_metrics = self._extract_check_metrics(
                df, "_warnings", "WARNING", table_name, layer, execution_id, execution_time
            )
            metrics_data.extend(warning_metrics)

        if not metrics_data:
            logger.info(f"No quality issues found for table: {table_name}")
            # Return empty DataFrame with schema
            return self.spark.createDataFrame([], schema=self._get_metrics_schema())

        metrics_df = self.spark.createDataFrame(metrics_data, schema=self._get_metrics_schema())

        logger.info(f"Generated {metrics_df.count()} metric records for table: {table_name}")
        return metrics_df

    def _extract_check_metrics(
        self,
        df: DataFrame,
        column_name: str,
        severity: str,
        table_name: str,
        layer: str,
        execution_id: str,
        execution_time: datetime,
    ) -> List[dict]:
        """Extract metrics from a specific metadata column (_errors or _warnings)."""
        metrics = []

        # Explode the array column and group by check details
        # The _errors/_warnings columns contain structs with 'name', 'function', 'columns' fields
        check_counts = (
            df.filter(F.size(F.col(column_name)) > 0)
            .select(F.explode(column_name).alias("check_struct"))
            .select(
                F.col("check_struct.name").alias("check_name"),
                F.col("check_struct.message").alias("check_message"),
                F.col("check_struct.function").alias("check_function"),
                F.col("check_struct.columns").alias("check_columns"),
            )
            .groupBy("check_name", "check_message", "check_function", "check_columns")
            .agg(F.count("*").alias("failure_count"))
            .collect()
        )

        total_rows = df.count()

        for row in check_counts:
            raw_check_name = row["check_name"] or ""
            check_function = row["check_function"] or "unknown"
            check_columns = row["check_columns"] or []
            check_message = row["check_message"] or f"Check failed: {check_function}"
            failure_count = row["failure_count"]
            failure_rate = float((failure_count / total_rows * 100) if total_rows > 0 else 0.0)
            
            # Extract column name(s) from the columns array
            validated_columns = ", ".join(check_columns) if check_columns else "N/A"
            
            # Format check_name nicely: "function_name (column_name)"
            # e.g., "column_datatype_validator (created_date)"
            if check_columns:
                formatted_check_name = f"{check_function} ({validated_columns})"
            else:
                formatted_check_name = check_function

            metrics.append({
                "execution_id": execution_id,
                "execution_date": execution_time,
                "table_name": table_name,
                "layer": layer,
                "check_function": check_function,
                "validated_column": validated_columns,
                "check_name": formatted_check_name,
                "severity": severity,
                "total_rows": int(total_rows),
                "failure_count": int(failure_count),
                "failure_rate": round(failure_rate, 2),
            })

        return metrics

    @staticmethod
    def _get_metrics_schema() -> StructType:
        """Get schema for metrics DataFrame."""
        return StructType([
            StructField("execution_id", StringType(), False),
            StructField("execution_date", TimestampType(), False),
            StructField("table_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("check_function", StringType(), False),
            StructField("validated_column", StringType(), False),
            StructField("check_name", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("total_rows", LongType(), False),
            StructField("failure_count", LongType(), False),
            StructField("failure_rate", DoubleType(), False),
        ])
