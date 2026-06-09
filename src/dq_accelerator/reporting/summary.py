"""Validation summary and reporting functionality."""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class ValidationSummary:
    """Generate and format validation summary from DQ check results."""

    def __init__(self, spark: SparkSession) -> None:
        """
        Initialize validation summary generator.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark

    def generate_summary(
        self,
        df: DataFrame,
        table_name: str,
        layer: str,
        execution_id: str,
    ) -> DataFrame:
        """
        Generate validation summary from DataFrame with DQ metadata columns.

        Args:
            df: DataFrame with _warnings and _errors columns
            table_name: Table name
            layer: Layer name (bronze, silver, gold)
            execution_id: Unique execution identifier

        Returns:
            Summary DataFrame
        """
        logger.info(f"Generating validation summary for table: {table_name}")

        total_rows = df.count()

        # Rows with warnings
        if "_warnings" in df.columns:
            warning_rows = df.filter(F.size(F.col("_warnings")) > 0).count()
        else:
            warning_rows = 0

        # Rows with errors
        if "_errors" in df.columns:
            error_rows = df.filter(F.size(F.col("_errors")) > 0).count()
        else:
            error_rows = 0

        valid_rows = total_rows - error_rows

        # Calculate pass rate
        pass_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0

        # Aggregate check details
        check_details = self._aggregate_check_details(df)

        summary_data = [{
            "execution_id": execution_id,
            "execution_date": datetime.now(),
            "table_name": table_name,
            "layer": layer,
            "total_rows": total_rows,
            "valid_rows": valid_rows,
            "error_rows": error_rows,
            "warning_rows": warning_rows,
            "pass_rate": round(pass_rate, 2),
            "check_details": check_details,
        }]

        summary_schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("execution_date", TimestampType(), False),
            StructField("table_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("total_rows", LongType(), False),
            StructField("valid_rows", LongType(), False),
            StructField("error_rows", LongType(), False),
            StructField("warning_rows", LongType(), False),
            StructField("pass_rate", DoubleType(), False),  # Fixed: was StringType, should be DoubleType
            StructField("check_details", ArrayType(StringType()), False),
        ])

        summary_df = self.spark.createDataFrame(summary_data, schema=summary_schema)

        logger.info(
            f"Summary for {table_name}: "
            f"Total={total_rows}, Valid={valid_rows}, "
            f"Errors={error_rows}, Warnings={warning_rows}, "
            f"Pass Rate={pass_rate:.2f}%"
        )

        return summary_df

    def _aggregate_check_details(self, df: DataFrame) -> List[str]:
        """
        Aggregate detailed check failure information.

        Args:
            df: DataFrame with DQ metadata columns

        Returns:
            List of check detail strings
        """
        check_details = []

        # Aggregate errors
        if "_errors" in df.columns:
            error_details = (
                df.filter(F.size(F.col("_errors")) > 0)
                .select(F.explode("_errors").alias("error"))
                .groupBy("error")
                .count()
                .collect()
            )

            for row in error_details:
                check_details.append(f"ERROR: {row['error']} (count: {row['count']})")

        # Aggregate warnings
        if "_warnings" in df.columns:
            warning_details = (
                df.filter(F.size(F.col("_warnings")) > 0)
                .select(F.explode("_warnings").alias("warning"))
                .groupBy("warning")
                .count()
                .collect()
            )

            for row in warning_details:
                check_details.append(f"WARNING: {row['warning']} (count: {row['count']})")

        return check_details if check_details else ["No issues detected"]

    def print_summary(self, summary_df: DataFrame) -> None:
        """
        Print formatted summary to console.

        Args:
            summary_df: Summary DataFrame
        """
        summary_row = summary_df.first()

        if summary_row:
            print("\n" + "=" * 80)
            print(f"DATA QUALITY VALIDATION SUMMARY")
            print("=" * 80)
            print(f"Execution ID:    {summary_row['execution_id']}")
            print(f"Table:           {summary_row['table_name']}")
            print(f"Layer:           {summary_row['layer']}")
            print(f"Execution Time:  {summary_row['execution_date']}")
            print("-" * 80)
            print(f"Total Rows:      {summary_row['total_rows']:,}")
            print(f"Valid Rows:      {summary_row['valid_rows']:,}")
            print(f"Error Rows:      {summary_row['error_rows']:,}")
            print(f"Warning Rows:    {summary_row['warning_rows']:,}")
            print(f"Pass Rate:       {summary_row['pass_rate']}%")
            print("-" * 80)
            print("Check Details:")
            for detail in summary_row['check_details']:
                print(f"  - {detail}")
            print("=" * 80 + "\n")
