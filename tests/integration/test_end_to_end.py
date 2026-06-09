"""End-to-end integration tests."""

import pytest
import tempfile
from pathlib import Path

from dq_accelerator.engine.dq_engine import DQAccelerator
from dq_accelerator.config.schemas import TableConfig, StorageConfig, DQRuleConfig, CheckConfig
import pyspark.sql.functions as F


@pytest.mark.integration
def test_end_to_end_workflow(spark, use_mock_dqx):
    """Test complete DQ workflow from config to results."""

    # Create sample data
    data = [
        (1, "john@example.com", "John", "123-456-7890"),
        (2, "jane@example.com", "Jane", "987-654-3210"),
        (3, "invalid", "Bob", "555"),  # Invalid email and phone
        (None, "alice@example.com", "Alice", "111-222-3333"),  # Null ID
    ]

    df = spark.createDataFrame(data, ["id", "email", "name", "phone"])

    # Create table config
    table_config = TableConfig(
        table_name="test_table",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="test"),
        checks=[
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="is_not_null",
                    arguments={"column": "id"}
                )
            ),
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="email_format_validator",
                    arguments={"column": "email"}
                )
            ),
        ]
    )

    # Initialize accelerator (will use MockDQEngine due to use_mock_dqx fixture)
    accelerator = DQAccelerator(spark=spark)

    # Run checks
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        layer_config=None,
        split_valid_invalid=True,
        write_results=False,
    )

    # Validate results
    assert "result" in results
    assert "quarantine" in results
    assert "metrics" in results
    assert "summary" in results

    # Check that one row was quarantined (null ID)
    quarantine_count = results["quarantine"].count()
    assert quarantine_count == 2, f"Expected 2 quarantined row, got {quarantine_count}"

    # Check that valid data has 3 rows
    valid_count = results["result"].count()
    assert valid_count == 3, f"Expected 3 valid rows, got {valid_count}"

    # Check summary
    summary = results["summary"].first()
    assert summary["table_name"] == "test_table"
    assert summary["total_rows"] == 4
    assert summary["error_rows"] == 1


@pytest.mark.integration
def test_custom_check_loading(spark, use_mock_dqx):
    """Test loading and executing custom checks."""

    # Create custom check module
    custom_check_code = """
from pyspark.sql import Column
import pyspark.sql.functions as F

def starts_with_uppercase(column: str) -> Column:
    col_expr = F.col(column)
    condition = col_expr.isNull() | (col_expr.substr(1, 1) == F.upper(col_expr.substr(1, 1)))
    return condition
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(custom_check_code)
        custom_module_path = f.name

    try:
        # Create data
        df = spark.createDataFrame([("John",), ("alice",), ("Bob",)], ["name"])

        # Create table config with custom check
        table_config = TableConfig(
            table_name="test_custom",
            layer="bronze",
            source=StorageConfig(catalog="main", schema_name="raw", table="test"),
            checks=[
                DQRuleConfig(
                    criticality="warn",
                    check=CheckConfig(
                        function="starts_with_uppercase",
                        arguments={"column": "name"}
                    )
                )
            ],
            custom_checks=[
                {"name": "starts_with_uppercase", "module_path": custom_module_path}
            ]
        )

        # Initialize accelerator
        accelerator = DQAccelerator(spark=spark)

        # Run checks
        results = accelerator.run_checks(
            df=df,
            table_config=table_config,
            split_valid_invalid=False,
            write_results=False,
        )

        # Check that warning was generated for "alice"
        result_df = results["result"]
        assert "_warnings" in result_df.columns

        # One row should have warning (alice)
        warning_count = result_df.filter(F.size(F.col("_warnings")) > 0).count()
        assert warning_count == 1, f"Expected 1 warning, got {warning_count}"

    finally:
        Path(custom_module_path).unlink()
