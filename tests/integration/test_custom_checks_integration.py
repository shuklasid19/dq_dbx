"""Integration tests for custom checks with DQ Engine."""

import pytest
import tempfile
from pathlib import Path

from dq_accelerator.engine.dq_engine import DQAccelerator
from dq_accelerator.config.schemas import (
    TableConfig,
    StorageConfig,
    DQRuleConfig,
    CheckConfig,
    CustomCheckConfig,
)
import pyspark.sql.functions as F


@pytest.mark.integration
def test_custom_checks_with_yaml_config(spark, tmp_path, use_mock_dqx):
    """Test custom checks loaded from YAML configuration."""
    
    # Create custom check module
    custom_module_content = """
from pyspark.sql import Column
import pyspark.sql.functions as F

def age_validator(column: str, min_age: int = 18) -> Column:
    col_expr = F.col(column)
    return col_expr.isNull() | (col_expr >= min_age)
"""
    module_file = tmp_path / "age_checks.py"
    module_file.write_text(custom_module_content)
    
    # Create test data
    data = [
        (1, "John", 25),
        (2, "Jane", 30),
        (3, "Bob", 15),  # Below minimum age
        (4, "Alice", 45),
    ]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    
    # Create table config with custom check
    table_config = TableConfig(
        table_name="users",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="users"),
        custom_checks=[
            CustomCheckConfig(
                name="age_validator",
                module_path=str(module_file),
                function_name="age_validator"
            )
        ],
        checks=[
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="age_validator",
                    arguments={"column": "age", "min_age": 18}
                )
            )
        ]
    )
    
    # Run DQ checks WITHOUT mock workspace client
    accelerator = DQAccelerator(spark=spark, enable_workspace_features=False)
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        split_valid_invalid=False,
        write_results=False,
    )
    
    # Verify results
    assert "result" in results
    assert "_warnings" in results["result"].columns
    
    # Check that one row has warning (Bob with age 15)
    warning_count = results["result"].filter(
        F.size(F.col("_warnings")) > 0
    ).count()
    assert warning_count == 1

@pytest.mark.integration
def test_builtin_custom_checks(spark, use_mock_dqx):
    """Test built-in custom checks without module loading."""
    
    # Create test data with various validation scenarios
    data = [
        (1, "john@example.com", "555-123-4567", "2023-01-15"),
        (2, "invalid-email", "123", "2023-02-20"),
        (3, "jane@test.com", "(555) 987-6543", "invalid-date"),
        (None, "bob@example.com", "555-111-2222", "2023-03-10"),  # NULL id
    ]
    df = spark.createDataFrame(data, ["id", "email", "phone", "date_str"])
    
    # Create table config with multiple built-in custom checks
    table_config = TableConfig(
        table_name="contacts",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="contacts"),
        checks=[
            # NULL check - this should catch the NULL id
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="is_not_null",
                    arguments={"column": "id"}
                )
            ),
            # Data type validation - validates non-null values can be cast
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="column_datatype_validator",
                    arguments={
                        "column": "id",
                        "expected_type": "int",
                        "cast_if_possible": True
                    }
                )
            ),
            # Email validation
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="email_format_validator",
                    arguments={"column": "email"}
                )
            ),
            # Phone validation
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="phone_number_validator",
                    arguments={"column": "phone", "country_code": "US"}
                )
            ),
            # Date validation
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="column_datatype_validator",
                    arguments={
                        "column": "date_str",
                        "expected_type": "date",
                        "cast_if_possible": True
                    }
                )
            ),
        ]
    )
    
    # Run DQ checks
    accelerator = DQAccelerator(spark=spark)
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        split_valid_invalid=True,
        write_results=False,
    )
    
    print(results["quarantine"].toPandas())
    print(results["result"].toPandas())
    print(results["metrics"].toPandas())
    print(results["summary"].toPandas())

    # Verify split results
    assert "result" in results
    assert "quarantine" in results
    
    # # #Row with null ID should be quarantined (error level)
    quarantine_count = results["quarantine"].count()
    assert quarantine_count == 3, f"Expected 3 quarantined row, got {quarantine_count}"
    
    # # # #Verify it's the row with null ID
    # quarantined_row = results["quarantine"].first()
    # assert quarantined_row["id"] is None, "Quarantined row should have null ID"
    
    # # Valid data should have 3 rows
    valid_count = results["result"].count()
    assert valid_count == 3, f"Expected 3 valid rows, got {valid_count}"
    
    # Check metrics
    assert "metrics" in results
    metrics_df = results["metrics"]
    assert metrics_df.count() > 0


@pytest.mark.integration
def test_string_validation_checks(spark, use_mock_dqx):
    """Test string-related custom validation checks."""
    
    data = [
        ("John", "VALID123", "short", "john.doe@example.com"),
        ("A", "abc", "this is a very long string that exceeds maximum", "invalid"),
        ("Jane", "TEST456", "medium", "jane@test.com"),
    ]
    df = spark.createDataFrame(data, ["name", "code", "description", "email"])
    
    table_config = TableConfig(
        table_name="products",
        layer="silver",
        source=StorageConfig(catalog="main", schema_name="bronze", table="products"),
        checks=[
            # Name length validation
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="string_length_range",
                    arguments={"column": "name", "min_length": 2, "max_length": 50}
                )
            ),
            # Description length validation
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="string_length_range",
                    arguments={"column": "description", "min_length": 5, "max_length": 30}
                )
            ),
            # Email validation
            DQRuleConfig(
                criticality="warn",
                check=CheckConfig(
                    function="email_format_validator",
                    arguments={"column": "email"}
                )
            ),
        ]
    )
    
    accelerator = DQAccelerator(spark=spark, enable_workspace_features=False)
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        split_valid_invalid=True,
        write_results=False,
    )

    
    # Row with name "A" should be quarantined (length < 2)
    quarantine_count = results["quarantine"].count()
    assert quarantine_count == 1
    
    # Check that warnings exist for other issues
    valid_df = results["result"]
    if "_warnings" in valid_df.columns:
        warning_rows = valid_df.filter(F.size(F.col("_warnings")) > 0).count()
        assert warning_rows >= 1


@pytest.mark.integration
def test_date_validation_checks(spark, use_mock_dqx):
    """Test date-related custom validation checks."""
    
    data = [
        ("2023-06-15", "2023-07-01"),
        ("2022-12-31", "2023-01-15"),  # Start date before range
        ("2024-01-01", "2024-02-01"),  # Start date after range
        ("2023-05-01", "2023-06-15"),
    ]
    df = spark.createDataFrame(data, ["start_date", "end_date"]).select(
        F.col("start_date").cast("date").alias("start_date"),
        F.col("end_date").cast("date").alias("end_date")
    )
    
    table_config = TableConfig(
        table_name="events",
        layer="silver",
        source=StorageConfig(catalog="main", schema_name="bronze", table="events"),
        checks=[
            # Date range validation
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="date_range_validator",
                    arguments={
                        "column": "start_date",
                        "min_date": "2023-01-01",
                        "max_date": "2023-12-31"
                    }
                )
            ),
        ]
    )
    
    accelerator = DQAccelerator(spark=spark, enable_workspace_features=False)
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        split_valid_invalid=True,
        write_results=False,
    )
    
    # Two rows should be quarantined (dates outside 2023)
    quarantine_count = results["quarantine"].count()
    assert quarantine_count == 2
    
    valid_count = results["result"].count()
    assert valid_count == 2


@pytest.mark.integration
def test_numeric_precision_validation(spark, use_mock_dqx):
    """Test numeric precision validation."""
    
    data = [
        (123.45,),
        (12.3,),
        (123456.78,),  # Exceeds precision
        (12.345,),  # Exceeds scale
    ]
    df = spark.createDataFrame(data, ["amount"])
    
    table_config = TableConfig(
        table_name="transactions",
        layer="silver",
        source=StorageConfig(catalog="main", schema_name="bronze", table="transactions"),
        checks=[
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(
                    function="numeric_precision_validator",
                    arguments={
                        "column": "amount",
                        "max_precision": 5,
                        "max_scale": 2
                    }
                )
            ),
        ]
    )
    
    accelerator = DQAccelerator(spark=spark, enable_workspace_features=False)
    results = accelerator.run_checks(
        df=df,
        table_config=table_config,
        split_valid_invalid=True,
        write_results=False,
    )
    
    # Two rows should fail precision/scale checks
    quarantine_count = results["quarantine"].count()
    assert quarantine_count == 2
    
    valid_count = results["result"].count()
    assert valid_count == 2
