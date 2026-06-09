"""Pytest configuration and fixtures."""

import pytest
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
import os
import sys
from pathlib import Path

# Add tests directory to path for imports
tests_dir = Path(__file__).parent
sys.path.insert(0, str(tests_dir))


@pytest.fixture(scope="session")
def spark():
    """Create SparkSession for testing."""
    # Set environment variables to avoid pickling issues
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("dq-accelerator-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Disable features that might cause serialization issues
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    yield spark

    spark.stop()


@pytest.fixture
def workspace_client():
    """
    Create a WorkspaceClient for testing.
    
    This is only used when needed for production code paths.
    Tests should use MockDQEngine instead.
    """
    try:
        # Try with minimal config
        config = Config(
            host="http://localhost",
            auth_type="anonymous"
        )
        return WorkspaceClient(config=config)
    except Exception:
        # Return None if creation fails - tests should handle this
        return None

@pytest.fixture
def test_schema_manager(spark):
    """
    Fixture for managing test schemas.
    
    Automatically cleans up schemas after test completion.
    """
    from tests.helpers.spark_utils import TestSchemaManager
    
    manager = TestSchemaManager(spark, use_unity_catalog=False)
    
    yield manager
    
    # Cleanup after test
    manager.cleanup_all()


@pytest.fixture
def use_mock_dqx(monkeypatch):
    """
    Fixture that patches DQEngine with MockDQEngine for testing.
    
    Usage:
        @pytest.mark.integration
        def test_something(spark, use_mock_dqx):
            # DQEngine will be MockDQEngine
            ...
    """
    from mocks.mock_dqx import MockDQEngine
    
    # Patch the DQEngine import in check_executor module
    monkeypatch.setattr(
        "dq_accelerator.engine.check_executor.DQEngine",
        MockDQEngine
    )
    
    return True


@pytest.fixture
def sample_dataframe(spark):
    """Create sample DataFrame for testing."""
    data = [
        (1, "john.doe@example.com", "John", "Doe", "555-123-4567", "2023-01-15"),
        (2, "jane.smith@example.com", "Jane", "Smith", "555-987-6543", "2023-02-20"),
        (3, "invalid-email", "Bob", "Johnson", "123", "2023-03-10"),
        (None, "alice@example.com", "Alice", "Williams", "555-111-2222", "invalid-date"),
    ]

    columns = ["customer_id", "email", "first_name", "last_name", "phone", "created_date"]

    return spark.createDataFrame(data, columns)

@pytest.fixture
def create_df_with_nulls(spark):
    """
    Factory fixture to create DataFrames with null values and explicit schema.
    
    Usage:
        df = create_df_with_nulls(
            [("value1",), (None,), ("value2",)],
            ["column_name"],
            [StringType()]
        )
    """
    from pyspark.sql.types import StructType, StructField
    
    def _create_df(data, column_names, column_types):
        """
        Create DataFrame with explicit schema.
        
        Args:
            data: List of tuples
            column_names: List of column names
            column_types: List of PySpark types
            
        Returns:
            DataFrame with explicit schema
        """
        fields = [
            StructField(name, col_type, True)
            for name, col_type in zip(column_names, column_types)
        ]
        schema = StructType(fields)
        return spark.createDataFrame(data, schema=schema)
    
    return _create_df
