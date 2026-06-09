"""Integration tests for per-table quarantine segregation."""

import pytest
from unittest.mock import patch, call
import pyspark.sql.functions as F

from dq_accelerator.engine.dq_engine import DQAccelerator
from dq_accelerator.config.schemas import (
    TableConfig,
    LayerConfig,
    StorageConfig,
    OutputConfig,
    DQRuleConfig,
    CheckConfig,
)


@pytest.mark.integration
def test_separate_quarantine_tables_per_source(spark, use_mock_dqx):
    """Test that different source tables write to separate quarantine tables."""
    
    # Initialize accelerator
    dq = DQAccelerator(spark=spark, enable_workspace_features=False)
    
    # Layer config with dynamic table naming
    layer_config = LayerConfig(
        layer_name="bronze",
        execution_mode="continue",
        log_level="INFO",
        output=OutputConfig(
            quarantine_data=StorageConfig(
                catalog="main",
                schema_name="dq_quarantine",
                table="bronze_quarantine",
                use_table_name_suffix=True  # Creates table per source
            )
        )
    )
    
    # Test with customers table
    customers_data = [
        (1, "john@example.com"),
        (None, "invalid"),  # Should be quarantined
    ]
    customers_df = spark.createDataFrame(customers_data, ["id", "email"])
    
    customers_config = TableConfig(
        table_name="customers",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="customers"),
        checks=[
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(function="is_not_null", arguments={"column": "id"})
            )
        ]
    )
    
    with patch.object(dq.result_writer, 'write_dataframe') as mock_write:
        dq.run_checks(
            df=customers_df,
            table_config=customers_config,
            layer_config=layer_config,
            split_valid_invalid=True,
            write_results=True
        )
        
        # Verify write_dataframe was called
        assert mock_write.called, "write_dataframe should have been called"
        
        # Check that at least one call had source_table_name='customers'
        customers_calls = [
            c for c in mock_write.call_args_list
            if len(c) > 1 and c.kwargs.get('source_table_name') == 'customers'
        ]
        assert len(customers_calls) > 0, "Should have calls with source_table_name='customers'"
    
    # Test with orders table (different schema)
    orders_data = [
        (1, 100.50, "2024-01-15"),
        (None, 200.00, "2024-01-16"),  # Should be quarantined
    ]
    orders_df = spark.createDataFrame(orders_data, ["order_id", "amount", "order_date"])
    
    orders_config = TableConfig(
        table_name="orders",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="orders"),
        checks=[
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(function="is_not_null", arguments={"column": "order_id"})
            )
        ]
    )
    
    with patch.object(dq.result_writer, 'write_dataframe') as mock_write:
        dq.run_checks(
            df=orders_df,
            table_config=orders_config,
            layer_config=layer_config,
            split_valid_invalid=True,
            write_results=True
        )
        
        # Verify write_dataframe was called
        assert mock_write.called, "write_dataframe should have been called"
        
        # Check that at least one call had source_table_name='orders'
        orders_calls = [
            c for c in mock_write.call_args_list
            if len(c) > 1 and c.kwargs.get('source_table_name') == 'orders'
        ]
        assert len(orders_calls) > 0, "Should have calls with source_table_name='orders'"


@pytest.mark.integration
def test_external_location_per_table(spark, use_mock_dqx):
    """Test quarantine writes to external location with per-table paths."""
    
    dq = DQAccelerator(spark=spark, enable_workspace_features=False)
    
    # Layer config with external location and placeholders
    layer_config = LayerConfig(
        layer_name="bronze",
        execution_mode="continue",
        log_level="INFO",
        output=OutputConfig(
            quarantine_data=StorageConfig(
                catalog="main",
                schema_name="dq_quarantine",
                external_location="abfss://quarantine@datalake.dfs.core.windows.net/{layer}/{table_name}/"
            )
        )
    )
    
    # Create test data
    data = [(1, "test"), (None, "invalid")]
    df = spark.createDataFrame(data, ["id", "value"])
    
    table_config = TableConfig(
        table_name="products",
        layer="bronze",
        source=StorageConfig(catalog="main", schema_name="raw", table="products"),
        checks=[
            DQRuleConfig(
                criticality="error",
                check=CheckConfig(function="is_not_null", arguments={"column": "id"})
            )
        ]
    )
    
    with patch.object(dq.result_writer, 'write_dataframe') as mock_write:
        dq.run_checks(
            df=df,
            table_config=table_config,
            layer_config=layer_config,
            split_valid_invalid=True,
            write_results=True
        )
        
        # Verify write_dataframe was called
        assert mock_write.called, "write_dataframe should have been called"
        
        # Find calls with source_table_name='products'
        product_calls = [
            c for c in mock_write.call_args_list
            if len(c) > 1 and c.kwargs.get('source_table_name') == 'products'
        ]
        
        assert len(product_calls) > 0, "Should have calls with source_table_name='products'"
        
        # Check that at least one call has external_location config
        has_external_location = False
        for call_obj in product_calls:
            # Check both positional args and kwargs
            if len(call_obj.args) > 1:
                storage_config = call_obj.args[1]
                if hasattr(storage_config, 'external_location') and storage_config.external_location:
                    has_external_location = True
                    # Verify placeholder exists or was substituted
                    assert "{table_name}" in storage_config.external_location or \
                           "products" in storage_config.external_location, \
                           f"External location should contain placeholder or substituted value: {storage_config.external_location}"
                    break
        
        # If no positional args, check kwargs
        if not has_external_location:
            for call_obj in product_calls:
                storage_config = call_obj.kwargs.get('storage_config')
                if storage_config and hasattr(storage_config, 'external_location') and storage_config.external_location:
                    has_external_location = True
                    assert "{table_name}" in storage_config.external_location or \
                           "products" in storage_config.external_location
                    break
        
        assert has_external_location, "At least one call should have external_location configured"
