"""Unit tests for path resolver with dynamic substitution."""

import pytest

from dq_accelerator.storage.path_resolver import PathResolver
from dq_accelerator.config.schemas import StorageConfig
from dq_accelerator.utils.exceptions import StorageError


class TestPathResolver:
    """Tests for PathResolver class."""

    def test_resolve_table_path_simple(self):
        """Test simple table path resolution."""
        config = StorageConfig(
            catalog="main",
            schema_name="bronze",
            table="customers"
        )
        
        path = PathResolver.resolve_table_path(config)
        assert path == "main.bronze.customers"

    def test_resolve_table_path_with_suffix(self):
        """Test table path with suffix."""
        config = StorageConfig(
            catalog="main",
            schema_name="dq_quarantine",
            table="bronze_quarantine",
            use_table_name_suffix=True
        )
        
        path = PathResolver.resolve_table_path(
            config,
            source_table_name="customers",
            source_layer="bronze"
        )
        assert path == "main.dq_quarantine.bronze_quarantine_customers"

    def test_resolve_table_path_with_placeholder(self):
        """Test table path with placeholder substitution."""
        config = StorageConfig(
            catalog="main",
            schema_name="dq_quarantine",
            table="{layer}_quarantine_{table_name}"
        )
        
        path = PathResolver.resolve_table_path(
            config,
            source_table_name="orders",
            source_layer="silver"
        )
        assert path == "main.dq_quarantine.silver_quarantine_orders"

    def test_resolve_external_location_simple(self):
        """Test simple external location resolution."""
        config = StorageConfig(
            catalog="main",
            schema_name="bronze",
            external_location="abfss://data@storage.dfs.core.windows.net/path/"
        )
        
        path = PathResolver.resolve_external_location(config)
        assert path == "abfss://data@storage.dfs.core.windows.net/path/"

    def test_resolve_external_location_with_placeholder(self):
        """Test external location with placeholder."""
        config = StorageConfig(
            catalog="main",
            schema_name="dq_quarantine",
            external_location="abfss://quarantine@storage.dfs.core.windows.net/{layer}/{table_name}/"
        )
        
        path = PathResolver.resolve_external_location(
            config,
            source_table_name="customers",
            source_layer="bronze"
        )
        assert path == "abfss://quarantine@storage.dfs.core.windows.net/bronze/customers/"

    def test_resolve_external_location_with_suffix(self):
        """Test external location with suffix."""
        config = StorageConfig(
            catalog="main",
            schema_name="dq_quarantine",
            external_location="abfss://quarantine@storage.dfs.core.windows.net/bronze/",
            use_table_name_suffix=True
        )
        
        path = PathResolver.resolve_external_location(
            config,
            source_table_name="orders",
            source_layer="bronze"
        )
        assert path == "abfss://quarantine@storage.dfs.core.windows.net/bronze/orders/"

    def test_resolve_volume_path(self):
        """Test volume path resolution."""
        config = StorageConfig(
            catalog="main",
            schema_name="bronze",
            volume="data_volume"
        )
        
        path = PathResolver.resolve_volume_path(config)
        assert path == "/Volumes/main/bronze/data_volume"

    def test_resolve_volume_path_with_suffix(self):
        """Test volume path with table suffix."""
        config = StorageConfig(
            catalog="main",
            schema_name="dq_quarantine",
            volume="quarantine",
            use_table_name_suffix=True
        )
        
        path = PathResolver.resolve_volume_path(
            config,
            source_table_name="customers",
            source_layer="bronze"
        )
        assert path == "/Volumes/main/dq_quarantine/quarantine/customers/"

    def test_resolve_storage_path_no_config(self):
        """Test error when no storage path configured."""
        config = StorageConfig(
            catalog="main",
            schema_name="bronze"
        )
        
        with pytest.raises(StorageError, match="must specify either"):
            PathResolver.resolve_storage_path(config)

    def test_get_cloud_provider_azure(self):
        """Test Azure cloud provider detection."""
        path = "abfss://container@storage.dfs.core.windows.net/path"
        provider = PathResolver.get_cloud_provider(path)
        assert provider == "azure"

    def test_get_cloud_provider_aws(self):
        """Test AWS cloud provider detection."""
        path = "s3://bucket/path"
        provider = PathResolver.get_cloud_provider(path)
        assert provider == "aws"

    def test_get_cloud_provider_gcp(self):
        """Test GCP cloud provider detection."""
        path = "gs://bucket/path"
        provider = PathResolver.get_cloud_provider(path)
        assert provider == "gcp"

    def test_get_cloud_provider_unity_catalog(self):
        """Test Unity Catalog detection."""
        path = "main.bronze.customers"
        provider = PathResolver.get_cloud_provider(path)
        assert provider == "unity_catalog"
