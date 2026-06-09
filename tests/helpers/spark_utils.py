"""Spark utilities for testing."""

from typing import Optional
from pyspark.sql import SparkSession


class TestSchemaManager:
    """Manages test schemas for Spark testing."""
    
    def __init__(self, spark: SparkSession, use_unity_catalog: bool = False):
        """
        Initialize test schema manager.
        
        Args:
            spark: SparkSession instance
            use_unity_catalog: If True, use Unity Catalog format (catalog.schema)
        """
        self.spark = spark
        self.use_unity_catalog = use_unity_catalog
        self._created_schemas = []
    
    def create_test_schema(self, schema_name: str, catalog: Optional[str] = None) -> str:
        """
        Create a test schema.
        
        Args:
            schema_name: Schema name
            catalog: Optional catalog name (for Unity Catalog)
            
        Returns:
            Full schema path (catalog.schema or just schema)
        """
        if self.use_unity_catalog and catalog:
            full_schema = f"{catalog}.{schema_name}"
        else:
            full_schema = schema_name
        
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
        self._created_schemas.append(full_schema)
        
        return full_schema
    
    def cleanup_all(self):
        """Drop all created test schemas."""
        for schema in self._created_schemas:
            try:
                self.spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            except Exception as e:
                print(f"Warning: Failed to drop schema {schema}: {e}")
        
        self._created_schemas.clear()
    
    def get_full_table_name(self, schema: str, table: str, catalog: Optional[str] = None) -> str:
        """
        Get full table name based on catalog support.
        
        Args:
            schema: Schema name
            table: Table name
            catalog: Optional catalog name
            
        Returns:
            Full table path
        """
        if self.use_unity_catalog and catalog:
            return f"{catalog}.{schema}.{table}"
        else:
            return f"{schema}.{table}"
