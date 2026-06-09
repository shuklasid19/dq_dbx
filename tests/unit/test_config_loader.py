"""Unit tests for configuration loader."""

import pytest
import tempfile
from pathlib import Path

from dq_accelerator.config.loader import ConfigLoader
from dq_accelerator.config.schemas import LayerConfig, TableConfig
from dq_accelerator.utils.exceptions import ConfigError


class TestConfigLoader:
    """Tests for ConfigLoader class."""

    def test_load_layer_config_valid(self, tmp_path):
        """Test loading valid layer configuration."""
        config_content = """
layer_name: bronze
execution_mode: continue
log_level: INFO

output:
  quarantine_data:
    catalog: main
    schema: dq_quarantine
    table: bronze_quarantine
"""
        config_file = tmp_path / "layer_config.yaml"
        config_file.write_text(config_content)

        layer_config = ConfigLoader.load_layer_config(str(config_file))

        assert isinstance(layer_config, LayerConfig)
        assert layer_config.layer_name == "bronze"
        assert layer_config.execution_mode == "continue"

    def test_load_table_config_valid(self, tmp_path):
        """Test loading valid table configuration."""
        config_content = """
table_name: customers
layer: bronze

source:
  catalog: main
  schema: raw
  table: raw_customers

checks:
  - criticality: error
    check:
      function: is_not_null
      arguments:
        column: customer_id
"""
        config_file = tmp_path / "table_config.yaml"
        config_file.write_text(config_content)

        table_config = ConfigLoader.load_table_config(str(config_file))

        assert isinstance(table_config, TableConfig)
        assert table_config.table_name == "customers"
        assert table_config.layer == "bronze"
        assert len(table_config.checks) == 1

    def test_load_yaml_file_not_found(self):
        """Test error when config file doesn't exist."""
        with pytest.raises(ConfigError, match="Configuration file not found"):
            ConfigLoader.load_layer_config("nonexistent.yaml")

    def test_load_yaml_invalid_yaml(self, tmp_path):
        """Test error when YAML syntax is invalid."""
        config_file = tmp_path / "invalid.yaml"
        # Write invalid YAML (unclosed bracket, improper indentation, etc.)
        config_file.write_text("""
layer_name: bronze
execution_mode: [unclosed
  bad_indent:
    - item
""")

        with pytest.raises(ConfigError, match="Invalid YAML syntax"):
            ConfigLoader.load_layer_config(str(config_file))

    def test_load_yaml_empty_file(self, tmp_path):
        """Test error when YAML file is empty."""
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        with pytest.raises(ConfigError, match="Empty configuration file"):
            ConfigLoader.load_layer_config(str(config_file))

    def test_load_yaml_not_dict(self, tmp_path):
        """Test error when YAML root is not a dictionary."""
        config_file = tmp_path / "list.yaml"
        config_file.write_text("""
- item1
- item2
- item3
""")

        with pytest.raises(ConfigError, match="Configuration must be a dictionary"):
            ConfigLoader.load_layer_config(str(config_file))

    def test_load_layer_config_missing_required_field(self, tmp_path):
        """Test error when required field is missing."""
        config_content = """
execution_mode: continue
log_level: INFO
"""
        config_file = tmp_path / "incomplete.yaml"
        config_file.write_text(config_content)

        with pytest.raises(ConfigError, match="Error parsing layer configuration"):
            ConfigLoader.load_layer_config(str(config_file))

    def test_load_table_config_invalid_check_structure(self, tmp_path):
        """Test error when check structure is invalid."""
        config_content = """
table_name: customers
layer: bronze

source:
  catalog: main
  schema: raw
  table: raw_customers

checks:
  - criticality: error
    # Missing 'check' field
    arguments:
      column: customer_id
"""
        config_file = tmp_path / "bad_check.yaml"
        config_file.write_text(config_content)

        with pytest.raises(ConfigError):
            ConfigLoader.load_table_config(str(config_file))
