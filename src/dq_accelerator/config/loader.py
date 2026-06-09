"""Configuration loader for YAML files."""

import yaml
from pathlib import Path
from typing import Any, Dict

from dq_accelerator.config.schemas import LayerConfig, TableConfig
from dq_accelerator.utils.exceptions import ConfigError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class ConfigLoader:
    """Loads and validates DQ configuration from YAML files."""

    @staticmethod
    def _load_yaml(file_path: str) -> Dict[str, Any]:
        """
        Load YAML file and return as dictionary.

        Args:
            file_path: Path to YAML file

        Returns:
            Dictionary containing YAML content

        Raises:
            ConfigError: If file cannot be loaded or parsed
        """
        try:
            path = Path(file_path)
            if not path.exists():
                raise ConfigError(f"Configuration file not found: {file_path}")

            with open(path, "r") as f:
                try:
                    config_dict = yaml.safe_load(f)
                except yaml.YAMLError as e:
                    raise ConfigError(f"Invalid YAML syntax in {file_path}: {str(e)}") from e
                
                if config_dict is None:
                    raise ConfigError(f"Empty configuration file: {file_path}")
                
                if not isinstance(config_dict, dict):
                    raise ConfigError(f"Configuration must be a dictionary, got {type(config_dict)}: {file_path}")
                
                return config_dict

        except ConfigError:
            raise
        except Exception as e:
            raise ConfigError(f"Error loading configuration from {file_path}: {str(e)}") from e

    @staticmethod
    def load_layer_config(file_path: str) -> LayerConfig:
        """
        Load layer configuration from YAML file.

        Args:
            file_path: Path to layer configuration YAML file

        Returns:
            LayerConfig object

        Raises:
            ConfigError: If configuration cannot be loaded or validated
        """
        try:
            config_dict = ConfigLoader._load_yaml(file_path)
            layer_config = LayerConfig(**config_dict)
            logger.info(f"Successfully loaded layer config from: {file_path}")
            return layer_config
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"Error parsing layer configuration: {str(e)}") from e

    @staticmethod
    def load_table_config(file_path: str) -> TableConfig:
        """
        Load table configuration from YAML file.

        Args:
            file_path: Path to table configuration YAML file

        Returns:
            TableConfig object

        Raises:
            ConfigError: If configuration cannot be loaded or validated
        """
        try:
            config_dict = ConfigLoader._load_yaml(file_path)
            table_config = TableConfig(**config_dict)
            logger.info(f"Successfully loaded table config from: {file_path}")
            return table_config
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"Error parsing table configuration: {str(e)}") from e
