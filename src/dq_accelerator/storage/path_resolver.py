"""Path resolver for multi-cloud storage using Unity Catalog."""

from typing import Optional
from urllib.parse import urlparse

from dq_accelerator.config.schemas import StorageConfig
from dq_accelerator.utils.exceptions import StorageError
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class PathResolver:
    """
    Resolves storage paths using Unity Catalog abstractions.

    This class handles multi-cloud storage access through Unity Catalog
    external locations and storage credentials, eliminating the need for
    hardcoded cloud provider credentials.
    
    Supports dynamic path substitution using placeholders:
    - {table_name}: Source table name
    - {layer}: Layer name (bronze, silver, gold)
    """

    @staticmethod
    def _apply_substitutions(
        path: str,
        table_name: str,
        layer: str,
    ) -> str:
        """
        Apply placeholder substitutions to path.

        Args:
            path: Path with placeholders
            table_name: Table name for substitution
            layer: Layer name for substitution

        Returns:
            Path with substitutions applied
        """
        return path.format(table_name=table_name, layer=layer)

    @staticmethod
    def resolve_table_path(
        storage_config: StorageConfig,
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> str:
        """
        Resolve Unity Catalog table path with dynamic substitution.

        Args:
            storage_config: Storage configuration
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation

        Returns:
            Fully qualified table name (catalog.schema.table)

        Raises:
            StorageError: If table configuration is invalid
        """
        if not storage_config.table:
            raise StorageError("Table name not specified in storage config")

        table_name = storage_config.table

        # Apply substitutions if placeholders exist
        if source_table_name and "{table_name}" in table_name:
            table_name = PathResolver._apply_substitutions(
                table_name, source_table_name, source_layer or ""
            )

        # Append source table name as suffix if configured
        if storage_config.use_table_name_suffix and source_table_name:
            table_name = f"{table_name}_{source_table_name}"

        table_path = f"{storage_config.catalog}.{storage_config.schema_name}.{table_name}"
        logger.debug(f"Resolved table path: {table_path}")
        return table_path

    @staticmethod
    def resolve_external_location(
        storage_config: StorageConfig,
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> str:
        """
        Resolve external location path with dynamic substitution.

        External locations use Unity Catalog storage credentials for authentication,
        supporting Azure (ADLS Gen2), AWS (S3), and GCP (GCS) transparently.

        Args:
            storage_config: Storage configuration
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation

        Returns:
            External location path

        Raises:
            StorageError: If external location is not specified
        """
        if not storage_config.external_location:
            raise StorageError("External location not specified in storage config")

        location = storage_config.external_location

        # Apply substitutions if placeholders exist
        if source_table_name:
            location = PathResolver._apply_substitutions(
                location, source_table_name, source_layer or ""
            )

        # Append source table name as suffix if configured
        if storage_config.use_table_name_suffix and source_table_name:
            # Ensure path ends with /
            if not location.endswith("/"):
                location += "/"
            location = f"{location}{source_table_name}/"

        logger.debug(f"Resolved external location: {location}")
        return location

    @staticmethod
    def resolve_volume_path(
        storage_config: StorageConfig,
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> str:
        """
        Resolve Unity Catalog volume path with dynamic substitution.

        Args:
            storage_config: Storage configuration
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation

        Returns:
            Volume path (catalog.schema.volume)

        Raises:
            StorageError: If volume configuration is invalid
        """
        if not storage_config.volume:
            raise StorageError("Volume not specified in storage config")

        volume_name = storage_config.volume

        # Apply substitutions if placeholders exist
        if source_table_name:
            volume_name = PathResolver._apply_substitutions(
                volume_name, source_table_name, source_layer or ""
            )

        volume_path = f"/Volumes/{storage_config.catalog}/{storage_config.schema_name}/{volume_name}"

        # Append source table name as suffix if configured
        if storage_config.use_table_name_suffix and source_table_name:
            if not volume_path.endswith("/"):
                volume_path += "/"
            volume_path = f"{volume_path}{source_table_name}/"

        logger.debug(f"Resolved volume path: {volume_path}")
        return volume_path

    @staticmethod
    def resolve_storage_path(
        storage_config: StorageConfig,
        source_table_name: Optional[str] = None,
        source_layer: Optional[str] = None,
    ) -> str:
        """
        Resolve storage path from configuration with dynamic substitution.

        Automatically determines the appropriate path type (table, external location, or volume).

        Args:
            storage_config: Storage configuration
            source_table_name: Source table name for dynamic path generation
            source_layer: Source layer name for dynamic path generation

        Returns:
            Resolved storage path

        Raises:
            StorageError: If no valid storage path can be resolved
        """
        if storage_config.table:
            return PathResolver.resolve_table_path(
                storage_config, source_table_name, source_layer
            )
        elif storage_config.external_location:
            return PathResolver.resolve_external_location(
                storage_config, source_table_name, source_layer
            )
        elif storage_config.volume:
            return PathResolver.resolve_volume_path(
                storage_config, source_table_name, source_layer
            )
        else:
            raise StorageError(
                "Storage config must specify either table, external_location, or volume"
            )

    @staticmethod
    def get_cloud_provider(path: str) -> str:
        """
        Detect cloud provider from storage path.

        Args:
            path: Storage path

        Returns:
            Cloud provider name (azure, aws, gcp, or unity_catalog)
        """
        parsed = urlparse(path)
        scheme = parsed.scheme.lower()

        provider_map = {
            "abfss": "azure",
            "abfs": "azure",
            "wasbs": "azure",
            "s3": "aws",
            "s3a": "aws",
            "gs": "gcp",
        }

        provider = provider_map.get(scheme, "unity_catalog")
        logger.debug(f"Detected cloud provider: {provider} from path: {path}")
        return provider
