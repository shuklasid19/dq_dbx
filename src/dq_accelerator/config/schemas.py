"""Pydantic models for configuration validation."""

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, field_validator


class CheckConfig(BaseModel):
    """Configuration for a single DQ check."""

    function: str = Field(..., description="Check function name")
    arguments: Dict[str, Any] = Field(default_factory=dict, description="Function arguments")


class DQRuleConfig(BaseModel):
    """Configuration for a DQ rule."""

    criticality: str = Field(..., description="Criticality level: error or warn")
    check: CheckConfig = Field(..., description="Check configuration")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")

    @field_validator("criticality")
    @classmethod
    def validate_criticality(cls, v: str) -> str:
        """Validate criticality value."""
        if v not in ["error", "warn", "warning"]:
            raise ValueError(f"Criticality must be 'error' or 'warn', got: {v}")
        return "warn" if v == "warning" else v


class CustomCheckConfig(BaseModel):
    """Configuration for custom check functions."""

    name: str = Field(..., description="Custom check name")
    module_path: Optional[str] = Field(default=None, description="Python module path")
    function_name: Optional[str] = Field(default=None, description="Function name in module")


class StorageConfig(BaseModel):
    """Storage configuration using Unity Catalog."""

    catalog: str = Field(..., description="Unity Catalog name")
    schema_name: str = Field(..., alias="schema", description="Schema name")
    table: Optional[str] = Field(default=None, description="Table name (supports {table_name} placeholder)")
    external_location: Optional[str] = Field(
        default=None, 
        description="External location path (supports {table_name}, {layer} placeholders)"
    )
    volume: Optional[str] = Field(default=None, description="Unity Catalog volume path")
    use_table_name_suffix: bool = Field(
        default=False, 
        description="If True, append source table name to table/path"
    )

    class Config:
        populate_by_name = True


class OutputConfig(BaseModel):
    """Output configuration for DQ results."""

    valid_data: Optional[StorageConfig] = Field(default=None, description="Valid data output")
    quarantine_data: Optional[StorageConfig] = Field(default=None, description="Quarantine data output")
    metrics: Optional[StorageConfig] = Field(default=None, description="Metrics output")
    summary: Optional[StorageConfig] = Field(default=None, description="Summary output")


class LayerConfig(BaseModel):
    """Layer-level configuration (bronze, silver, gold)."""

    layer_name: str = Field(..., description="Layer name (bronze, silver, gold)")
    output: Optional[OutputConfig] = Field(default=None, description="Default output configuration")
    custom_checks: Optional[List[CustomCheckConfig]] = Field(
        default=None, description="Layer-level custom checks"
    )
    execution_mode: str = Field(default="fail_fast", description="Execution mode: fail_fast or continue")
    log_level: str = Field(default="INFO", description="Logging level")


class TableConfig(BaseModel):
    """Table-level configuration."""

    table_name: str = Field(..., description="Table name")
    layer: str = Field(..., description="Layer name (bronze, silver, gold)")
    source: Optional[StorageConfig] = Field(
        default=None, 
        description="Source configuration (optional if table already exists)"
    )
    output: Optional[OutputConfig] = Field(default=None, description="Table-specific output configuration")
    checks: List[DQRuleConfig] = Field(default_factory=list, description="DQ rules for this table")
    custom_checks: Optional[List[CustomCheckConfig]] = Field(
        default=None, description="Table-specific custom checks"
    )
    custom_checks_module: Optional[str] = Field(
        default=None, 
        description="Path to Python module containing custom check functions (e.g., /path/to/custom_checks.py)"
    )
    enabled: bool = Field(default=True, description="Enable/disable checks for this table")


class DQConfig(BaseModel):
    """Complete DQ configuration after merging layer and table configs."""

    table_name: str
    layer: str
    source: Optional[StorageConfig] = None
    output: OutputConfig
    checks: List[DQRuleConfig]
    custom_checks: List[CustomCheckConfig]
    custom_checks_module: Optional[str] = None
    execution_mode: str
    log_level: str
    enabled: bool
