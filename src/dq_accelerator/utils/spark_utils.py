"""Spark utility functions for compatibility across versions."""

from pyspark.sql import Column
import pyspark.sql.functions as F
from typing import Union


def safe_try_cast(column: Union[str, Column], data_type: str) -> Column:
    """
    Safely cast a column using try_cast, compatible with Spark Connect.
    
    Uses pattern matching and safe casting for Databricks Serverless compatibility.
    Returns NULL on invalid input instead of throwing error.
    
    IMPORTANT: For Spark Connect compatibility, always pass a STRING column name, not a Column object.
    
    Args:
        column: Column name (STRING preferred) or Column object
        data_type: Target data type as string
        
    Returns:
        Column expression with safe cast applied (returns NULL if cast fails)
    """
    col_name = column if isinstance(column, str) else None
    col_expr = F.col(column) if isinstance(column, str) else column
    
    # Map common type names to Spark SQL types
    type_mapping = {
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "long": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "string": "STRING",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "decimal": "DECIMAL(38,18)",
    }
    
    spark_type = type_mapping.get(data_type.lower(), data_type.upper())
    
    # For numeric types, use pattern-based validation that works with Spark Connect
    # This approach is more reliable than F.expr("try_cast...") in Serverless
    if spark_type in ("INT", "BIGINT", "DOUBLE", "FLOAT", "DECIMAL(38,18)"):
        # Convert to string for pattern matching (works for both string and numeric sources)
        str_val = F.trim(col_expr.cast("string"))
        
        if spark_type in ("INT", "BIGINT"):
            # Integer pattern: optional minus, followed by digits
            # Matches: 0, 1, 123, -456, 0001, etc.
            int_pattern = r"^-?[0-9]+$"
            is_valid = str_val.rlike(int_pattern)
            
            # For INT, also check range (-2147483648 to 2147483647)
            if spark_type == "INT":
                # Cast to BIGINT first (safe), then check range
                as_bigint = col_expr.cast("bigint")
                in_range = as_bigint.between(-2147483648, 2147483647)
                return F.when(
                    col_expr.isNull(), F.lit(None).cast("int")
                ).when(
                    is_valid & in_range, col_expr.cast("int")
                ).otherwise(F.lit(None).cast("int"))
            else:
                # BIGINT - just check pattern
                return F.when(
                    col_expr.isNull(), F.lit(None).cast("bigint")
                ).when(
                    is_valid, col_expr.cast("bigint")
                ).otherwise(F.lit(None).cast("bigint"))
        else:
            # Float/Double/Decimal pattern: optional minus, digits, optional decimal
            # Matches: 0, 1.5, -3.14, .5, 123.456, etc.
            numeric_pattern = r"^-?[0-9]*\.?[0-9]+$"
            is_valid = str_val.rlike(numeric_pattern)
            
            return F.when(
                col_expr.isNull(), F.lit(None).cast(spark_type.lower())
            ).when(
                is_valid, col_expr.cast(spark_type.lower())
            ).otherwise(F.lit(None).cast(spark_type.lower()))
    
    elif spark_type == "DATE":
        # to_date returns NULL on invalid input
        return F.to_date(col_expr)
    
    elif spark_type == "TIMESTAMP":
        # to_timestamp returns NULL on invalid input
        return F.to_timestamp(col_expr)
    
    elif spark_type == "BOOLEAN":
        # For booleans, check valid representations
        lower_val = F.lower(F.trim(col_expr.cast("string")))
        return F.when(
            col_expr.isNull(),
            F.lit(None).cast("boolean")
        ).when(
            lower_val.isin("true", "false", "1", "0", "yes", "no", "t", "f", "y", "n"),
            F.when(lower_val.isin("true", "1", "yes", "t", "y"), F.lit(True)).otherwise(F.lit(False))
        ).otherwise(F.lit(None).cast("boolean"))
    
    else:
        # For string and other types, regular cast works
        return col_expr.cast(spark_type)


def is_castable(column: Union[str, Column], data_type: str) -> Column:
    """
    Check if a column value can be cast to the specified data type.
    Compatible with Spark Connect (no JVM dependency).
    
    Args:
        column: Column name (STRING preferred) or Column object
        data_type: Target data type as string
        
    Returns:
        Boolean Column expression (True if castable or null, False otherwise)
    """
    col_expr = F.col(column) if isinstance(column, str) else column
    
    # IMPORTANT: Pass the original column (preserving string if it was string)
    # to safe_try_cast for best Spark Connect compatibility
    casted = safe_try_cast(column, data_type)
    
    return col_expr.isNull() | casted.isNotNull()


def safe_try_cast_with_validation(column: Union[str, Column], data_type: str) -> Column:
    """
    Cast with validation - returns the casted value only if the 
    string representation matches (strict validation).
    
    Args:
        column: Column name (STRING preferred) or Column object
        data_type: Target data type as string
        
    Returns:
        Column expression with validated cast
    """
    col_expr = F.col(column) if isinstance(column, str) else column
    
    # IMPORTANT: Pass original column for best Spark Connect compatibility
    casted = safe_try_cast(column, data_type)
    
    # For strict validation, compare string representations
    original_str = F.trim(col_expr.cast("string"))
    casted_str = F.trim(casted.cast("string"))
    
    return F.when(
        col_expr.isNull(),
        F.lit(None)
    ).when(
        casted.isNotNull() & (original_str == casted_str),
        casted
    ).otherwise(F.lit(None))
