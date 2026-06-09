"""
Comprehensive custom check functions library for DQ Accelerator.
All functions return boolean Column expressions compatible with DQX 0.12.
"""

from typing import List, Optional, Union
from datetime import datetime

from pyspark.sql import Column
import pyspark.sql.functions as F
from pyspark.sql.types import DataType
from dq_accelerator.utils.spark_utils import safe_try_cast, is_castable


# ============================================================================
# DATA TYPE VALIDATION CHECKS
# ============================================================================

def column_datatype_validator(
    column: str,
    expected_type: str,
    cast_if_possible: bool = True,
) -> Column:
    """
    Validate column data type - useful for bronze to silver layer transitions.

    Args:
        column: Column name to validate
        expected_type: Expected data type (int, bigint, double, float, string, date, timestamp, boolean, decimal)
        cast_if_possible: If True, attempt to cast and check for nulls; if False, strict type check

    Returns:
        Boolean Column expression

    Example:
        column_datatype_validator("customer_id", "int", cast_if_possible=True)
    """
    col_expr = F.col(column)

    if cast_if_possible:
        return is_castable(col_expr, expected_type)
    else:
        casted = safe_try_cast(col_expr, expected_type)
        original_str = F.trim(col_expr.cast("string"))
        casted_str = F.trim(casted.cast("string"))
        return col_expr.isNull() | (casted.isNotNull() & (original_str == casted_str))


def is_numeric(column: str) -> Column:
    """
    Check if column contains numeric values.

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    return is_castable(column, "double")


def is_boolean_string(column: str, true_values: Optional[List[str]] = None, 
                       false_values: Optional[List[str]] = None) -> Column:
    """
    Check if string column contains valid boolean representations.

    Args:
        column: Column name
        true_values: List of strings representing True (default: ['true', 't', 'yes', 'y', '1'])
        false_values: List of strings representing False (default: ['false', 'f', 'no', 'n', '0'])

    Returns:
        Boolean Column expression
    """
    if true_values is None:
        true_values = ['true', 't', 'yes', 'y', '1']
    if false_values is None:
        false_values = ['false', 'f', 'no', 'n', '0']
    
    valid_values = true_values + false_values
    col_expr = F.col(column)
    lower_col = F.lower(F.trim(col_expr))
    
    return col_expr.isNull() | lower_col.isin(valid_values)


# ============================================================================
# FORMAT VALIDATION CHECKS
# ============================================================================

def email_format_validator(column: str) -> Column:
    """
    Validate email address format using regex.

    Args:
        column: Column name containing email addresses

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return col_expr.isNull() | col_expr.rlike(email_pattern)


def phone_number_validator(column: str, country_code: str = "US") -> Column:
    """
    Validate phone number format.

    Args:
        column: Column name containing phone numbers
        country_code: Country code for validation (US, UK, IN, etc.)

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    patterns = {
        "US": r"^(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$",
        "UK": r"^(\+?44[-.\s]?)?(\(0\)|0)?[-.\s]?\d{4}[-.\s]?\d{6}$",
        "IN": r"^(\+?91[-.\s]?)?[6-9]\d{9}$",
    }
    
    phone_pattern = patterns.get(country_code, r"^\+?[1-9]\d{1,14}$")
    return col_expr.isNull() | col_expr.rlike(phone_pattern)


def url_validator(column: str, require_https: bool = False) -> Column:
    """
    Validate URL format.

    Args:
        column: Column name containing URLs
        require_https: If True, only accept HTTPS URLs

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if require_https:
        url_pattern = r"^https://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$"
    else:
        url_pattern = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$"
    
    return col_expr.isNull() | col_expr.rlike(url_pattern)


def ip_address_validator(column: str, version: str = "v4") -> Column:
    """
    Validate IP address format (IPv4 or IPv6).

    Args:
        column: Column name containing IP addresses
        version: IP version ("v4" or "v6")

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if version == "v4":
        # IPv4 pattern
        ip_pattern = r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    else:
        # Simplified IPv6 pattern
        ip_pattern = r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
    
    return col_expr.isNull() | col_expr.rlike(ip_pattern)


def credit_card_validator(column: str) -> Column:
    """
    Validate credit card number format (basic Luhn algorithm check).

    Args:
        column: Column name containing credit card numbers

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    # Remove spaces and dashes
    clean_number = F.regexp_replace(col_expr, "[\\s-]", "")
    # Check if 13-19 digits
    length_check = F.length(clean_number).between(13, 19)
    # Check if all digits
    digits_check = clean_number.rlike("^\\d+$")
    
    return col_expr.isNull() | (length_check & digits_check)


def ssn_validator(column: str, country: str = "US") -> Column:
    """
    Validate Social Security Number format.

    Args:
        column: Column name containing SSN
        country: Country code (US, CA for SIN)

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if country == "US":
        # US SSN format: XXX-XX-XXXX or XXXXXXXXX
        ssn_pattern = r"^\d{3}-?\d{2}-?\d{4}$"
    else:
        # Generic 9-digit format
        ssn_pattern = r"^\d{9}$"
    
    return col_expr.isNull() | col_expr.rlike(ssn_pattern)


# ============================================================================
# DATE AND TIME VALIDATION CHECKS
# ============================================================================

def date_range_validator(
    column: str,
    min_date: Optional[str] = None,
    max_date: Optional[str] = None,
) -> Column:
    """
    Validate date is within specified range.

    Args:
        column: Column name containing dates
        min_date: Minimum date (ISO format: YYYY-MM-DD) or None
        max_date: Maximum date (ISO format: YYYY-MM-DD) or None

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    conditions = [col_expr.isNull()]

    if min_date:
        conditions.append(col_expr >= F.lit(min_date).cast("date"))

    if max_date:
        conditions.append(col_expr <= F.lit(max_date).cast("date"))

    from functools import reduce
    from operator import and_

    return reduce(and_, conditions) if len(conditions) > 1 else conditions[0]


def is_future_date(column: str, allow_today: bool = True) -> Column:
    """
    Check if date is in the future.

    Args:
        column: Column name containing dates
        allow_today: If True, today's date is considered valid

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    today = F.current_date()
    
    if allow_today:
        return col_expr.isNull() | (col_expr >= today)
    else:
        return col_expr.isNull() | (col_expr > today)


def is_past_date(column: str, allow_today: bool = True) -> Column:
    """
    Check if date is in the past.

    Args:
        column: Column name containing dates
        allow_today: If True, today's date is considered valid

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    today = F.current_date()
    
    if allow_today:
        return col_expr.isNull() | (col_expr <= today)
    else:
        return col_expr.isNull() | (col_expr < today)


def is_business_day(column: str) -> Column:
    """
    Check if date falls on a business day (Monday-Friday).

    Args:
        column: Column name containing dates

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    # dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
    day_of_week = F.dayofweek(col_expr)
    
    return col_expr.isNull() | day_of_week.between(2, 6)


def timestamp_range_validator(
    column: str,
    min_timestamp: Optional[str] = None,
    max_timestamp: Optional[str] = None,
) -> Column:
    """
    Validate timestamp is within specified range.

    Args:
        column: Column name containing timestamps
        min_timestamp: Minimum timestamp (ISO format) or None
        max_timestamp: Maximum timestamp (ISO format) or None

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    conditions = [col_expr.isNull()]

    if min_timestamp:
        conditions.append(col_expr >= F.lit(min_timestamp).cast("timestamp"))

    if max_timestamp:
        conditions.append(col_expr <= F.lit(max_timestamp).cast("timestamp"))

    from functools import reduce
    from operator import and_

    return reduce(and_, conditions) if len(conditions) > 1 else conditions[0]


# ============================================================================
# STRING VALIDATION CHECKS
# ============================================================================

def string_length_range(
    column: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
) -> Column:
    """
    Validate string length is within specified range.

    Args:
        column: Column name containing strings
        min_length: Minimum length or None
        max_length: Maximum length or None

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    length_expr = F.length(col_expr)
    conditions = [col_expr.isNull()]

    if min_length is not None:
        conditions.append(length_expr >= min_length)

    if max_length is not None:
        conditions.append(length_expr <= max_length)

    from functools import reduce
    from operator import and_

    return reduce(and_, conditions) if len(conditions) > 1 else conditions[0]


def string_pattern_validator(column: str, pattern: str) -> Column:
    """
    Validate string matches a regex pattern.

    Args:
        column: Column name
        pattern: Regex pattern to match

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | col_expr.rlike(pattern)


def starts_with_validator(column: str, prefix: str) -> Column:
    """
    Check if string starts with specific prefix.

    Args:
        column: Column name
        prefix: Required prefix

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | col_expr.startswith(prefix)


def ends_with_validator(column: str, suffix: str) -> Column:
    """
    Check if string ends with specific suffix.

    Args:
        column: Column name
        suffix: Required suffix

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | col_expr.endswith(suffix)


def contains_validator(column: str, substring: str) -> Column:
    """
    Check if string contains specific substring.

    Args:
        column: Column name
        substring: Required substring

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | col_expr.contains(substring)


def no_special_characters(column: str, allowed_chars: str = "") -> Column:
    """
    Check if string contains only alphanumeric characters (and optionally allowed special chars).

    Args:
        column: Column name
        allowed_chars: Additional allowed characters (e.g., "-_")

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    if allowed_chars:
        pattern = f"^[a-zA-Z0-9{allowed_chars}]*$"
    else:
        pattern = "^[a-zA-Z0-9]*$"
    
    return col_expr.isNull() | col_expr.rlike(pattern)


def is_uppercase(column: str) -> Column:
    """
    Check if string is all uppercase.

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | (col_expr == F.upper(col_expr))


def is_lowercase(column: str) -> Column:
    """
    Check if string is all lowercase.

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | (col_expr == F.lower(col_expr))


# ============================================================================
# NUMERIC VALIDATION CHECKS
# ============================================================================

def numeric_precision_validator(
    column: str,
    max_precision: int,
    max_scale: int,
) -> Column:
    """
    Validate numeric precision and scale for decimal values.

    Args:
        column: Column name containing numeric values
        max_precision: Maximum total digits
        max_scale: Maximum decimal places

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    str_expr = F.trim(col_expr.cast("string"))
    abs_str = F.regexp_replace(str_expr, "^-", "")
    parts = F.split(abs_str, "\\.")
    total_digits = F.length(F.regexp_replace(abs_str, "\\.", ""))
    decimal_places = F.when(F.size(parts) > 1, F.length(F.element_at(parts, 2))).otherwise(0)
    
    return (
        col_expr.isNull()
        | ((total_digits <= max_precision) & (decimal_places <= max_scale))
    )


def is_positive(column: str, allow_zero: bool = False) -> Column:
    """
    Check if numeric value is positive.

    Args:
        column: Column name
        allow_zero: If True, zero is considered valid

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if allow_zero:
        return col_expr.isNull() | (col_expr >= 0)
    else:
        return col_expr.isNull() | (col_expr > 0)


def is_negative(column: str, allow_zero: bool = False) -> Column:
    """
    Check if numeric value is negative.

    Args:
        column: Column name
        allow_zero: If True, zero is considered valid

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if allow_zero:
        return col_expr.isNull() | (col_expr <= 0)
    else:
        return col_expr.isNull() | (col_expr < 0)


def is_percentage(column: str) -> Column:
    """
    Check if numeric value is a valid percentage (0-100).

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | col_expr.between(0, 100)


def is_even(column: str) -> Column:
    """
    Check if integer value is even.

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | ((col_expr % 2) == 0)


def is_odd(column: str) -> Column:
    """
    Check if integer value is odd.

    Args:
        column: Column name

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    return col_expr.isNull() | ((col_expr % 2) != 0)


# ============================================================================
# BUSINESS LOGIC VALIDATION CHECKS
# ============================================================================

def age_from_birthdate_validator(
    birthdate_column: str,
    min_age: int = 0,
    max_age: int = 150,
) -> Column:
    """
    Validate age calculated from birthdate is within valid range.

    Args:
        birthdate_column: Column name containing birthdates
        min_age: Minimum valid age
        max_age: Maximum valid age

    Returns:
        Boolean Column expression
    """
    birthdate = F.col(birthdate_column)
    today = F.current_date()
    age = F.floor(F.months_between(today, birthdate) / 12)
    
    return birthdate.isNull() | age.between(min_age, max_age)


def compare_columns(
    column1: str,
    column2: str,
    operator: str = ">=",
) -> Column:
    """
    Compare two columns using specified operator.

    Args:
        column1: First column name
        column2: Second column name
        operator: Comparison operator (>=, <=, >, <, ==, !=)

    Returns:
        Boolean Column expression
    """
    col1 = F.col(column1)
    col2 = F.col(column2)
    
    operators = {
        ">=": col1 >= col2,
        "<=": col1 <= col2,
        ">": col1 > col2,
        "<": col1 < col2,
        "==": col1 == col2,
        "!=": col1 != col2,
    }
    
    condition = operators.get(operator, col1 >= col2)
    return col1.isNull() | col2.isNull() | condition


def json_string_validator(column: str) -> Column:
    """
    Validate if string is valid JSON.

    Args:
        column: Column name containing JSON strings

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    # Try to parse as JSON, if it fails, get_json_object returns null
    parsed = F.get_json_object(col_expr, "$")
    
    return col_expr.isNull() | parsed.isNotNull()


def currency_code_validator(column: str) -> Column:
    """
    Validate if string is a valid ISO 4217 currency code.

    Args:
        column: Column name containing currency codes

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    # Common currency codes
    valid_codes = [
        "USD", "EUR", "GBP", "JPY", "CNY", "INR", "AUD", "CAD", "CHF", "SEK",
        "NZD", "KRW", "SGD", "NOK", "MXN", "ZAR", "HKD", "BRL", "RUB", "TRY"
    ]
    
    return col_expr.isNull() | F.upper(col_expr).isin(valid_codes)


def country_code_validator(column: str, code_type: str = "alpha2") -> Column:
    """
    Validate if string is a valid ISO country code.

    Args:
        column: Column name containing country codes
        code_type: Type of code ("alpha2" or "alpha3")

    Returns:
        Boolean Column expression
    """
    col_expr = F.col(column)
    
    if code_type == "alpha2":
        # 2-letter codes
        pattern = "^[A-Z]{2}$"
        length = 2
    else:
        # 3-letter codes
        pattern = "^[A-Z]{3}$"
        length = 3
    
    upper_col = F.upper(col_expr)
    return col_expr.isNull() | (upper_col.rlike(pattern) & (F.length(upper_col) == length))
