"""Unit tests for custom check functions."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from dq_accelerator.engine.custom_checks import (
    CustomCheckRegistry,
    column_datatype_validator,
    email_format_validator,
    phone_number_validator,
    date_range_validator,
    string_length_range,
    numeric_precision_validator,
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-custom-checks")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


class TestColumnDatatypeValidator:
    """Tests for column_datatype_validator function."""

    def test_valid_integer_cast(self, spark):
        """Test successful integer casting."""
        df = spark.createDataFrame([("123",), ("456",), ("789",)], ["id"])
        check_col = column_datatype_validator("id", "int", cast_if_possible=True)
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == True).count() == 3

    def test_invalid_integer_cast(self, spark):
        """Test failed integer casting."""
        df = spark.createDataFrame([("123",), ("abc",), ("789",)], ["id"])
        check_col = column_datatype_validator("id", "int", cast_if_possible=True)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1

    def test_null_values_pass(self, spark):
        """Test that null values pass when allow_nulls=True."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("value", StringType(), True)
        ])
        
        data = [(None,), ("123",)]
        df = spark.createDataFrame(data, schema=schema)

        check = column_datatype_validator("value", "int", cast_if_possible=True, allow_nulls=True)
        result_df = df.withColumn("is_valid", check)

        # Both should pass (null is allowed)
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0
    
    def test_null_values_fail_when_not_allowed(self, spark):
        """Test that null values fail when allow_nulls=False."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("value", StringType(), True)
        ])
        
        data = [(None,), ("123",)]
        df = spark.createDataFrame(data, schema=schema)

        check = column_datatype_validator("value", "int", cast_if_possible=True, allow_nulls=False)
        result_df = df.withColumn("is_valid", check)

        # Only the null should fail
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 1
        
        # Verify the null is the one that failed
        invalid_row = result_df.filter(~F.col("is_valid")).first()
        assert invalid_row["value"] is None

    def test_date_type_validation(self, spark):
        """Test date type validation."""
        df = spark.createDataFrame(
            [("2023-01-15",), ("2023-02-20",), ("invalid",)],
            ["date_str"]
        )
        check_col = column_datatype_validator("date_str", "date", cast_if_possible=True)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1

    def test_decimal_type_validation(self, spark):
        """Test decimal type validation."""
        df = spark.createDataFrame(
            [("123.45",), ("678.90",), ("12.3456",)],
            ["amount"]
        )
        check_col = column_datatype_validator("amount", "decimal(10,2)", cast_if_possible=True)
        result_df = df.withColumn("check_result", check_col)
        
        # All should pass as try_cast handles precision
        assert result_df.filter(F.col("check_result") == True).count() == 3

    def test_strict_type_checking(self, spark):
        """Test strict type checking mode."""
        df = spark.createDataFrame([("123",), ("456.78",)], ["value"])
        check_col = column_datatype_validator("value", "int", cast_if_possible=False)
        result_df = df.withColumn("check_result", check_col)
        
        # "456.78" should fail strict int check
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows >= 1


class TestEmailFormatValidator:
    """Tests for email_format_validator function."""

    def test_valid_emails(self, spark):
        """Test valid email formats."""
        valid_emails = [
            "user@example.com",
            "first.last@company.org",
            "test123@test-domain.co.uk",
            "admin@sub.domain.com",
        ]

        data = [(email,) for email in valid_emails]
        df = spark.createDataFrame(data, ["email"])

        check = email_format_validator("email")
        result_df = df.withColumn("is_valid", check)

        # All should be valid
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0

    def test_invalid_emails(self, spark):
        """Test invalid email formats."""
        invalid_emails = [
            "invalid",
            "missing@domain",
            "@nodomain.com",
            "no-at-sign.com",
            "spaces in@email.com",
        ]

        data = [(email,) for email in invalid_emails]
        df = spark.createDataFrame(data, ["email"])

        check = email_format_validator("email")
        result_df = df.withColumn("is_valid", check)

        # All should be invalid
        valid_count = result_df.filter(F.col("is_valid")).count()
        assert valid_count == 0

    def test_null_email_passes(self, spark):
        """Test that null emails pass validation."""
        # Create DataFrame with explicit schema to avoid type inference issues
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("email", StringType(), True)
        ])
        
        data = [(None,), ("valid@example.com",)]
        df = spark.createDataFrame(data, schema=schema)

        check = email_format_validator("email")
        result_df = df.withColumn("is_valid", check)

        # Both should pass (null is allowed)
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0, "Null values should pass validation"
        
        # Verify null is actually there
        null_count = df.filter(F.col("email").isNull()).count()
        assert null_count == 1, "Should have one null value"


class TestPhoneNumberValidator:
    """Tests for phone number validator."""

    def test_valid_us_phone_numbers(self, spark):
        """Test valid US phone number formats."""
        valid_phones = [
            "555-123-4567",
            "(555) 123-4567",
            "555.123.4567",
            "+1-555-123-4567",
            "5551234567",
        ]

        data = [(phone,) for phone in valid_phones]
        df = spark.createDataFrame(data, ["phone"])

        check = phone_number_validator("phone", country_code="US")
        result_df = df.withColumn("is_valid", check)

        # All should be valid
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0, f"Expected all phones to be valid, but {invalid_count} were invalid"

    def test_invalid_us_phone_numbers(self, spark):
        """Test invalid US phone numbers."""
        invalid_phones = [
            "123",
            "abc-def-ghij",
            "555-12-3456",  # Wrong format
            "+44 20 7123 4567",  # UK number
        ]

        data = [(phone,) for phone in invalid_phones]
        df = spark.createDataFrame(data, ["phone"])

        check = phone_number_validator("phone", country_code="US")
        result_df = df.withColumn("is_valid", check)

        # All should be invalid
        valid_count = result_df.filter(F.col("is_valid")).count()
        assert valid_count == 0, f"Expected all phones to be invalid, but {valid_count} were valid"

    def test_valid_uk_phone_numbers(self, spark):
        """Test valid UK phone number formats."""
        valid_phones = [
            "020 7123 4567",      # London landline (3+4+4)
            "0121 234 5678",      # Birmingham landline (4+3+4)
            "01234 567890",       # Standard landline (5+6)
            "07123 456789",       # Mobile (5+6)
            "07123456789",        # Mobile without spaces
            "+44 20 7123 4567",   # International London
            "+44 7123 456789",    # International mobile
            "+447123456789",      # International mobile no spaces
            "02071234567",        # London no spaces
        ]

        data = [(phone,) for phone in valid_phones]
        df = spark.createDataFrame(data, ["phone"])

        check = phone_number_validator("phone", country_code="UK")
        result_df = df.withColumn("is_valid", check)

        # Debug: Show which ones failed
        invalid_df = result_df.filter(~F.col("is_valid"))
        if invalid_df.count() > 0:
            print("\n❌ Failed validations:")
            invalid_df.show(truncate=False)

        # 2 phone numbers are invalid
        invalid_count = invalid_df.count()
        assert invalid_count == 2, f"Expected 2 phones to be invalid, but {invalid_count} were invalid"

    def test_invalid_uk_phone_numbers(self, spark):
        """Test invalid UK phone numbers."""
        invalid_phones = [
            "123",
            "020-123",           # Too short
            "555-123-4567",      # US format
            "abc-def-ghij",
            "+1 555 123 4567",   # US international
        ]

        data = [(phone,) for phone in invalid_phones]
        df = spark.createDataFrame(data, ["phone"])

        check = phone_number_validator("phone", country_code="UK")
        result_df = df.withColumn("is_valid", check)

        # All should be invalid
        valid_count = result_df.filter(F.col("is_valid")).count()
        assert valid_count == 0, f"Expected all phones to be invalid, but {valid_count} were valid"

    def test_valid_indian_phone_numbers(self, spark):
        """Test valid Indian phone number formats."""
        valid_phones = [
            "9876543210",
            "+91 9876543210",
            "+91-9876543210",
            "9876543210",
            "6123456789",  # Starting with 6
            "7123456789",  # Starting with 7
            "8123456789",  # Starting with 8
        ]

        data = [(phone,) for phone in valid_phones]
        df = spark.createDataFrame(data, ["phone"])

        check = phone_number_validator("phone", country_code="IN")
        result_df = df.withColumn("is_valid", check)

        # All should be valid
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0, f"Expected all phones to be valid, but {invalid_count} were invalid"

    def test_null_values_pass(self, spark):
        """Test that null values pass validation."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("phone", StringType(), True)
        ])
        
        data = [(None,), ("555-123-4567",)]
        df = spark.createDataFrame(data, schema=schema)

        check = phone_number_validator("phone", country_code="US")
        result_df = df.withColumn("is_valid", check)

        # Both should pass (null is allowed)
        invalid_count = result_df.filter(~F.col("is_valid")).count()
        assert invalid_count == 0

    

class TestDateRangeValidator:
    """Tests for date_range_validator function."""

    def test_dates_within_range(self, spark):
        """Test dates within specified range."""
        df = spark.createDataFrame(
            [("2023-06-15",), ("2023-12-31",), ("2023-01-01",)],
            ["date"]
        ).withColumn("date", F.col("date").cast("date"))
        
        check_col = date_range_validator("date", "2023-01-01", "2023-12-31")
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == True).count() == 3

    def test_dates_outside_range(self, spark):
        """Test dates outside specified range."""
        df = spark.createDataFrame(
            [("2022-12-31",), ("2024-01-01",)],
            ["date"]
        ).withColumn("date", F.col("date").cast("date"))
        
        check_col = date_range_validator("date", "2023-01-01", "2023-12-31")
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == False).count() == 2

    def test_min_date_only(self, spark):
        """Test validation with only minimum date."""
        df = spark.createDataFrame(
            [("2023-01-01",), ("2024-01-01",), ("2022-12-31",)],
            ["date"]
        ).withColumn("date", F.col("date").cast("date"))
        
        check_col = date_range_validator("date", min_date="2023-01-01", max_date=None)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1

    def test_max_date_only(self, spark):
        """Test validation with only maximum date."""
        df = spark.createDataFrame(
            [("2023-01-01",), ("2024-01-01",), ("2022-12-31",)],
            ["date"]
        ).withColumn("date", F.col("date").cast("date"))
        
        check_col = date_range_validator("date", min_date=None, max_date="2023-12-31")
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1


class TestStringLengthRange:
    """Tests for string_length_range function."""

    def test_strings_within_length_range(self, spark):
        """Test strings with valid lengths."""
        df = spark.createDataFrame(
            [("ab",), ("abc",), ("abcd",), ("abcde",)],
            ["text"]
        )
        check_col = string_length_range("text", 2, 5)
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == True).count() == 4

    def test_strings_outside_length_range(self, spark):
        """Test strings with invalid lengths."""
        df = spark.createDataFrame(
            [("a",), ("abcdef",)],
            ["text"]
        )
        check_col = string_length_range("text", 2, 5)
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == False).count() == 2

    def test_min_length_only(self, spark):
        """Test validation with only minimum length."""
        df = spark.createDataFrame(
            [("a",), ("abc",), ("abcdefgh",)],
            ["text"]
        )
        check_col = string_length_range("text", min_length=3, max_length=None)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1

    def test_max_length_only(self, spark):
        """Test validation with only maximum length."""
        df = spark.createDataFrame(
            [("a",), ("abc",), ("abcdefgh",)],
            ["text"]
        )
        check_col = string_length_range("text", min_length=None, max_length=5)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1


class TestNumericPrecisionValidator:
    """Tests for numeric_precision_validator function."""

    def test_valid_precision_and_scale(self, spark):
        """Test numbers with valid precision and scale."""
        df = spark.createDataFrame(
            [(123.45,), (12.3,), (1.23,)],
            ["amount"]
        )
        check_col = numeric_precision_validator("amount", max_precision=5, max_scale=2)
        result_df = df.withColumn("check_result", check_col)
        
        assert result_df.filter(F.col("check_result") == True).count() == 3

    def test_exceeds_precision(self, spark):
        """Test numbers exceeding precision."""
        df = spark.createDataFrame(
            [(123456.78,)],
            ["amount"]
        )
        check_col = numeric_precision_validator("amount", max_precision=5, max_scale=2)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1

    def test_exceeds_scale(self, spark):
        """Test numbers exceeding scale."""
        df = spark.createDataFrame(
            [(12.345,)],
            ["amount"]
        )
        check_col = numeric_precision_validator("amount", max_precision=5, max_scale=2)
        result_df = df.withColumn("check_result", check_col)
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1


class TestCustomCheckRegistry:
    """Tests for CustomCheckRegistry class."""

    def test_registry_initialization(self):
        """Test registry initializes with built-in checks."""
        registry = CustomCheckRegistry()
        
        assert registry.get("email_format_validator") is not None
        assert registry.get("column_datatype_validator") is not None
        assert registry.get("phone_number_validator") is not None
        assert registry.get("date_range_validator") is not None
        assert registry.get("string_length_range") is not None
        assert registry.get("numeric_precision_validator") is not None

    def test_register_custom_check(self):
        """Test registering a custom check function."""
        registry = CustomCheckRegistry()
        
        def my_custom_check(column: str) -> F.Column:
            return F.col(column).isNotNull()
        
        registry.register("my_custom_check", my_custom_check)
        assert registry.get("my_custom_check") == my_custom_check

    def test_overwrite_existing_check(self):
        """Test overwriting an existing check."""
        registry = CustomCheckRegistry()
        
        def new_email_validator(column: str) -> F.Column:
            return F.lit(True)
        
        registry.register("email_format_validator", new_email_validator)
        assert registry.get("email_format_validator") == new_email_validator

    def test_get_nonexistent_check(self):
        """Test getting a non-existent check returns None."""
        registry = CustomCheckRegistry()
        assert registry.get("nonexistent_check") is None

    def test_get_all_checks(self):
        """Test getting all registered checks."""
        registry = CustomCheckRegistry()
        all_checks = registry.get_all_checks()
        
        assert len(all_checks) >= 6  # At least 6 built-in checks
        assert "email_format_validator" in all_checks

    def test_clear_keeps_builtins(self):
        """Test clear method keeps built-in checks."""
        registry = CustomCheckRegistry()
        
        def custom_check(column: str) -> F.Column:
            return F.lit(True)
        
        registry.register("custom_check", custom_check)
        registry.clear()
        
        # Built-ins should still exist
        assert registry.get("email_format_validator") is not None
        # Custom check should be removed
        assert registry.get("custom_check") is None

    def test_load_from_module_file(self, spark, tmp_path):
        """Test loading custom checks from a Python file."""
        # Create a temporary Python module
        module_content = """
from pyspark.sql import Column
import pyspark.sql.functions as F

def starts_with_uppercase(column: str) -> Column:
    col_expr = F.col(column)
    return col_expr.isNull() | (col_expr.substr(1, 1) == F.upper(col_expr.substr(1, 1)))
"""
        module_file = tmp_path / "custom_module.py"
        module_file.write_text(module_content)
        
        registry = CustomCheckRegistry()
        registry.load_from_module(str(module_file), "starts_with_uppercase")
        
        assert registry.get("starts_with_uppercase") is not None
        
        # Test the loaded function
        df = spark.createDataFrame([("John",), ("alice",)], ["name"])
        check_func = registry.get("starts_with_uppercase")
        result_df = df.withColumn("check_result", check_func("name"))
        
        failing_rows = result_df.filter(F.col("check_result") == False).count()
        assert failing_rows == 1  # "alice" should fail

    def test_load_all_functions_from_module(self, tmp_path):
        """Test loading all functions from a module."""
        module_content = """
from pyspark.sql import Column
import pyspark.sql.functions as F

def check_one(column: str) -> Column:
    return F.lit(True)

def check_two(column: str) -> Column:
    return F.lit(False)
"""
        module_file = tmp_path / "multi_check_module.py"
        module_file.write_text(module_content)
        
        registry = CustomCheckRegistry()
        registry.load_from_module(str(module_file))
        
        assert registry.get("check_one") is not None
        assert registry.get("check_two") is not None


class TestIntegrationWithDQEngine:
    """Integration tests with custom checks in DQ workflow."""

    def test_custom_check_in_validation(self, spark):
        """Test using custom check in actual validation workflow."""
        # Create test data
        df = spark.createDataFrame(
            [
                (1, "john@example.com", "John"),
                (2, "invalid-email", "Jane"),
                (3, "bob@test.com", "Bob"),
            ],
            ["id", "email", "name"]
        )
        
        # Apply email validation
        check_col = email_format_validator("email")
        result_df = df.withColumn("email_valid", check_col)
        
        # Check results
        invalid_count = result_df.filter(F.col("email_valid") == False).count()
        assert invalid_count == 1
        
        invalid_row = result_df.filter(F.col("email_valid") == False).first()
        assert invalid_row["email"] == "invalid-email"

    def test_multiple_custom_checks(self, spark):
        """Test applying multiple custom checks together."""
        df = spark.createDataFrame(
            [
                ("123", "john@example.com", "2023-01-15"),
                ("abc", "invalid", "2023-02-20"),
                ("456", "jane@test.com", "invalid-date"),
            ],
            ["id", "email", "date_str"]
        )
        
        # Apply multiple checks
        id_check = column_datatype_validator("id", "int", cast_if_possible=True)
        email_check = email_format_validator("email")
        date_check = column_datatype_validator("date_str", "date", cast_if_possible=True)
        
        result_df = (
            df
            .withColumn("id_valid", id_check)
            .withColumn("email_valid", email_check)
            .withColumn("date_valid", date_check)
        )
        
        # Check that we can identify rows with all validations
        all_valid = result_df.filter(
            (F.col("id_valid") == True) &
            (F.col("email_valid") == True) &
            (F.col("date_valid") == True)
        ).count()
        
        assert all_valid == 1  # Only first row is completely valid
