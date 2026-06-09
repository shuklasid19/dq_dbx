"""Generate DQ metrics test data."""

import argparse
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType
)


class DQMetricsGenerator:
    """Generate DQ metrics and summary test data."""
    
    def __init__(self, spark: SparkSession, catalog: str = "dbx_dataquality", schema: str = "dq_observe"):
        """
        Initialize DQ metrics generator.
        
        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name for DQ tables
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"
        
        # Financial tables configuration
        self.tables_config = [
            # Bronze layer
            ("customers", "bronze", ["is_not_null", "is_unique", "email_format_validator", "phone_number_validator"], 95.5, 98.5),
            ("accounts", "bronze", ["is_not_null", "is_unique", "is_in", "string_length_range"], 96.0, 99.0),
            ("transactions", "bronze", ["is_not_null", "is_numeric", "date_range_validator", "numeric_precision_validator"], 94.0, 97.5),
            
            # Silver layer
            # ("accounts", "silver", ["column_datatype_validator", "sql_expression"], 97.0, 99.5),
            ("dim_customer", "silver", ["is_not_null", "is_unique", "email_format_validator", "is_in"], 98.0, 99.8),
            ("fact_transaction", "silver", ["is_not_null", "is_positive", "is_in"], 96.5, 99.0),
            
            # Gold layer
            ("agg_daily_account_balance", "gold", ["is_not_null", "is_unique", "is_numeric", "sql_expression"], 98.5, 99.9),
            ("agg_customer_metrics", "gold", ["is_not_null", "is_unique", "is_positive", "sql_expression"], 99.0, 99.9),
        ]
        
        random.seed(42)
    
    def create_schema(self):
        """Create DQ monitoring schema."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.full_schema}")
        print(f"✓ Created schema: {self.full_schema}")
    
    def generate_metrics(self, days: int = 30) -> None:
        """
        Generate DQ metrics data.
        
        Args:
            days: Number of days of historical data
        """
        print(f"\nGenerating DQ metrics for {days} days...")
        
        metrics_data = []
        start_date = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            execution_id = f"exec_{current_date.strftime('%Y%m%d')}_{random.randint(1000, 9999)}"
            
            for table_name, layer, checks, min_pass, max_pass in self.tables_config:
                # Determine row counts based on layer and table
                if layer == "bronze":
                    if table_name == "customers":
                        total_rows = random.randint(950, 1050)
                    elif table_name == "accounts":
                        total_rows = random.randint(1800, 2200)
                    else:  # transactions
                        total_rows = random.randint(4500, 5500)
                elif layer == "silver":
                    if "dim" in table_name:
                        total_rows = random.randint(950, 1050)
                    elif "fact" in table_name:
                        total_rows = random.randint(4000, 5000)
                    else:
                        total_rows = random.randint(1800, 2200)
                else:  # gold
                    total_rows = random.randint(800, 1200)
                
                for check_name in checks:
                    # Determine severity
                    if check_name in ["is_not_null", "is_unique", "column_datatype_validator"]:
                        severity = "error"
                    else:
                        severity = random.choice(["error", "warn"])
                    
                    # Generate failure rate with trend (improving over time)
                    base_failure_rate = (100 - random.uniform(min_pass, max_pass))
                    trend_factor = (days - day) / days  # Earlier days have higher failures
                    failure_rate = base_failure_rate * (0.5 + 0.5 * trend_factor)
                    
                    # Add some randomness and occasional spikes
                    if random.random() < 0.05:  # 5% chance of spike
                        failure_rate *= random.uniform(2, 5)
                    
                    failure_rate = min(failure_rate, 10.0)  # Cap at 10%
                    failure_count = int(total_rows * failure_rate / 100)
                    
                    # Execution duration (in milliseconds)
                    base_duration = random.randint(50, 500)
                    if check_name == "is_unique":
                        base_duration *= 2  # Uniqueness checks are slower
                    
                    execution_duration_ms = base_duration + random.randint(-20, 50)
                    
                    metrics_data.append((
                        execution_id,
                        current_date,
                        table_name,
                        layer,
                        check_name,
                        severity,
                        total_rows,
                        failure_count,
                        round(failure_rate, 2),
                        execution_duration_ms
                    ))
        
        # Create DataFrame
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("execution_date", TimestampType(), False),
            StructField("table_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("check_name", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("total_rows", LongType(), False),
            StructField("failure_count", LongType(), False),
            StructField("failure_rate", DoubleType(), False),
            StructField("execution_duration_ms", IntegerType(), True),
        ])
        
        df = self.spark.createDataFrame(metrics_data, schema=schema)
        
        # Write to metrics table
        df.write.mode("overwrite").saveAsTable(f"{self.full_schema}.dq_metrics")
        print(f"✓ Created {self.full_schema}.dq_metrics with {df.count()} records")
    
    def generate_summary(self, days: int = 30) -> None:
        """
        Generate DQ summary data.
        
        Args:
            days: Number of days of historical data
        """
        print(f"\nGenerating DQ summary for {days} days...")
        
        summary_data = []
        start_date = datetime.now() - timedelta(days=days)
        
        for day in range(days):
            current_date = start_date + timedelta(days=day)
            execution_id = f"exec_{current_date.strftime('%Y%m%d')}_{random.randint(1000, 9999)}"
            
            for table_name, layer, checks, min_pass, max_pass in self.tables_config:
                # Determine row counts
                if layer == "bronze":
                    if table_name == "customers":
                        total_rows = random.randint(950, 1050)
                    elif table_name == "accounts":
                        total_rows = random.randint(1800, 2200)
                    else:
                        total_rows = random.randint(4500, 5500)
                elif layer == "silver":
                    if "dim" in table_name:
                        total_rows = random.randint(950, 1050)
                    elif "fact" in table_name:
                        total_rows = random.randint(4000, 5000)
                    else:
                        total_rows = random.randint(1800, 2200)
                else:
                    total_rows = random.randint(800, 1200)
                
                # Calculate pass rate with improvement trend
                trend_factor = day / days  # Later days have better quality
                pass_rate = random.uniform(min_pass, max_pass) + (trend_factor * 2)
                pass_rate = min(pass_rate, 99.9)
                
                # Add occasional quality drops
                if random.random() < 0.03:  # 3% chance
                    pass_rate -= random.uniform(2, 10)
                
                # Calculate row counts
                error_rate = (100 - pass_rate) * random.uniform(0.6, 0.9)
                warning_rate = (100 - pass_rate) - error_rate
                
                error_rows = int(total_rows * error_rate / 100)
                warning_rows = int(total_rows * warning_rate / 100)
                valid_rows = total_rows - error_rows - warning_rows
                
                # Execution duration
                execution_duration_ms = len(checks) * random.randint(80, 200)
                
                summary_data.append((
                    execution_id,
                    current_date,
                    table_name,
                    layer,
                    total_rows,
                    valid_rows,
                    error_rows,
                    warning_rows,
                    round(pass_rate, 2),
                    execution_duration_ms
                ))
        
        # Create DataFrame
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("execution_date", TimestampType(), False),
            StructField("table_name", StringType(), False),
            StructField("layer", StringType(), False),
            StructField("total_rows", LongType(), False),
            StructField("valid_rows", LongType(), False),
            StructField("error_rows", LongType(), False),
            StructField("warning_rows", LongType(), False),
            StructField("pass_rate", DoubleType(), False),
            StructField("execution_duration_ms", IntegerType(), True),
        ])
        
        df = self.spark.createDataFrame(summary_data, schema=schema)
        
        # Write to summary table
        df.write.mode("overwrite").saveAsTable(f"{self.full_schema}.dq_summary")
        print(f"✓ Created {self.full_schema}.dq_summary with {df.count()} records")
    
    def generate_all(self, days: int = 30):
        """Generate all DQ test data."""
        print(f"\n{'='*80}")
        print("DQ METRICS DATA GENERATION")
        print(f"{'='*80}")
        
        self.create_schema()
        self.generate_metrics(days)
        self.generate_summary(days)
        
        print(f"\n{'='*80}")
        print("✅ DQ METRICS GENERATION COMPLETE")
        print(f"{'='*80}")
        print(f"\nGenerated tables:")
        print(f"  - {self.full_schema}.dq_metrics")
        print(f"  - {self.full_schema}.dq_summary")
        print(f"\nCoverage:")
        print(f"  - 8 tables (3 bronze, 2 silver, 2 gold)")
        print(f"  - {days} days of historical data")
        print(f"  - Quality trend: improving over time")
        print(f"\nNext steps:")
        print(f"  1. Run: python scripts/setup_dashboard.py")
        print(f"  2. Create Databricks SQL Dashboard")
        print(f"  3. Monitor data quality trends")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate DQ metrics test data")
    parser.add_argument(
        "--catalog",
        default="main",
        help="Unity Catalog name (default: main)"
    )
    parser.add_argument(
        "--schema",
        default="dq_monitoring",
        help="Schema name (default: dq_monitoring)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days of historical data (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("DQ-Metrics-Generator")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        generator = DQMetricsGenerator(
            spark,
            catalog=args.catalog,
            schema=args.schema
        )
        generator.generate_all(days=args.days)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
