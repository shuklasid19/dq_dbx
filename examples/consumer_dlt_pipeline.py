"""
Delta Live Tables (DLT) Pipeline with DQ Accelerator Integration

This example shows how to integrate DQ Accelerator into a DLT pipeline
for continuous data quality monitoring.
"""

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from dq_accelerator import DQAccelerator
from dq_accelerator.config.loader import ConfigLoader
from dq_accelerator.config.merger import ConfigMerger
import logging

# Configuration
CONFIG_BASE_PATH = "/Workspace/Shared/dq_configs"
DQ_ENABLED = True

# Initialize logger
logger = logging.getLogger(__name__)


def get_dq_accelerator() -> DQAccelerator:
    """Get or create DQ Accelerator instance."""
    spark = SparkSession.builder.getOrCreate()
    return DQAccelerator(spark=spark)


def apply_dq_checks(
    df: DataFrame,
    table_name: str,
    layer: str,
    write_results: bool = True,
) -> DataFrame:
    """
    Apply DQ checks to a DataFrame and return valid data.
    
    Args:
        df: Input DataFrame
        table_name: Table name for config lookup
        layer: Layer name (bronze, silver, gold)
        write_results: Whether to write quarantine/metrics data
        
    Returns:
        Valid DataFrame after quality checks
    """
    if not DQ_ENABLED:
        logger.info("DQ checks disabled, returning original DataFrame")
        return df
    
    try:
        # Load configurations
        layer_config_path = f"{CONFIG_BASE_PATH}/layer_{layer}.yaml"
        table_config_path = f"{CONFIG_BASE_PATH}/table_{table_name}.yaml"
        
        layer_config = ConfigLoader.load_layer_config(layer_config_path)
        table_config = ConfigLoader.load_table_config(table_config_path)
        
        # Initialize DQ Accelerator
        dq = get_dq_accelerator()
        
        # Run checks
        results = dq.run_checks(
            df=df,
            table_config=table_config,
            layer_config=layer_config,
            split_valid_invalid=True,
            write_results=write_results,
        )
        
        # Return valid data
        valid_df = results["result"]
        
        # Add DQ metadata columns
        valid_df = (
            valid_df
            .withColumn("dq_checked", F.lit(True))
            .withColumn("dq_check_timestamp", F.current_timestamp())
        )
        
        logger.info(f"DQ checks completed for {table_name}: "
                   f"{valid_df.count()} valid records")
        
        return valid_df
        
    except Exception as e:
        logger.error(f"DQ check failed for {table_name}: {str(e)}")
        # In production, decide whether to fail or continue
        # For now, we'll continue with original data but flag it
        return df.withColumn("dq_checked", F.lit(False))


# ============================================================================
# BRONZE LAYER - Raw Data Ingestion with Quality Checks
# ============================================================================

@dlt.table(
    name="bronze_customers_raw",
    comment="Raw customer data from source system",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_customers_raw():
    """Ingest raw customer data."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/customers")
        .load("/mnt/landing/customers/")
    )


@dlt.table(
    name="bronze_customers",
    comment="Customer data with quality checks applied",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all_or_drop({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_email": "email IS NOT NULL"
})
def bronze_customers():
    """Apply DQ checks to customer data."""
    raw_df = dlt.read_stream("bronze_customers_raw")
    
    # Apply comprehensive DQ checks using accelerator
    return apply_dq_checks(
        df=raw_df,
        table_name="customers",
        layer="bronze",
        write_results=True
    )


@dlt.table(
    name="bronze_orders_raw",
    comment="Raw order data from source system",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_orders_raw():
    """Ingest raw order data."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/schemas/orders")
        .load("/mnt/landing/orders/")
    )


@dlt.table(
    name="bronze_orders",
    comment="Order data with quality checks applied",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_orders():
    """Apply DQ checks to order data."""
    raw_df = dlt.read_stream("bronze_orders_raw")
    
    return apply_dq_checks(
        df=raw_df,
        table_name="orders",
        layer="bronze",
        write_results=True
    )


# ============================================================================
# SILVER LAYER - Cleaned and Conformed Data
# ============================================================================

@dlt.table(
    name="silver_customers",
    comment="Cleaned and validated customer data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_email_format": "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
    "positive_customer_id": "customer_id > 0"
})
def silver_customers():
    """Transform bronze customers to silver with additional checks."""
    bronze_df = dlt.read_stream("bronze_customers")
    
    # Apply transformations
    transformed_df = (
        bronze_df
        .withColumn("customer_id", F.col("customer_id").cast("bigint"))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        .withColumn("created_date", F.col("created_date").cast("date"))
        .withColumn("updated_timestamp", F.current_timestamp())
    )
    
    # Apply silver-layer quality checks
    return apply_dq_checks(
        df=transformed_df,
        table_name="customers",
        layer="silver",
        write_results=True
    )


@dlt.table(
    name="silver_orders",
    comment="Cleaned and validated order data",
    table_properties={
        "quality": "silver"
    }
)
def silver_orders():
    """Transform bronze orders to silver with additional checks."""
    bronze_df = dlt.read_stream("bronze_orders")
    
    # Apply transformations
    transformed_df = (
        bronze_df
        .withColumn("order_id", F.col("order_id").cast("bigint"))
        .withColumn("customer_id", F.col("customer_id").cast("bigint"))
        .withColumn("order_amount", F.col("order_amount").cast("decimal(18,2)"))
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("updated_timestamp", F.current_timestamp())
    )
    
    return apply_dq_checks(
        df=transformed_df,
        table_name="orders",
        layer="silver",
        write_results=True
    )


# ============================================================================
# GOLD LAYER - Business-Level Aggregates
# ============================================================================

@dlt.table(
    name="gold_customer_summary",
    comment="Customer summary with order metrics",
    table_properties={
        "quality": "gold"
    }
)
def gold_customer_summary():
    """Create customer summary with order aggregations."""
    customers_df = dlt.read("silver_customers")
    orders_df = dlt.read("silver_orders")
    
    summary_df = (
        customers_df.alias("c")
        .join(
            orders_df.alias("o"),
            F.col("c.customer_id") == F.col("o.customer_id"),
            "left"
        )
        .groupBy(
            F.col("c.customer_id"),
            F.col("c.email"),
            F.col("c.full_name")
        )
        .agg(
            F.count("o.order_id").alias("total_orders"),
            F.sum("o.order_amount").alias("total_revenue"),
            F.avg("o.order_amount").alias("avg_order_value"),
            F.max("o.order_date").alias("last_order_date"),
            F.min("o.order_date").alias("first_order_date")
        )
        .withColumn("customer_lifetime_days",
                   F.datediff(F.col("last_order_date"), F.col("first_order_date")))
    )
    
    return summary_df


# ============================================================================
# DATA QUALITY MONITORING VIEWS
# ============================================================================

@dlt.table(
    name="dq_metrics_summary",
    comment="Data quality metrics aggregated by table and check"
)
def dq_metrics_summary():
    """Aggregate DQ metrics for monitoring dashboard."""
    return spark.sql("""
        SELECT 
            table_name,
            layer,
            check_name,
            severity,
            DATE(execution_date) as date,
            COUNT(*) as check_executions,
            AVG(CAST(failure_rate AS DOUBLE)) as avg_failure_rate,
            MAX(CAST(failure_rate AS DOUBLE)) as max_failure_rate,
            SUM(failure_count) as total_failures,
            SUM(total_rows) as total_rows_checked
        FROM main.dq_monitoring.dq_metrics
        WHERE execution_date >= current_date() - INTERVAL 30 DAYS
        GROUP BY table_name, layer, check_name, severity, DATE(execution_date)
        ORDER BY date DESC, avg_failure_rate DESC
    """)


@dlt.table(
    name="dq_quarantine_alerts",
    comment="Tables exceeding quarantine thresholds"
)
def dq_quarantine_alerts():
    """Identify tables with high quarantine rates."""
    return spark.sql("""
        SELECT 
            table_name,
            layer,
            execution_date,
            total_rows,
            error_rows,
            CAST(pass_rate AS DOUBLE) as pass_rate,
            CASE 
                WHEN CAST(pass_rate AS DOUBLE) < 95 THEN 'CRITICAL'
                WHEN CAST(pass_rate AS DOUBLE) < 98 THEN 'WARNING'
                ELSE 'OK'
            END as alert_level
        FROM main.dq_monitoring.dq_summary
        WHERE execution_date >= current_date() - INTERVAL 7 DAYS
            AND CAST(pass_rate AS DOUBLE) < 98
        ORDER BY pass_rate ASC, execution_date DESC
    """)
