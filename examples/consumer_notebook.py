# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Accelerator - Consumer Notebook Example
# MAGIC 
# MAGIC This notebook demonstrates how to use the DQ Accelerator in a typical Databricks workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install the DQ Accelerator wheel (if not already installed)
# %pip install /Workspace/Shared/wheels/dq_accelerator-1.0.0-py3-none-any.whl
# dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import SparkSession
from dq_accelerator import DQAccelerator
import pyspark.sql.functions as F
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Paths

# COMMAND ----------

# Define configuration paths
CONFIG_BASE_PATH = "/Workspace/Shared/dq_configs"
LAYER_CONFIG_PATH = f"{CONFIG_BASE_PATH}/bronze/layer_bronze.yaml"
TABLE_CONFIG_PATH = f"{CONFIG_BASE_PATH}/bronze/table_customers.yaml"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize DQ Accelerator

# COMMAND ----------

# Initialize the accelerator
dq_accelerator = DQAccelerator(spark=spark)

print("✅ DQ Accelerator initialized successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Data

# COMMAND ----------

# Load data from Unity Catalog
source_table = "main.raw.raw_customers"
df = spark.table(source_table)

print(f"📊 Loaded {df.count():,} rows from {source_table}")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Data Quality Checks

# COMMAND ----------

# Run DQ checks with config files
results = dq_accelerator.run_checks_from_config_files(
    df=df,
    table_config_path=TABLE_CONFIG_PATH,
    layer_config_path=LAYER_CONFIG_PATH,
    split_valid_invalid=True,
    write_results=True,
    execution_id=f"customer_dq_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Results

# COMMAND ----------

# Valid data
valid_df = results["result"]
print(f"✅ Valid records: {valid_df.count():,}")

# Quarantined data
quarantine_df = results["quarantine"]
print(f"❌ Quarantined records: {quarantine_df.count():,}")

# Display quarantined records
if quarantine_df.count() > 0:
    print("\n🔍 Sample quarantined records:")
    display(quarantine_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Metrics

# COMMAND ----------

# Display metrics
metrics_df = results["metrics"]
print(f"📈 Generated {metrics_df.count()} metric records")
display(metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Summary

# COMMAND ----------

# Display summary
summary_df = results["summary"]
display(summary_df)

# Summary statistics
summary_row = summary_df.first()
print(f"""
📊 Data Quality Summary:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Table:           {summary_row['table_name']}
Layer:           {summary_row['layer']}
Total Rows:      {summary_row['total_rows']:,}
Valid Rows:      {summary_row['valid_rows']:,}
Error Rows:      {summary_row['error_rows']:,}
Warning Rows:    {summary_row['warning_rows']:,}
Pass Rate:       {summary_row['pass_rate']}%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Valid Data

# COMMAND ----------

# Continue processing with valid data only
processed_df = (
    valid_df
    .withColumn("processed_timestamp", F.current_timestamp())
    .withColumn("data_quality_passed", F.lit(True))
)

# Write to next layer (if needed)
# processed_df.write.mode("overwrite").saveAsTable("main.bronze.customers")

print(f"✅ Processed {processed_df.count():,} valid records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Historical Metrics

# COMMAND ----------

# Query metrics table for trend analysis
metrics_history = spark.sql("""
    SELECT 
        DATE(execution_date) as date,
        table_name,
        check_name,
        severity,
        AVG(CAST(failure_rate AS DOUBLE)) as avg_failure_rate,
        SUM(failure_count) as total_failures
    FROM main.dq_monitoring.dq_metrics
    WHERE table_name = 'customers'
        AND execution_date >= current_date() - INTERVAL 7 DAYS
    GROUP BY DATE(execution_date), table_name, check_name, severity
    ORDER BY date DESC, avg_failure_rate DESC
""")

display(metrics_history)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigate Specific Quarantined Records

# COMMAND ----------

if quarantine_df.count() > 0:
    # Analyze error patterns
    error_analysis = (
        quarantine_df
        .select(F.explode("_errors").alias("error_message"))
        .groupBy("error_message")
        .count()
        .orderBy(F.desc("count"))
    )
    
    print("🔍 Error Pattern Analysis:")
    display(error_analysis)
    
    # Analyze warning patterns
    if "_warnings" in quarantine_df.columns:
        warning_analysis = (
            quarantine_df
            .filter(F.size(F.col("_warnings")) > 0)
            .select(F.explode("_warnings").alias("warning_message"))
            .groupBy("warning_message")
            .count()
            .orderBy(F.desc("count"))
        )
        
        print("⚠️ Warning Pattern Analysis:")
        display(warning_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Results for Review

# COMMAND ----------

# Export quarantined data for manual review
if quarantine_df.count() > 0:
    export_path = "/Workspace/Shared/dq_exports/customers_quarantine"
    
    quarantine_df.write.mode("overwrite").format("delta").save(export_path)
    
    print(f"📤 Exported quarantined data to: {export_path}")
