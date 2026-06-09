"""Dashboard metrics calculator."""

from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


class DashboardMetrics:
    """Calculate dashboard metrics from DQ data."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "main",
        schema: str = "dq_monitoring",
    ):
        """
        Initialize dashboard metrics calculator.

        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"

    def get_overall_quality_score(self, days: int = 7) -> float:
        """
        Calculate overall quality score across all tables.

        Args:
            days: Number of days to look back

        Returns:
            Quality score (0-100)
        """
        query = f"""
        SELECT 
            AVG(CAST(pass_rate AS DOUBLE)) as overall_score
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
        """
        
        result = self.spark.sql(query).first()
        return round(result["overall_score"], 2) if result and result["overall_score"] else 0.0

    def get_sla_compliance_rate(self, sla_threshold: float = 95.0, days: int = 7) -> float:
        """
        Calculate SLA compliance rate.

        Args:
            sla_threshold: Minimum pass rate for SLA compliance
            days: Number of days to look back

        Returns:
            SLA compliance percentage
        """
        query = f"""
        SELECT 
            SUM(CASE WHEN CAST(pass_rate AS DOUBLE) >= {sla_threshold} THEN 1 ELSE 0 END) * 100.0 
                / COUNT(*) as compliance_rate
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
        """
        
        result = self.spark.sql(query).first()
        return round(result["compliance_rate"], 2) if result and result["compliance_rate"] else 0.0

    def get_tables_monitored(self, days: int = 7) -> int:
        """
        Get count of tables monitored.

        Args:
            days: Number of days to look back

        Returns:
            Number of unique tables
        """
        query = f"""
        SELECT COUNT(DISTINCT table_name) as table_count
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
        """
        
        result = self.spark.sql(query).first()
        return result["table_count"] if result else 0

    def get_active_issues(self) -> int:
        """
        Get count of active issues (tables with errors in last 24 hours).

        Returns:
            Number of tables with active issues
        """
        query = f"""
        SELECT COUNT(DISTINCT table_name) as issue_count
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_timestamp() - INTERVAL 24 HOURS
            AND error_rows > 0
        """
        
        result = self.spark.sql(query).first()
        return result["issue_count"] if result else 0

    def get_quality_trend(self, days: int = 30) -> DataFrame:
        """
        Get quality score trend over time.

        Args:
            days: Number of days to look back

        Returns:
            DataFrame with daily quality scores
        """
        query = f"""
        SELECT 
            DATE(execution_date) as date,
            AVG(CAST(pass_rate AS DOUBLE)) as quality_score,
            SUM(total_rows) as rows_processed,
            SUM(error_rows) as error_rows
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
        GROUP BY DATE(execution_date)
        ORDER BY date
        """
        
        return self.spark.sql(query)

    def get_top_failing_tables(self, limit: int = 10) -> DataFrame:
        """
        Get tables with highest failure rates.

        Args:
            limit: Number of tables to return

        Returns:
            DataFrame with top failing tables
        """
        query = f"""
        WITH latest_metrics AS (
            SELECT 
                table_name,
                layer,
                CAST(pass_rate AS DOUBLE) as pass_rate,
                error_rows,
                total_rows,
                ROW_NUMBER() OVER (
                    PARTITION BY table_name, layer 
                    ORDER BY execution_date DESC
                ) as rn
            FROM {self.full_schema}.dq_summary
            WHERE execution_date >= current_date() - INTERVAL 7 DAYS
        )
        SELECT 
            table_name,
            layer,
            pass_rate,
            error_rows,
            total_rows,
            (error_rows * 100.0 / NULLIF(total_rows, 0)) as error_rate
        FROM latest_metrics
        WHERE rn = 1 AND error_rows > 0
        ORDER BY pass_rate ASC
        LIMIT {limit}
        """
        
        return self.spark.sql(query)

    def get_layer_comparison(self, days: int = 7) -> DataFrame:
        """
        Compare quality metrics across layers.

        Args:
            days: Number of days to look back

        Returns:
            DataFrame with layer-wise metrics
        """
        query = f"""
        SELECT 
            layer,
            COUNT(DISTINCT table_name) as tables,
            AVG(CAST(pass_rate AS DOUBLE)) as avg_quality_score,
            SUM(total_rows) as total_rows,
            SUM(error_rows) as error_rows,
            (SUM(error_rows) * 100.0 / NULLIF(SUM(total_rows), 0)) as error_rate
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
        GROUP BY layer
        ORDER BY layer
        """
        
        return self.spark.sql(query)

    def get_quarantine_summary(self, days: int = 7) -> DataFrame:
        """
        Get quarantine volume summary.

        Args:
            days: Number of days to look back

        Returns:
            DataFrame with quarantine metrics
        """
        query = f"""
        SELECT 
            DATE(execution_date) as date,
            table_name,
            layer,
            SUM(error_rows) as quarantined_rows,
            SUM(total_rows) as total_rows,
            (SUM(error_rows) * 100.0 / NULLIF(SUM(total_rows), 0)) as quarantine_rate
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL {days} DAYS
            AND error_rows > 0
        GROUP BY DATE(execution_date), table_name, layer
        ORDER BY date DESC, quarantined_rows DESC
        """
        
        return self.spark.sql(query)

    def export_metrics_snapshot(self, output_path: str) -> None:
        """
        Export current metrics snapshot to Delta table.

        Args:
            output_path: Path to write snapshot
        """
        metrics = {
            "snapshot_timestamp": datetime.now(),
            "overall_quality_score": self.get_overall_quality_score(),
            "sla_compliance_rate": self.get_sla_compliance_rate(),
            "tables_monitored": self.get_tables_monitored(),
            "active_issues": self.get_active_issues(),
        }
        
        df = self.spark.createDataFrame([metrics])
        df.write.mode("append").format("delta").save(output_path)
        
        logger.info(f"Exported metrics snapshot to {output_path}")
