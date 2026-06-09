"""Dashboard view creator for Databricks SQL."""

from typing import Optional
from pyspark.sql import SparkSession

from dq_accelerator.utils.logger import get_logger
from dq_accelerator.utils.exceptions import DQAcceleratorError

logger = get_logger(__name__)


class DashboardViewCreator:
    """Creates and manages SQL views for DQ dashboard."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "main",
        schema: str = "dq_monitoring",
    ):
        """
        Initialize dashboard view creator.

        Args:
            spark: SparkSession instance
            catalog: Unity Catalog name
            schema: Schema name for views
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.full_schema = f"{catalog}.{schema}"

    def create_all_views(self) -> None:
        """Create all dashboard views."""
        logger.info(f"Creating dashboard views in {self.full_schema}")
        
        self.create_daily_quality_scores()
        self.create_check_failure_trends()
        self.create_table_health_status()
        self.create_layer_quality_comparison()
        self.create_sla_compliance_tracking()
        self.create_quarantine_volume_tracking()
        self.create_check_performance_metrics()
        self.create_error_pattern_analysis()
        self.create_data_freshness_monitoring()
        self.create_business_impact_scores()
        
        logger.info("Successfully created all dashboard views")

    def create_daily_quality_scores(self) -> None:
        """Create view for daily quality score trends."""
        view_name = f"{self.full_schema}.vw_daily_quality_scores"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            DATE(execution_date) as date,
            layer,
            COUNT(DISTINCT table_name) as tables_checked,
            AVG(CAST(pass_rate AS DOUBLE)) as avg_quality_score,
            SUM(total_rows) as total_rows_processed,
            SUM(error_rows) as total_error_rows,
            SUM(warning_rows) as total_warning_rows,
            
            -- Weighted quality score (gives more weight to larger tables)
            SUM(CAST(pass_rate AS DOUBLE) * total_rows) / NULLIF(SUM(total_rows), 0) as weighted_quality_score,
            
            -- Error rate
            (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) as error_rate,
            
            -- Warning rate
            (SUM(warning_rows) * 100.0) / NULLIF(SUM(total_rows), 0) as warning_rate,
            
            -- Quality trend indicator
            CASE 
                WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 98 THEN 'Excellent'
                WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 95 THEN 'Good'
                WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 90 THEN 'Fair'
                ELSE 'Poor'
            END as quality_grade,
            
            current_timestamp() as view_refreshed_at
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL 90 DAYS
        GROUP BY DATE(execution_date), layer
        ORDER BY date DESC, layer
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_check_failure_trends(self) -> None:
        """Create view for check-level failure trends."""
        view_name = f"{self.full_schema}.vw_check_failure_trends"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            DATE(execution_date) as date,
            table_name,
            layer,
            check_name,
            severity,
            
            -- Aggregated metrics
            COUNT(*) as execution_count,
            AVG(CAST(failure_rate AS DOUBLE)) as avg_failure_rate,
            MAX(CAST(failure_rate AS DOUBLE)) as max_failure_rate,
            MIN(CAST(failure_rate AS DOUBLE)) as min_failure_rate,
            SUM(failure_count) as total_failures,
            SUM(total_rows) as total_rows_checked,
            
            -- Trend indicators
            CASE 
                WHEN AVG(CAST(failure_rate AS DOUBLE)) > 5 THEN 'Critical'
                WHEN AVG(CAST(failure_rate AS DOUBLE)) > 2 THEN 'Warning'
                ELSE 'Healthy'
            END as health_status,
            
            -- Stability metric (coefficient of variation)
            STDDEV(CAST(failure_rate AS DOUBLE)) / NULLIF(AVG(CAST(failure_rate AS DOUBLE)), 0) as failure_rate_volatility,
            
            current_timestamp() as view_refreshed_at
        FROM {self.full_schema}.dq_metrics
        WHERE execution_date >= current_date() - INTERVAL 30 DAYS
        GROUP BY DATE(execution_date), table_name, layer, check_name, severity
        HAVING AVG(CAST(failure_rate AS DOUBLE)) > 0  -- Only show checks with failures
        ORDER BY date DESC, avg_failure_rate DESC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_table_health_status(self) -> None:
        """Create view for current table health status."""
        view_name = f"{self.full_schema}.vw_table_health_status"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest_execution AS (
            SELECT 
                table_name,
                layer,
                MAX(execution_date) as last_execution_date
            FROM {self.full_schema}.dq_summary
            WHERE execution_date >= current_date() - INTERVAL 7 DAYS
            GROUP BY table_name, layer
        ),
        latest_metrics AS (
            SELECT 
                s.table_name,
                s.layer,
                s.execution_date,
                s.total_rows,
                s.valid_rows,
                s.error_rows,
                s.warning_rows,
                CAST(s.pass_rate AS DOUBLE) as pass_rate,
                ROW_NUMBER() OVER (
                    PARTITION BY s.table_name, s.layer 
                    ORDER BY s.execution_date DESC
                ) as rn
            FROM {self.full_schema}.dq_summary s
            INNER JOIN latest_execution le 
                ON s.table_name = le.table_name 
                AND s.layer = le.layer
                AND s.execution_date = le.last_execution_date
        ),
        check_summary AS (
            SELECT 
                m.table_name,
                m.layer,
                COUNT(*) as total_checks,
                SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) as error_checks,
                SUM(CASE WHEN severity = 'warn' THEN 1 ELSE 0 END) as warning_checks,
                SUM(CASE WHEN CAST(failure_rate AS DOUBLE) > 0 THEN 1 ELSE 0 END) as failing_checks
            FROM {self.full_schema}.dq_metrics m
            INNER JOIN latest_execution le 
                ON m.table_name = le.table_name 
                AND m.layer = le.layer
                AND DATE(m.execution_date) = DATE(le.last_execution_date)
            GROUP BY m.table_name, m.layer
        )
        SELECT 
            lm.table_name,
            lm.layer,
            lm.execution_date as last_check_date,
            lm.total_rows,
            lm.valid_rows,
            lm.error_rows,
            lm.warning_rows,
            lm.pass_rate,
            
            -- Check statistics
            cs.total_checks,
            cs.error_checks,
            cs.warning_checks,
            cs.failing_checks,
            
            -- Health indicators
            CASE 
                WHEN lm.error_rows > 0 THEN 'Critical'
                WHEN lm.warning_rows > lm.total_rows * 0.05 THEN 'Warning'
                WHEN lm.pass_rate >= 98 THEN 'Healthy'
                ELSE 'Degraded'
            END as health_status,
            
            -- Data freshness
            DATEDIFF(CURRENT_DATE(), DATE(lm.execution_date)) as days_since_check,
            CASE 
                WHEN DATEDIFF(CURRENT_DATE(), DATE(lm.execution_date)) = 0 THEN 'Fresh'
                WHEN DATEDIFF(CURRENT_DATE(), DATE(lm.execution_date)) <= 1 THEN 'Recent'
                WHEN DATEDIFF(CURRENT_DATE(), DATE(lm.execution_date)) <= 3 THEN 'Stale'
                ELSE 'Critical'
            END as freshness_status,
            
            -- SLA compliance (assume 95% pass rate SLA)
            CASE WHEN lm.pass_rate >= 95 THEN 'Met' ELSE 'Missed' END as sla_status,
            
            current_timestamp() as view_refreshed_at
        FROM latest_metrics lm
        LEFT JOIN check_summary cs 
            ON lm.table_name = cs.table_name 
            AND lm.layer = cs.layer
        WHERE lm.rn = 1
        ORDER BY 
            CASE health_status 
                WHEN 'Critical' THEN 1
                WHEN 'Warning' THEN 2
                WHEN 'Degraded' THEN 3
                ELSE 4
            END,
            lm.pass_rate ASC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_layer_quality_comparison(self) -> None:
        """Create view comparing quality across layers."""
        view_name = f"{self.full_schema}.vw_layer_quality_comparison"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            layer,
            
            -- Table coverage
            COUNT(DISTINCT table_name) as tables_monitored,
            
            -- Volume metrics
            SUM(total_rows) as total_rows,
            SUM(valid_rows) as valid_rows,
            SUM(error_rows) as error_rows,
            SUM(warning_rows) as warning_rows,
            
            -- Quality metrics
            AVG(CAST(pass_rate AS DOUBLE)) as avg_pass_rate,
            MIN(CAST(pass_rate AS DOUBLE)) as min_pass_rate,
            MAX(CAST(pass_rate AS DOUBLE)) as max_pass_rate,
            
            -- Error statistics
            (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) as error_rate_pct,
            (SUM(warning_rows) * 100.0) / NULLIF(SUM(total_rows), 0) as warning_rate_pct,
            
            -- SLA compliance
            SUM(CASE WHEN CAST(pass_rate AS DOUBLE) >= 95 THEN 1 ELSE 0 END) * 100.0 
                / COUNT(*) as sla_compliance_pct,
            
            -- Trend indicators (comparing to last week)
            AVG(CAST(pass_rate AS DOUBLE)) - LAG(AVG(CAST(pass_rate AS DOUBLE))) 
                OVER (PARTITION BY layer ORDER BY DATE(execution_date)) as quality_change_pct,
            
            DATE(execution_date) as date,
            current_timestamp() as view_refreshed_at
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL 30 DAYS
        GROUP BY layer, DATE(execution_date)
        ORDER BY date DESC, layer
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_sla_compliance_tracking(self) -> None:
        """Create view for SLA compliance tracking."""
        view_name = f"{self.full_schema}.vw_sla_compliance_tracking"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH daily_compliance AS (
            SELECT 
                DATE(execution_date) as date,
                table_name,
                layer,
                CAST(pass_rate AS DOUBLE) as pass_rate,
                CASE 
                    WHEN CAST(pass_rate AS DOUBLE) >= 99 THEN 'Gold'
                    WHEN CAST(pass_rate AS DOUBLE) >= 95 THEN 'Silver'
                    WHEN CAST(pass_rate AS DOUBLE) >= 90 THEN 'Bronze'
                    ELSE 'Non-Compliant'
                END as sla_tier,
                total_rows,
                error_rows,
                warning_rows
            FROM {self.full_schema}.dq_summary
            WHERE execution_date >= current_date() - INTERVAL 90 DAYS
        )
        SELECT 
            date,
            layer,
            sla_tier,
            COUNT(DISTINCT table_name) as table_count,
            SUM(total_rows) as total_rows,
            SUM(error_rows) as total_errors,
            AVG(pass_rate) as avg_pass_rate,
            
            -- Compliance percentage
            COUNT(DISTINCT table_name) * 100.0 / 
                SUM(COUNT(DISTINCT table_name)) OVER (PARTITION BY date, layer) as pct_of_tables,
            
            -- Business impact (weighted by volume)
            SUM(total_rows * pass_rate / 100.0) as quality_weighted_volume,
            
            current_timestamp() as view_refreshed_at
        FROM daily_compliance
        GROUP BY date, layer, sla_tier
        ORDER BY date DESC, layer, 
            CASE sla_tier 
                WHEN 'Gold' THEN 1 
                WHEN 'Silver' THEN 2 
                WHEN 'Bronze' THEN 3 
                ELSE 4 
            END
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_quarantine_volume_tracking(self) -> None:
        """Create view for quarantine volume trends."""
        view_name = f"{self.full_schema}.vw_quarantine_volume_tracking"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT 
            DATE(execution_date) as date,
            table_name,
            layer,
            
            -- Volume metrics
            SUM(total_rows) as total_rows_processed,
            SUM(error_rows) as quarantined_rows,
            (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) as quarantine_rate_pct,
            
            -- Trend analysis
            SUM(error_rows) - LAG(SUM(error_rows)) 
                OVER (PARTITION BY table_name, layer ORDER BY DATE(execution_date)) as quarantine_change,
            
            -- Categorization
            CASE 
                WHEN (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) > 10 THEN 'Critical'
                WHEN (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) > 5 THEN 'High'
                WHEN (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) > 2 THEN 'Medium'
                WHEN (SUM(error_rows) * 100.0) / NULLIF(SUM(total_rows), 0) > 0 THEN 'Low'
                ELSE 'None'
            END as quarantine_severity,
            
            -- Moving averages
            AVG(SUM(error_rows)) OVER (
                PARTITION BY table_name, layer 
                ORDER BY DATE(execution_date) 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as quarantine_7day_avg,
            
            current_timestamp() as view_refreshed_at
        FROM {self.full_schema}.dq_summary
        WHERE execution_date >= current_date() - INTERVAL 90 DAYS
        GROUP BY DATE(execution_date), table_name, layer
        ORDER BY date DESC, quarantine_rate_pct DESC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_check_performance_metrics(self) -> None:
        """Create view for check execution performance."""
        view_name = f"{self.full_schema}.vw_check_performance_metrics"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH execution_stats AS (
            SELECT 
                table_name,
                layer,
                check_name,
                severity,
                COUNT(*) as execution_count,
                AVG(execution_duration_ms) as avg_duration_ms,
                MIN(execution_duration_ms) as min_duration_ms,
                MAX(execution_duration_ms) as max_duration_ms,
                STDDEV(execution_duration_ms) as stddev_duration_ms,
                PERCENTILE(execution_duration_ms, 0.50) as p50_duration_ms,
                PERCENTILE(execution_duration_ms, 0.95) as p95_duration_ms,
                PERCENTILE(execution_duration_ms, 0.99) as p99_duration_ms
            FROM {self.full_schema}.dq_metrics
            WHERE execution_date >= current_date() - INTERVAL 30 DAYS
                AND execution_duration_ms IS NOT NULL
            GROUP BY table_name, layer, check_name, severity
        )
        SELECT 
            table_name,
            layer,
            check_name,
            severity,
            execution_count,
            
            -- Duration metrics (in seconds)
            avg_duration_ms / 1000.0 as avg_duration_sec,
            min_duration_ms / 1000.0 as min_duration_sec,
            max_duration_ms / 1000.0 as max_duration_sec,
            p50_duration_ms / 1000.0 as p50_duration_sec,
            p95_duration_ms / 1000.0 as p95_duration_sec,
            p99_duration_ms / 1000.0 as p99_duration_sec,
            
            -- Performance indicators
            CASE 
                WHEN avg_duration_ms > 60000 THEN 'Slow'
                WHEN avg_duration_ms > 30000 THEN 'Moderate'
                ELSE 'Fast'
            END as performance_category,
            
            -- Consistency score (lower is more consistent)
            CASE 
                WHEN stddev_duration_ms / NULLIF(avg_duration_ms, 0) < 0.2 THEN 'Consistent'
                WHEN stddev_duration_ms / NULLIF(avg_duration_ms, 0) < 0.5 THEN 'Variable'
                ELSE 'Unstable'
            END as consistency_rating,
            
            current_timestamp() as view_refreshed_at
        FROM execution_stats
        ORDER BY avg_duration_ms DESC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_error_pattern_analysis(self) -> None:
        """Create view for error pattern analysis."""
        view_name = f"{self.full_schema}.vw_error_pattern_analysis"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH error_patterns AS (
            SELECT 
                DATE(execution_date) as date,
                check_name,
                severity,
                table_name,
                layer,
                failure_count,
                total_rows,
                CAST(failure_rate AS DOUBLE) as failure_rate
            FROM {self.full_schema}.dq_metrics
            WHERE execution_date >= current_date() - INTERVAL 30 DAYS
                AND failure_count > 0
        )
        SELECT 
            check_name,
            severity,
            
            -- Frequency metrics
            COUNT(DISTINCT table_name) as affected_tables,
            COUNT(DISTINCT date) as days_with_failures,
            COUNT(*) as total_occurrences,
            
            -- Impact metrics
            SUM(failure_count) as total_failures,
            AVG(failure_rate) as avg_failure_rate,
            MAX(failure_rate) as max_failure_rate,
            
            -- Pattern indicators
            CASE 
                WHEN COUNT(DISTINCT date) >= 25 THEN 'Chronic'
                WHEN COUNT(DISTINCT date) >= 15 THEN 'Recurring'
                WHEN COUNT(DISTINCT date) >= 5 THEN 'Intermittent'
                ELSE 'Sporadic'
            END as failure_pattern,
            
            -- Affected layers
            COLLECT_SET(layer) as affected_layers,
            
            current_timestamp() as view_refreshed_at
        FROM error_patterns
        GROUP BY check_name, severity
        HAVING COUNT(*) > 5  -- Only show patterns with multiple occurrences
        ORDER BY total_failures DESC, affected_tables DESC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_data_freshness_monitoring(self) -> None:
        """Create view for data freshness monitoring."""
        view_name = f"{self.full_schema}.vw_data_freshness_monitoring"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH latest_checks AS (
            SELECT 
                table_name,
                layer,
                MAX(execution_date) as last_check_date,
                COUNT(DISTINCT DATE(execution_date)) as days_with_checks
            FROM {self.full_schema}.dq_summary
            WHERE execution_date >= current_date() - INTERVAL 30 DAYS
            GROUP BY table_name, layer
        )
        SELECT 
            table_name,
            layer,
            last_check_date,
            days_with_checks,
            
            -- Freshness metrics
            DATEDIFF(CURRENT_TIMESTAMP(), last_check_date) as hours_since_check,
            DATEDIFF(CURRENT_DATE(), DATE(last_check_date)) as days_since_check,
            
            -- Frequency metrics
            days_with_checks * 100.0 / 30 as check_frequency_pct,
            
            -- Status indicators
            CASE 
                WHEN DATEDIFF(CURRENT_TIMESTAMP(), last_check_date) < 24 THEN 'Fresh'
                WHEN DATEDIFF(CURRENT_TIMESTAMP(), last_check_date) < 48 THEN 'Recent'
                WHEN DATEDIFF(CURRENT_TIMESTAMP(), last_check_date) < 168 THEN 'Stale'
                ELSE 'Critical'
            END as freshness_status,
            
            CASE 
                WHEN days_with_checks >= 28 THEN 'Daily'
                WHEN days_with_checks >= 12 THEN 'Regular'
                WHEN days_with_checks >= 4 THEN 'Weekly'
                ELSE 'Infrequent'
            END as check_cadence,
            
            current_timestamp() as view_refreshed_at
        FROM latest_checks
        ORDER BY days_since_check DESC, table_name
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def create_business_impact_scores(self) -> None:
        """Create view for business impact scoring."""
        view_name = f"{self.full_schema}.vw_business_impact_scores"
        
        sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        WITH impact_factors AS (
            SELECT 
                s.table_name,
                s.layer,
                s.execution_date,
                s.total_rows,
                s.error_rows,
                s.warning_rows,
                CAST(s.pass_rate AS DOUBLE) as pass_rate,
                
                -- Count critical checks
                COUNT(CASE WHEN m.severity = 'error' AND m.failure_count > 0 THEN 1 END) as critical_failures,
                COUNT(CASE WHEN m.severity = 'warn' AND m.failure_count > 0 THEN 1 END) as warning_failures,
                
                -- Downstream dependency score (placeholder - would be from lineage)
                CASE s.layer
                    WHEN 'gold' THEN 3  -- Gold tables have highest impact
                    WHEN 'silver' THEN 2
                    WHEN 'bronze' THEN 1
                END as layer_weight
                
            FROM {self.full_schema}.dq_summary s
            LEFT JOIN {self.full_schema}.dq_metrics m
                ON s.table_name = m.table_name
                AND s.layer = m.layer
                AND DATE(s.execution_date) = DATE(m.execution_date)
            WHERE s.execution_date >= current_date() - INTERVAL 7 DAYS
            GROUP BY s.table_name, s.layer, s.execution_date, 
                     s.total_rows, s.error_rows, s.warning_rows, s.pass_rate
        )
        SELECT 
            table_name,
            layer,
            DATE(execution_date) as date,
            
            -- Volume impact
            total_rows,
            error_rows,
            
            -- Quality metrics
            pass_rate,
            critical_failures,
            warning_failures,
            
            -- Business impact score (0-100, lower is worse)
            LEAST(100, GREATEST(0,
                pass_rate * 0.6 +                                    -- 60% weight on pass rate
                (100 - (critical_failures * 10)) * 0.3 +            -- 30% weight on critical failures
                (100 - (warning_failures * 2)) * 0.1                -- 10% weight on warnings
            )) * layer_weight / 3.0 as business_impact_score,
            
            -- Impact category
            CASE 
                WHEN error_rows > total_rows * 0.1 AND layer = 'gold' THEN 'Critical'
                WHEN error_rows > total_rows * 0.05 AND layer IN ('silver', 'gold') THEN 'High'
                WHEN error_rows > total_rows * 0.02 THEN 'Medium'
                WHEN error_rows > 0 THEN 'Low'
                ELSE 'None'
            END as impact_category,
            
            current_timestamp() as view_refreshed_at
        FROM impact_factors
        WHERE ROW_NUMBER() OVER (
            PARTITION BY table_name, layer 
            ORDER BY execution_date DESC
        ) = 1
        ORDER BY business_impact_score ASC, error_rows DESC
        """
        
        self.spark.sql(sql)
        logger.info(f"Created view: {view_name}")

    def drop_all_views(self) -> None:
        """Drop all dashboard views."""
        views = [
            "vw_daily_quality_scores",
            "vw_check_failure_trends",
            "vw_table_health_status",
            "vw_layer_quality_comparison",
            "vw_sla_compliance_tracking",
            "vw_quarantine_volume_tracking",
            "vw_check_performance_metrics",
            "vw_error_pattern_analysis",
            "vw_data_freshness_monitoring",
            "vw_business_impact_scores",
        ]
        
        for view in views:
            try:
                self.spark.sql(f"DROP VIEW IF EXISTS {self.full_schema}.{view}")
                logger.info(f"Dropped view: {view}")
            except Exception as e:
                logger.warning(f"Failed to drop view {view}: {e}")
