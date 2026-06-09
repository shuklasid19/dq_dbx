"""SQL queries for Databricks SQL Dashboard."""

from typing import Dict

# Dashboard SQL queries that can be used directly in Databricks SQL
DASHBOARD_QUERIES: Dict[str, str] = {
    "overall_quality_score": """
-- Overall Data Quality Score (Last 7 Days)
SELECT 
    ROUND(AVG(CAST(pass_rate AS DOUBLE)), 2) as quality_score,
    CASE 
        WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 98 THEN '🟢 Excellent'
        WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 95 THEN '🟡 Good'
        WHEN AVG(CAST(pass_rate AS DOUBLE)) >= 90 THEN '🟠 Fair'
        ELSE '🔴 Poor'
    END as status
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 7 DAYS
""",

    "tables_monitored_counter": """
-- Total Tables Monitored
SELECT COUNT(DISTINCT table_name) as table_count
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 7 DAYS
""",

    "active_issues_counter": """
-- Active Issues (Last 24 Hours)
SELECT COUNT(DISTINCT table_name) as issue_count
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_timestamp() - INTERVAL 24 HOURS
    AND error_rows > 0
""",

    "sla_compliance_gauge": """
-- SLA Compliance Rate (95% Threshold)
SELECT 
    ROUND(
        SUM(CASE WHEN CAST(pass_rate AS DOUBLE) >= 95 THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        2
    ) as compliance_pct
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 7 DAYS
""",

    "quality_trend_chart": """
-- Quality Score Trend (30 Days)
SELECT 
    DATE(execution_date) as date,
    ROUND(AVG(CAST(pass_rate AS DOUBLE)), 2) as quality_score,
    COUNT(DISTINCT table_name) as tables_checked
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE(execution_date)
ORDER BY date
""",

    "layer_quality_comparison": """
-- Quality by Layer
SELECT 
    layer,
    COUNT(DISTINCT table_name) as tables,
    ROUND(AVG(CAST(pass_rate AS DOUBLE)), 2) as avg_quality_score,
    SUM(total_rows) as total_rows,
    SUM(error_rows) as error_rows
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 7 DAYS
GROUP BY layer
ORDER BY layer
""",

    "top_failing_tables": """
-- Top 10 Failing Tables
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
    FROM main.dq_monitoring.dq_summary
    WHERE execution_date >= current_date() - INTERVAL 7 DAYS
)
SELECT 
    table_name,
    layer,
    ROUND(pass_rate, 2) as pass_rate,
    error_rows,
    total_rows
FROM latest_metrics
WHERE rn = 1 AND error_rows > 0
ORDER BY pass_rate ASC
LIMIT 10
""",

    "failure_heatmap": """
-- Failure Rate Heatmap (Table x Day)
SELECT 
    table_name,
    DATE(execution_date) as date,
    ROUND((error_rows * 100.0 / NULLIF(total_rows, 0)), 2) as error_rate
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 14 DAYS
    AND error_rows > 0
ORDER BY date DESC, error_rate DESC
""",

    "quarantine_volume_chart": """
-- Quarantine Volume Trend
SELECT 
    DATE(execution_date) as date,
    table_name,
    SUM(error_rows) as quarantined_rows
FROM main.dq_monitoring.dq_summary
WHERE execution_date >= current_date() - INTERVAL 30 DAYS
    AND error_rows > 0
GROUP BY DATE(execution_date), table_name
ORDER BY date DESC, quarantined_rows DESC
""",

    "check_performance_distribution": """
-- Check Execution Time Distribution
SELECT 
    check_name,
    COUNT(*) as execution_count,
    ROUND(AVG(execution_duration_ms) / 1000.0, 2) as avg_duration_sec,
    ROUND(PERCENTILE(execution_duration_ms, 0.95) / 1000.0, 2) as p95_duration_sec
FROM main.dq_monitoring.dq_metrics
WHERE execution_date >= current_date() - INTERVAL 7 DAYS
    AND execution_duration_ms IS NOT NULL
GROUP BY check_name
ORDER BY avg_duration_sec DESC
LIMIT 20
""",

    "error_warning_breakdown": """
-- Error vs Warning Breakdown
SELECT 
    DATE(execution_date) as date,
    SUM(CASE WHEN severity = 'error' THEN failure_count ELSE 0 END) as errors,
    SUM(CASE WHEN severity = 'warn' THEN failure_count ELSE 0 END) as warnings
FROM main.dq_monitoring.dq_metrics
WHERE execution_date >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE(execution_date)
ORDER BY date
""",

    "table_health_cards": """
-- Table Health Status Cards
SELECT 
    table_name,
    layer,
    ROUND(CAST(pass_rate AS DOUBLE), 2) as pass_rate,
    error_rows,
    warning_rows,
    total_rows,
    CASE 
        WHEN error_rows > 0 THEN '🔴 Critical'
        WHEN warning_rows > total_rows * 0.05 THEN '🟡 Warning'
        WHEN CAST(pass_rate AS DOUBLE) >= 98 THEN '🟢 Healthy'
        ELSE '🟠 Degraded'
    END as health_status,
    DATEDIFF(CURRENT_DATE(), DATE(execution_date)) as days_since_check
FROM main.dq_monitoring.vw_table_health_status
ORDER BY 
    CASE health_status 
        WHEN '🔴 Critical' THEN 1
        WHEN '🟡 Warning' THEN 2
        WHEN '🟠 Degraded' THEN 3
        ELSE 4
    END,
    pass_rate ASC
""",

    "anomaly_detection": """
-- Anomaly Detection (Significant Quality Drops)
WITH quality_baseline AS (
    SELECT 
        table_name,
        layer,
        AVG(CAST(pass_rate AS DOUBLE)) as baseline_quality,
        STDDEV(CAST(pass_rate AS DOUBLE)) as quality_stddev
    FROM main.dq_monitoring.dq_summary
    WHERE execution_date >= current_date() - INTERVAL 30 DAYS
        AND execution_date < current_date() - INTERVAL 1 DAY
    GROUP BY table_name, layer
)
SELECT 
    s.table_name,
    s.layer,
    ROUND(CAST(s.pass_rate AS DOUBLE), 2) as current_quality,
    ROUND(b.baseline_quality, 2) as baseline_quality,
    ROUND(CAST(s.pass_rate AS DOUBLE) - b.baseline_quality, 2) as quality_drop
FROM main.dq_monitoring.dq_summary s
INNER JOIN quality_baseline b
    ON s.table_name = b.table_name AND s.layer = b.layer
WHERE s.execution_date >= current_date() - INTERVAL 1 DAY
    AND CAST(s.pass_rate AS DOUBLE) < b.baseline_quality - (2 * b.quality_stddev)
ORDER BY quality_drop ASC
""",
}


def get_query(query_name: str) -> str:
    """
    Get dashboard query by name.

    Args:
        query_name: Name of the query

    Returns:
        SQL query string

    Raises:
        KeyError: If query name not found
    """
    return DASHBOARD_QUERIES[query_name]


def get_all_query_names() -> list:
    """Get list of all available query names."""
    return list(DASHBOARD_QUERIES.keys())
