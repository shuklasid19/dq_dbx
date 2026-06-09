"""Integration tests for dashboard."""

import pytest
from dq_accelerator.dashboard.views import DashboardViewCreator
from dq_accelerator.dashboard.metrics import DashboardMetrics


@pytest.mark.integration
def test_dashboard_views_creation(spark, test_schema_manager):
    """Test creating dashboard views in test environment."""

    # Create test schema
    test_schema = test_schema_manager.create_test_schema("test_dashboard")

    view_creator = DashboardViewCreator(
        spark=spark,
        catalog="",
        schema=test_schema
    )
    view_creator.full_schema = test_schema
    
    # Create sample metrics data
    metrics_data = [
        ("exec1", "2024-01-15", "customers", "bronze", "is_not_null", "error", 1000, 10, 1.0, 100),
        ("exec2", "2024-01-16", "customers", "bronze", "is_not_null", "error", 1000, 5, 0.5, 95),
    ]
    
    metrics_df = spark.createDataFrame(
        metrics_data,
        ["execution_id", "execution_date", "table_name", "layer", "check_name", 
         "severity", "total_rows", "failure_count", "failure_rate", "execution_duration_ms"]
    )
    
    summary_data = [
        ("exec1", "2026-01-25", "customers", "bronze", 1000, 990, 10, 0, 99.0, 500),
        ("exec2", "2026-01-25", "customers", "bronze", 1000, 995, 5, 0, 99.5, 480),
    ]
    
    summary_df = spark.createDataFrame(
        summary_data,
        ["execution_id", "execution_date", "table_name", "layer", "total_rows",
         "valid_rows", "error_rows", "warning_rows", "pass_rate", "execution_duration_ms"]
    )
    
    # Write test data
    metrics_df.write.mode("overwrite").saveAsTable(f"{test_schema}.dq_metrics")
    summary_df.write.mode("overwrite").saveAsTable(f"{test_schema}.dq_summary")
    
    # Create views
    try:
        view_creator.create_daily_quality_scores()
        view_creator.create_table_health_status()
        
        # Verify views were created
        daily_scores = spark.sql(f"SELECT * FROM {test_schema}.vw_daily_quality_scores")
        assert daily_scores.count() > 0
        
        health_status = spark.sql(f"SELECT * FROM {test_schema}.vw_table_health_status")
        assert health_status.count() > 0
        
    finally:
        pass


@pytest.mark.integration
def test_dashboard_metrics_calculation(spark, test_schema_manager):
    """Test dashboard metrics calculations."""
    # Create test schema
    test_schema = test_schema_manager.create_test_schema("test_dashboard")
    
    summary_data = [
        ("exec1", "2026-01-25", "customers", "bronze", 1000, 980, 20, 0, 98.0, 500),
        ("exec2", "2026-01-25", "orders", "bronze", 2000, 1900, 100, 0, 95.0, 600),
    ]
    
    summary_df = spark.createDataFrame(
        summary_data,
        ["execution_id", "execution_date", "table_name", "layer", "total_rows",
         "valid_rows", "error_rows", "warning_rows", "pass_rate", "execution_duration_ms"]
    )
    
    summary_df.write.mode("overwrite").saveAsTable(f"{test_schema}.dq_summary")
    
    try:
        metrics = DashboardMetrics(
            spark=spark,
            catalog="test_catalog",
            schema="test_schema"
        )
        
        metrics.full_schema = test_schema

        # Test metrics calculations
        quality_score = metrics.get_overall_quality_score(days=7)
        assert quality_score > 0
        
        tables_monitored = metrics.get_tables_monitored(days=7)
        assert tables_monitored == 2
        
        active_issues = metrics.get_active_issues()
        assert active_issues >= 0
        
    finally:
        pass
        # spark.sql("DROP SCHEMA IF EXISTS test_catalog.test_schema CASCADE")
