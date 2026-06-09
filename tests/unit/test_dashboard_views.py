"""Unit tests for dashboard views."""

import pytest
from unittest.mock import Mock, patch, call

from dq_accelerator.dashboard.views import DashboardViewCreator


class TestDashboardViewCreator:
    """Tests for DashboardViewCreator class."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        return Mock()

    @pytest.fixture
    def view_creator(self, mock_spark):
        """Create DashboardViewCreator instance."""
        return DashboardViewCreator(
            spark=mock_spark,
            catalog="test_catalog",
            schema="test_schema"
        )

    def test_initialization(self, view_creator):
        """Test view creator initialization."""
        assert view_creator.catalog == "test_catalog"
        assert view_creator.schema == "test_schema"
        assert view_creator.full_schema == "test_catalog.test_schema"

    def test_create_daily_quality_scores(self, view_creator, mock_spark):
        """Test creating daily quality scores view."""
        view_creator.create_daily_quality_scores()
        
        # Verify SQL was executed
        mock_spark.sql.assert_called_once()
        sql_query = mock_spark.sql.call_args[0][0]
        
        # Verify view name
        assert "CREATE OR REPLACE VIEW" in sql_query
        assert "vw_daily_quality_scores" in sql_query
        
        # Verify key columns
        assert "avg_quality_score" in sql_query
        assert "weighted_quality_score" in sql_query
        assert "quality_grade" in sql_query

    def test_create_table_health_status(self, view_creator, mock_spark):
        """Test creating table health status view."""
        view_creator.create_table_health_status()
        
        mock_spark.sql.assert_called_once()
        sql_query = mock_spark.sql.call_args[0][0]
        
        assert "vw_table_health_status" in sql_query
        assert "health_status" in sql_query
        assert "freshness_status" in sql_query

    def test_create_all_views(self, view_creator, mock_spark):
        """Test creating all views."""
        view_creator.create_all_views()
        
        # Should create 10 views
        assert mock_spark.sql.call_count == 10
        
        # Verify all view names were created
        view_names = [
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
        
        calls = [str(call) for call in mock_spark.sql.call_args_list]
        for view_name in view_names:
            assert any(view_name in call for call in calls)

    def test_drop_all_views(self, view_creator, mock_spark):
        """Test dropping all views."""
        view_creator.drop_all_views()
        
        # Should drop 10 views
        assert mock_spark.sql.call_count == 10
        
        # Verify DROP VIEW statements
        for call_obj in mock_spark.sql.call_args_list:
            sql = call_obj[0][0]
            assert "DROP VIEW IF EXISTS" in sql
