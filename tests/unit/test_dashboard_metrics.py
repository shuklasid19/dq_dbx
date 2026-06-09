"""Unit tests for dashboard metrics."""

import pytest
from unittest.mock import Mock, MagicMock
from datetime import datetime

from dq_accelerator.dashboard.metrics import DashboardMetrics


class TestDashboardMetrics:
    """Tests for DashboardMetrics class."""

    @pytest.fixture
    def mock_spark(self):
        """Create mock SparkSession."""
        spark = Mock()
        spark.sql = Mock()
        return spark

    @pytest.fixture
    def metrics(self, mock_spark):
        """Create DashboardMetrics instance."""
        return DashboardMetrics(
            spark=mock_spark,
            catalog="test_catalog",
            schema="test_schema"
        )

    def test_get_overall_quality_score(self, metrics, mock_spark):
        """Test getting overall quality score."""
        # Mock result
        mock_result = Mock()
        mock_result.__getitem__ = Mock(return_value=95.5)
        mock_spark.sql.return_value.first.return_value = mock_result
        
        score = metrics.get_overall_quality_score(days=7)
        
        assert score == 95.5
        mock_spark.sql.assert_called_once()
        assert "AVG" in mock_spark.sql.call_args[0][0]
        assert "INTERVAL 7 DAYS" in mock_spark.sql.call_args[0][0]

    def test_get_sla_compliance_rate(self, metrics, mock_spark):
        """Test getting SLA compliance rate."""
        mock_result = {"compliance_rate": 87.5}
        mock_spark.sql.return_value.first.return_value = mock_result
        
        rate = metrics.get_sla_compliance_rate(sla_threshold=95.0, days=7)
        
        assert rate == 87.5
        assert "95.0" in mock_spark.sql.call_args[0][0]

    def test_get_tables_monitored(self, metrics, mock_spark):
        """Test getting count of monitored tables."""
        mock_result = {"table_count": 42}
        mock_spark.sql.return_value.first.return_value = mock_result
        
        count = metrics.get_tables_monitored(days=7)
        
        assert count == 42
        assert "COUNT(DISTINCT table_name)" in mock_spark.sql.call_args[0][0]

    def test_get_active_issues(self, metrics, mock_spark):
        """Test getting count of active issues."""
        mock_result = {"issue_count": 5}
        mock_spark.sql.return_value.first.return_value = mock_result
        
        count = metrics.get_active_issues()
        
        assert count == 5
        assert "error_rows > 0" in mock_spark.sql.call_args[0][0]
        assert "24 HOURS" in mock_spark.sql.call_args[0][0]

    def test_get_quality_trend(self, metrics, mock_spark):
        """Test getting quality trend."""
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        
        result = metrics.get_quality_trend(days=30)
        
        assert result == mock_df
        assert "DATE(execution_date)" in mock_spark.sql.call_args[0][0]

    def test_get_top_failing_tables(self, metrics, mock_spark):
        """Test getting top failing tables."""
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        
        result = metrics.get_top_failing_tables(limit=10)
        
        assert result == mock_df
        assert "LIMIT 10" in mock_spark.sql.call_args[0][0]
        assert "ORDER BY pass_rate ASC" in mock_spark.sql.call_args[0][0]
