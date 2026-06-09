"""Script to set up DQ dashboard views and publish to Databricks SQL."""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from dq_accelerator.dashboard.views import DashboardViewCreator
from dq_accelerator.dashboard.metrics import DashboardMetrics
from dq_accelerator.utils.logger import get_logger

logger = get_logger(__name__)


def setup_dashboard(
    catalog: str = "main",
    schema: str = "dq_monitoring",
    recreate: bool = False,
) -> None:
    """
    Set up dashboard views in Databricks.

    Args:
        catalog: Unity Catalog name
        schema: Schema name
        recreate: If True, drop and recreate all views
    """
    logger.info(f"Setting up DQ Dashboard in {catalog}.{schema}")
    
    # Initialize Spark
    spark = SparkSession.builder.appName("DQ-Dashboard-Setup").getOrCreate()
    
    # Create view creator
    view_creator = DashboardViewCreator(spark, catalog, schema)
    
    # Drop existing views if recreate flag is set
    if recreate:
        logger.info("Recreating views (dropping existing)")
        view_creator.drop_all_views()
    
    # Create all views
    view_creator.create_all_views()
    
    # Calculate and display summary metrics
    metrics = DashboardMetrics(spark, catalog, schema)
    
    logger.info("\n" + "="*80)
    logger.info("DASHBOARD SETUP COMPLETE")
    logger.info("="*80)
    
    logger.info(f"\n📊 Current Metrics:")
    logger.info(f"   Overall Quality Score: {metrics.get_overall_quality_score():.2f}%")
    logger.info(f"   SLA Compliance: {metrics.get_sla_compliance_rate():.2f}%")
    logger.info(f"   Tables Monitored: {metrics.get_tables_monitored()}")
    logger.info(f"   Active Issues: {metrics.get_active_issues()}")
    
    logger.info(f"\n✅ Views created in: {catalog}.{schema}")
    logger.info(f"\n📈 Next Steps:")
    logger.info(f"   1. Open Databricks SQL Editor")
    logger.info(f"   2. Create new dashboard")
    logger.info(f"   3. Add visualizations using the views in {catalog}.{schema}")
    logger.info(f"   4. Schedule dashboard refresh")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Set up DQ Dashboard")
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
        "--recreate",
        action="store_true",
        help="Drop and recreate existing views"
    )
    
    args = parser.parse_args()
    
    try:
        setup_dashboard(
            catalog=args.catalog,
            schema=args.schema,
            recreate=args.recreate
        )
    except Exception as e:
        logger.error(f"Failed to set up dashboard: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
