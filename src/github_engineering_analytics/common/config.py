"""Configuration that defines the project's Unity Catalog object names."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Build fully qualified table names from one catalog and layer schemas."""

    catalog: str
    bronze_schema: str = "github_analytics_bronze"
    silver_schema: str = "github_analytics_silver"
    gold_schema: str = "github_analytics_gold"
    control_schema: str = "github_analytics_control"

    @property
    def bronze_issues_table(self) -> str:
        """Return the Bronze table that stores raw GitHub issue responses."""
        return f"{self.catalog}.{self.bronze_schema}.github_issues_raw"

    @property
    def silver_issues_table(self) -> str:
        """Return the Silver table containing normalized GitHub issues."""
        return f"{self.catalog}.{self.silver_schema}.github_issues"

    @property
    def watermark_table(self) -> str:
        """Return the control table holding the latest committed watermark."""
        return f"{self.catalog}.{self.control_schema}.ingestion_watermark"

    @property
    def pipeline_runs_table(self) -> str:
        """Return the control table that records pipeline run lifecycle state."""
        return f"{self.catalog}.{self.control_schema}.pipeline_runs"
