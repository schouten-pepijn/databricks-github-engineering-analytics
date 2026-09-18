from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    bronze_schema: str = "github_analytics_bronze"
    silver_schema: str = "github_analytics_silver"
    gold_schema: str = "github_analytics_gold"
    control_schema: str = "github_analytics_control"

    @property
    def bronze_issues_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.github_issues_raw"

    @property
    def silver_issues_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.github_issues"

    @property
    def watermark_table(self) -> str:
        return f"{self.catalog}.{self.control_schema}.ingestion_watermark"
