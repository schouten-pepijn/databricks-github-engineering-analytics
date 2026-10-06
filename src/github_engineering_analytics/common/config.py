"""Configuration that defines the project's Unity Catalog object names.

The current physical tables have deliberately distinct responsibilities:

* Bronze preserves append-only GitHub source evidence.
* Silver exposes the current normalized Issue, User and Label entities.
* Control stores pipeline state and is never an analytical source.

The dbt-owned Gold layer models Silver Issues as ``fact_issue``, Users and
Labels as dimensions, and Issue-to-Label associations as a bridge table.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    """Build fully qualified names while keeping layer responsibilities explicit."""

    catalog: str
    bronze_schema: str = "github_analytics_bronze"
    silver_schema: str = "github_analytics_silver"
    gold_schema: str = "github_analytics_gold"
    control_schema: str = "github_analytics_control"

    @property
    def bronze_issues_table(self) -> str:
        """Return append-only raw GitHub Issue evidence for replay and lineage."""
        return f"{self.catalog}.{self.bronze_schema}.github_issues_raw"

    @property
    def silver_issues_table(self) -> str:
        """Return one current normalized Issue per repository-scoped Issue ID.

        This is the source for the Gold ``fact_issue`` table.
        """
        return f"{self.catalog}.{self.silver_schema}.github_issues"

    @property
    def watermark_table(self) -> str:
        """Return operational state for the latest committed source position.

        A watermark controls incremental extraction; it is not a business or
        Gold-model entity.
        """
        return f"{self.catalog}.{self.control_schema}.ingestion_watermark"

    @property
    def pipeline_runs_table(self) -> str:
        """Return operational audit state for each pipeline attempt.

        This table explains how the pipeline ran. It is deliberately separate
        from GitHub analytical entities and must not feed a Gold fact directly.
        """
        return f"{self.catalog}.{self.control_schema}.pipeline_runs"

    @property
    def silver_users_table(self) -> str:
        """Return one current normalized GitHub User per global GitHub user ID.

        This is the source for the Gold ``dim_user`` dimension.
        """
        return f"{self.catalog}.{self.silver_schema}.github_users"

    @property
    def silver_labels_table(self) -> str:
        """Return one current Label definition per repository-scoped label ID.

        This is the source for the Gold ``dim_label`` dimension. It
        describes Labels themselves, not which Issues currently use them.
        """
        return f"{self.catalog}.{self.silver_schema}.github_labels"

    @property
    def silver_issue_labels_table(self) -> str:
        """Return current Issue-to-Label relationships at Silver grain.

        One row means that one Label is currently assigned to one Issue. This is
        the source for Gold ``bridge_issue_label``.
        """
        return f"{self.catalog}.{self.silver_schema}.github_issue_labels"

    @property
    def replay_manifest_table(self) -> str:
        """Return immutable replay-input boundaries for diagnosis and replay."""
        return f"{self.catalog}.{self.control_schema}.replay_manifests"
