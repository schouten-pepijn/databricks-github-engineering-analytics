from github_engineering_analytics.common.config import PipelineConfig


def test_pipeline_config_builds_table_names():
    config = PipelineConfig(catalog="main")

    assert (
        config.bronze_issues_table == "main.github_analytics_bronze.github_issues_raw"
    )

    assert config.silver_issues_table == "main.github_analytics_silver.github_issues"

    assert config.watermark_table == "main.github_analytics_control.ingestion_watermark"

    assert config.pipeline_runs_table == "main.github_analytics_control.pipeline_runs"

    assert config.silver_users_table == "main.github_analytics_silver.github_users"

    assert config.silver_labels_table == "main.github_analytics_silver.github_labels"
