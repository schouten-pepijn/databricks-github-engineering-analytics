"""Guard the dbt settings that let a proof build read and write elsewhere.

A proof build points dbt at a frozen copy of Silver and writes its Gold output
to a scratch schema. Without these settings the production build must behave
exactly as it always did, so the defaults are pinned here.
"""

from pathlib import Path

import yaml

DBT = Path(__file__).parents[2] / "dbt"


def test_silver_source_reads_the_live_schema_unless_overridden() -> None:
    sources = yaml.safe_load(
        (DBT / "models" / "source.yml").read_text(encoding="utf-8")
    )
    silver = next(s for s in sources["sources"] if s["name"] == "silver")

    assert silver["schema"] == "{{ var('silver_schema', 'github_analytics_silver') }}"


def test_gold_models_write_to_the_target_schema_unless_overridden() -> None:
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))
    gold = project["models"]["github_engineering_analytics"]["gold"]

    assert gold["+schema"] == "{{ var('gold_output_schema', target.schema) }}"


def test_the_schema_macro_refuses_a_proof_build_without_an_output_schema() -> None:
    macro = (DBT / "macros" / "generate_schema_name.sql").read_text(encoding="utf-8")

    # The safety net: reading a non-default Silver schema without naming an
    # output schema would overwrite the real Gold tables with proof data.
    assert "var('silver_schema', none) is not none" in macro
    assert "var('gold_output_schema', none) is none" in macro


def test_candidate_models_follow_the_same_output_schema_rule_as_gold() -> None:
    project = yaml.safe_load((DBT / "dbt_project.yml").read_text(encoding="utf-8"))
    candidate = project["models"]["github_engineering_analytics"]["gold_candidate"]

    assert candidate["+schema"] == "{{ var('gold_output_schema', target.schema) }}"


def test_workset_source_defaults_to_a_scratch_schema() -> None:
    sources = yaml.safe_load(
        (DBT / "models" / "source.yml").read_text(encoding="utf-8")
    )
    workset = next(s for s in sources["sources"] if s["name"] == "workset")

    # The default must carry the scratch prefix, never a live schema name.
    assert "github_analytics_s5_" in workset["schema"]
