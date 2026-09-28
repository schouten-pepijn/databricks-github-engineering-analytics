"""Namespace for business-ready dbt models built from conformed Silver.

Gold is intentionally implemented in dbt rather than Python. The model maps are:
``github_issues`` to ``fact_issue``, ``github_users`` to ``dim_user``,
``github_labels`` to ``dim_label`` and ``github_issue_labels`` to
``bridge_issue_label``.
"""
