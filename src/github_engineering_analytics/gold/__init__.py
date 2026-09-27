"""Namespace for future business-ready dbt models built from conformed Silver.

Gold is intentionally not implemented in Python. The planned model maps are:
``github_issues`` to ``fact_issue``, ``github_users`` to ``dim_user``,
``github_labels`` to ``dim_label`` and the pending ``github_issue_labels``
relation to ``bridge_issue_label``.
"""
