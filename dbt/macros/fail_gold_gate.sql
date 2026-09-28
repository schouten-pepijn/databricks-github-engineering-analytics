{#
  TEST ONLY.

  Yhis operation deliberately fails after the normal Gold build in the
  failure-path bundle. It executes no SQL and must never be added to
  the normal DAG.
#}
{% macro fail_gold_gate() %}
  {{ exceptions.raise_compiler_error(
    "INTENTIONAL_GOLD_FAILURE: failure-path bundle test"
  ) }}
{% endmacro %}
