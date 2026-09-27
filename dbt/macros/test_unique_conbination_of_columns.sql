{% test unique_combination_of_columns(model, combination_of_columns) %}

  -- The YAML contract supplies the model's business-key columns. Quoting each
  -- identifier keeps this generic test safe for any valid adapter identifier.
  -- Return one row for every duplicate key; dbt fails a data test with rows.
  with duplicate_business_keys as (

    select
      {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %},{% endif %}
      {% endfor %}
    from {{ model }}
    group by
      {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %},{% endif %}
      {% endfor %}
    having count(*) > 1

  )

  select
    {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from duplicate_business_keys

{% endtest %}
