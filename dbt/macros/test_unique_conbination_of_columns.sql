{% test unique_combination_of_columns(model, combination_of_columns) %}

  -- Return one row for every duplicate business key.
  -- dbt marks the test as failed when this query returns any rows.
  with duplicate_business_keys as (

    select
      {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    from {{ model }}
    group by
      {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    having count(*) > 1

  )

  select
    {% for column_name in combination_of_columns %}
      {{ adapter.quote(column_name) }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from duplicate_business_keys

{% endtest %}
