{% macro test_data_freshness(model, date_column, max_days_old=7) %}
    select count(*)
    from {{ model }}
    where {{ date_column }} < current_date() - {{ max_days_old }}
{% endmacro %}