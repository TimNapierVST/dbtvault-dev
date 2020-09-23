{%- macro test_assert_data_equal_to_expected(model, unique_id, compare_columns, expected_seed) -%}

WITH actual_data as (
    SELECT * FROM {{ model }}
),
expected_data as (
    SELECT * FROM {{ ref(expected_seed) }}
),
compare as (
    SELECT a.*
    FROM actual_data AS a
    FULL OUTER JOIN expected_data AS b
    {%- for column in compare_columns -%}
    {%- if loop.first %}
    ON ({{dbt_utils.safe_cast('a.'~ column, dbt_utils.type_string()) }} = {{dbt_utils.safe_cast('b.'~ column, dbt_utils.type_string()) }}
    {%- else %}
    AND {{dbt_utils.safe_cast('a.'~ column, dbt_utils.type_string()) }} = {{dbt_utils.safe_cast('b.'~ column, dbt_utils.type_string()) }}
    {{- ')' if loop.last -}}
    {%- endif -%}
    {%- endfor %}
    WHERE a.{{ unique_id }} IS NULL
    OR b.{{ unique_id }} IS NULL
)
SELECT COUNT(*) AS differences FROM compare
{%- endmacro -%}