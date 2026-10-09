{#
    By default dbt prefixes custom schemas with the target schema
    ("analytics_core"). This project wants the layer names as they are
    ("core", "marts"), so the custom schema is used unchanged.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
