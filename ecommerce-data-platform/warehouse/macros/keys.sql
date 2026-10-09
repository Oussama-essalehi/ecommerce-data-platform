{#
    Surrogate keys are a hash of the business key. Unlike a sequence they
    are stable: rebuilding a dimension gives every member the same key, so
    facts and dimensions can be rebuilt independently.
#}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('|'
        {%- for column in columns -%}
        , coalesce(cast({{ column }} as text), '_null_')
        {%- endfor -%}
    ))
{%- endmacro %}

{#
    Key of the "Unknown" member every dimension carries. A fact row whose
    customer or product cannot be found points to it, so joins from facts
    to dimensions never drop rows.
#}
{% macro unknown_key() -%}
    md5('_unknown_')
{%- endmacro %}

{% macro date_key(column) -%}
    cast(to_char({{ column }}, 'YYYYMMDD') as integer)
{%- endmacro %}
