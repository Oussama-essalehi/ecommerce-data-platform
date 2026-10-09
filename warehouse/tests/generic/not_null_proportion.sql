{#
    Completeness with a tolerance: at least `at_least` (a share between 0
    and 1) of the rows have a value in the column.
#}
{% test not_null_proportion(model, column_name, at_least) %}

select filled_share
from (
    select count({{ column_name }})::numeric / nullif(count(*), 0) as filled_share
    from {{ model }}
) as summary
where filled_share < {{ at_least }}

{% endtest %}
