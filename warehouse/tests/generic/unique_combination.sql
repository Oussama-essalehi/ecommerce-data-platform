{#
    Uniqueness of a grain made of several columns: no two rows share the
    same values for all of them.
#}
{% test unique_combination(model, columns) %}

select {{ columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
