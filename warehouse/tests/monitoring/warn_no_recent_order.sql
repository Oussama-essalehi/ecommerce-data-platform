-- The warehouse should hold orders from today or yesterday. If the latest
-- order is older, the pipeline has not delivered fresh data.
{{ config(severity='warn') }}

select latest_order_date, current_date - latest_order_date as days_behind
from (
    select max(order_date) as latest_order_date from {{ ref('fct_orders') }}
) as summary
where latest_order_date < current_date - 2
