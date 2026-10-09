-- A few orders arrive without a usable customer id and are attached to the
-- "Unknown" customer. Above 1% of orders, something upstream has changed.
{{ config(severity='warn') }}

select unknown_orders, orders, round(100.0 * unknown_orders / orders, 2) as unknown_pct
from (
    select
        count(*) filter (where customer_key = {{ unknown_key() }}) as unknown_orders,
        count(*) as orders
    from {{ ref('fct_orders') }}
) as summary
where orders > 0
  and unknown_orders > 0.01 * orders
