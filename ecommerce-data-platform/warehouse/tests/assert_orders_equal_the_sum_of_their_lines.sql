-- An order's net amount and units must equal the sum of its lines in the
-- sales fact. Returns the orders where the two facts disagree.
with from_lines as (

    select order_id, sum(quantity) as units, sum(net_amount) as net_amount
    from {{ ref('fct_sales') }}
    group by order_id

)

select orders.order_id, orders.units, from_lines.units as line_units,
       orders.net_amount, from_lines.net_amount as line_net_amount
from {{ ref('fct_orders') }} as orders
left join from_lines
    on from_lines.order_id = orders.order_id
where from_lines.order_id is null
   or orders.units != from_lines.units
   or orders.net_amount != from_lines.net_amount
