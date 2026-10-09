{{
    config(
        indexes=[
            {'columns': ['order_date_key']},
            {'columns': ['customer_key']},
        ]
    )
}}

-- Grain: one row per order. Holds what only exists at order level:
-- shipping, the order's journey from payment to delivery, its totals.
with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select customer_key, customer_id from {{ ref('dim_customers') }}

)

select
    orders.order_id,

    {{ date_key('orders.order_date') }} as order_date_key,
    coalesce(customers.customer_key, {{ unknown_key() }}) as customer_key,

    orders.order_date,
    orders.order_ts,
    orders.channel,
    orders.source,
    orders.order_status,
    orders.payment_method,
    orders.shipping_city,
    orders.shipping_postal_code,
    left(orders.shipping_postal_code, 2) as shipping_department_code,
    orders.is_cancelled,
    orders.is_returned,

    orders.paid_at,
    orders.shipped_at,
    orders.delivered_at,
    orders.cancelled_at,
    orders.returned_at,
    round((extract(epoch from orders.shipped_at - orders.order_ts) / 3600)::numeric, 1)
        as hours_to_ship,
    round((extract(epoch from orders.delivered_at - orders.order_ts) / 86400)::numeric, 2)
        as days_to_deliver,

    orders.lines_count,
    orders.units,
    orders.gross_amount,
    orders.discount_amount,
    orders.net_amount,
    orders.shipping_fee,
    orders.total_amount,
    orders.source_total_amount,

    -- Same rule as in fct_sales: a cancelled order sold and billed nothing.
    case when orders.is_cancelled then 0 else orders.units end as units_sold,
    case when orders.is_cancelled then 0 else orders.net_amount end as net_sales_amount,
    case when orders.is_cancelled then 0 else orders.total_amount end as billed_amount

from orders
left join customers
    on customers.customer_id = orders.customer_id
