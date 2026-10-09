{{
    config(
        indexes=[
            {'columns': ['order_date_key']},
            {'columns': ['product_key']},
            {'columns': ['customer_key']},
        ]
    )
}}

-- Grain: one row per product sold in an order (an order line).
with lines as (

    select * from {{ ref('stg_order_lines') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select customer_key, customer_id from {{ ref('dim_customers') }}

),

products as (

    select product_key, product_id from {{ ref('dim_products') }}

)

select
    {{ surrogate_key(['lines.order_id', 'lines.line_number']) }} as sale_key,
    lines.order_id,
    lines.line_number,

    {{ date_key('orders.order_date') }} as order_date_key,
    coalesce(customers.customer_key, {{ unknown_key() }}) as customer_key,
    coalesce(products.product_key, {{ unknown_key() }}) as product_key,

    orders.order_date,
    orders.channel,
    orders.source,
    orders.order_status,
    orders.is_cancelled,
    orders.is_returned,

    lines.quantity,
    lines.unit_price,
    lines.discount_pct,
    lines.gross_amount,
    lines.discount_amount,
    lines.net_amount,

    -- Cancelled orders stay in the table so cancellations can be analysed,
    -- but they sold nothing: these two measures count them as zero.
    case when orders.is_cancelled then 0 else lines.quantity end as units_sold,
    case when orders.is_cancelled then 0 else lines.net_amount end as net_sales_amount

from lines
inner join orders
    on orders.order_id = lines.order_id
left join customers
    on customers.customer_id = orders.customer_id
left join products
    on products.product_id = lines.product_id
