-- Sales per day, channel and product category: the table behind the sales
-- dashboard. Small enough to load whole into Power BI.
--
-- Careful with `orders`: an order holding products of two categories is
-- counted once in each. Sum it across categories and it overstates the
-- number of orders; use fct_orders for an exact order count.
with sales as (

    select * from {{ ref('fct_sales') }}

),

products as (

    select product_key, category from {{ ref('dim_products') }}

),

dates as (

    select * from {{ ref('dim_date') }}

)

select
    dates.date_day,
    dates.year,
    dates.year_month,
    dates.iso_week,
    dates.day_name,
    dates.is_weekend,
    dates.is_public_holiday,
    dates.sales_event,
    sales.channel,
    products.category,

    count(distinct sales.order_id) filter (where not sales.is_cancelled) as orders,
    sum(sales.units_sold) as units_sold,
    sum(sales.gross_amount) filter (where not sales.is_cancelled) as gross_sales,
    sum(sales.discount_amount) filter (where not sales.is_cancelled) as discounts,
    sum(sales.net_sales_amount) as net_sales,

    count(distinct sales.order_id) filter (where sales.is_cancelled) as cancelled_orders,
    coalesce(sum(sales.net_amount) filter (where sales.is_cancelled), 0) as cancelled_amount,
    count(distinct sales.order_id) filter (where sales.is_returned) as returned_orders,
    coalesce(sum(sales.net_amount) filter (where sales.is_returned), 0) as returned_amount

from sales
inner join dates
    on dates.date_key = sales.order_date_key
inner join products
    on products.product_key = sales.product_key
group by
    dates.date_day,
    dates.year,
    dates.year_month,
    dates.iso_week,
    dates.day_name,
    dates.is_weekend,
    dates.is_public_holiday,
    dates.sales_event,
    sales.channel,
    products.category
