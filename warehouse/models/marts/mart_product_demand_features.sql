-- One row per product and per day: the training table for demand
-- forecasting. `units_sold` is the value to predict; every other column is
-- a feature known before that day starts.
--
-- Three choices matter here:
--
-- 1. Days without sales are rows too, with zero units. A model trained only
--    on days with sales would never learn that a product can sell nothing.
-- 2. Past-demand features (lags, rolling averages) stop at the day before.
--    Including the day itself would leak the answer into the features.
-- 3. The current day is left out: its orders are still coming in.
with products as (

    select * from {{ ref('dim_products') }}
    where product_key != {{ unknown_key() }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

sales as (

    select * from {{ ref('fct_sales') }}

),

period as (

    select min(order_date) as first_day, max(order_date) - 1 as last_day
    from sales

),

-- every day a product was on sale, whether it sold or not
product_days as (

    select
        products.product_key,
        products.product_id,
        products.category,
        products.brand,
        dates.date_key,
        dates.date_day
    from products
    cross join period
    inner join dates
        on dates.date_day between greatest(products.created_date, period.first_day)
                              and least(coalesce(products.discontinued_date, period.last_day),
                                        period.last_day)

),

daily_sales as (

    select
        product_key,
        order_date_key,
        sum(units_sold) as units_sold,
        count(distinct order_id) filter (where not is_cancelled) as orders,
        sum(net_sales_amount) as net_sales,
        round(avg(unit_price) filter (where not is_cancelled), 2) as average_unit_price,
        round(
            sum(discount_amount) filter (where not is_cancelled)
            / nullif(sum(gross_amount) filter (where not is_cancelled), 0),
            4
        ) as discount_rate
    from sales
    group by product_key, order_date_key

),

demand as (

    select
        product_days.*,
        coalesce(daily_sales.units_sold, 0) as units_sold,
        coalesce(daily_sales.orders, 0) as orders,
        coalesce(daily_sales.net_sales, 0) as net_sales,
        daily_sales.average_unit_price,
        daily_sales.discount_rate
    from product_days
    left join daily_sales
        on daily_sales.product_key = product_days.product_key
        and daily_sales.order_date_key = product_days.date_key

)

select
    demand.product_key,
    demand.product_id,
    demand.date_day,
    demand.category,
    demand.brand,

    -- target
    demand.units_sold,

    -- same-day facts, useful for analysis but not available before the day ends
    demand.orders,
    demand.net_sales,
    demand.average_unit_price,
    demand.discount_rate,

    -- past demand, all strictly before date_day
    lag(demand.units_sold, 1) over product_history as units_sold_lag_1,
    lag(demand.units_sold, 7) over product_history as units_sold_lag_7,
    lag(demand.units_sold, 14) over product_history as units_sold_lag_14,
    round(avg(demand.units_sold) over (
        product_history rows between 7 preceding and 1 preceding
    ), 3) as units_sold_avg_7d,
    round(avg(demand.units_sold) over (
        product_history rows between 28 preceding and 1 preceding
    ), 3) as units_sold_avg_28d,
    count(*) over (
        product_history rows between unbounded preceding and 1 preceding
    ) as days_on_sale,

    -- calendar, known in advance
    dates.day_of_week,
    dates.is_weekend,
    dates.month,
    dates.is_public_holiday,
    dates.is_sales_event,
    dates.sales_event

from demand
inner join dates
    on dates.date_key = demand.date_key
window product_history as (partition by demand.product_key order by demand.date_day)
