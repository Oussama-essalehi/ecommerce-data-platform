-- Figures to check the Power BI report against.
--
-- Each query computes, straight from the warehouse, what one visual of the
-- report shows when no slicer is set. If a number differs in Power BI, the
-- cause is in the report: a missing relationship, a measure pasted on the
-- wrong column, or data that was not refreshed after the last pipeline run.
--
-- Run it with (the same two commands in any shell), or with `make figures`:
--   docker compose cp dashboards/check_figures.sql warehouse:/tmp/check_figures.sql
--   docker compose exec warehouse psql -U warehouse -d warehouse -f /tmp/check_figures.sql

\pset pager off
\pset footer off

\echo
\echo '=== Page 1, Sales overview: the cards ==='
\pset expanded on
select
    round(sum(net_sales_amount), 2)                                   as "Net Sales",
    sum(units_sold)                                                   as "Units Sold",
    count(distinct order_id) filter (where not is_cancelled)          as "Orders",
    round(sum(net_sales_amount)
          / count(distinct order_id) filter (where not is_cancelled), 2) as "Average Basket",
    round(sum(gross_amount) filter (where not is_cancelled), 2)       as "Gross Sales",
    round(sum(discount_amount) filter (where not is_cancelled), 2)    as "Discounts",
    round(100.0 * sum(discount_amount) filter (where not is_cancelled)
          / sum(gross_amount) filter (where not is_cancelled), 2)     as "Discount Rate (%)"
from core.fct_sales;
\pset expanded off

\echo
\echo '=== Page 1: Net Sales by month, with the previous month ==='
with monthly as (
    select d.year_month, sum(s.net_sales_amount) as net_sales
    from core.fct_sales s
    join core.dim_date d on d.date_key = s.order_date_key
    group by d.year_month
)
select
    year_month,
    round(net_sales, 2)                                     as "Net Sales",
    round(lag(net_sales) over (order by year_month), 2)     as "Net Sales Previous Month",
    round(100.0 * (net_sales - lag(net_sales) over (order by year_month))
          / lag(net_sales) over (order by year_month), 1)   as "Net Sales MoM (%)"
from monthly
order by year_month;

\echo
\echo '=== Page 1: Net Sales and its 7-day average, last 10 days ==='
with daily as (
    select order_date, sum(net_sales_amount) as net_sales
    from core.fct_sales
    group by order_date
),
smoothed as (
    select
        order_date,
        net_sales,
        avg(net_sales) over (order by order_date rows between 6 preceding and current row) as average_7d
    from daily
)
select order_date as date_day,
       round(net_sales, 2)  as "Net Sales",
       round(average_7d, 2) as "Net Sales 7-Day Average"
from smoothed
order by order_date desc
limit 10;

\echo
\echo '=== Page 1: by channel ==='
select
    c.channel_name,
    round(sum(s.net_sales_amount), 2)                             as "Net Sales",
    count(distinct s.order_id) filter (where not s.is_cancelled)  as "Orders",
    sum(s.units_sold)                                             as "Units Sold"
from core.fct_sales s
join core.dim_channels c on c.channel = s.channel
group by c.channel_name
order by 2 desc;

\echo
\echo '=== Page 1: by product category ==='
select
    p.category,
    round(sum(s.net_sales_amount), 2)                             as "Net Sales",
    sum(s.units_sold)                                             as "Units Sold",
    round(100.0 * sum(s.discount_amount) filter (where not s.is_cancelled)
          / sum(s.gross_amount) filter (where not s.is_cancelled), 2) as "Discount Rate (%)"
from core.fct_sales s
join core.dim_products p on p.product_key = s.product_key
group by p.category
order by 2 desc;

\echo
\echo '=== Page 1: top 10 products ==='
select
    p.product_name,
    p.category,
    round(sum(s.net_sales_amount), 2) as "Net Sales",
    sum(s.units_sold)                 as "Units Sold"
from core.fct_sales s
join core.dim_products p on p.product_key = s.product_key
group by p.product_name, p.category
order by 3 desc
limit 10;

\echo
\echo '=== Page 2, Customers: the cards ==='
\pset expanded on
select
    count(*)                                                        as "Customers",
    count(*) filter (where orders > 0)                              as "Buyers",
    count(*) filter (where orders >= 2)                             as "Repeat Buyers",
    round(100.0 * count(*) filter (where orders >= 2)
          / count(*) filter (where orders > 0), 1)                  as "Repeat Rate (%)",
    round(sum(total_spent), 2)                                      as "Lifetime Spend",
    round(sum(total_spent) / count(*) filter (where orders > 0), 2) as "Average Spend per Buyer"
from marts.mart_customers;
\pset expanded off

\echo
\echo '=== Page 2: by customer segment ==='
select
    customer_segment,
    count(*)                    as "Customers",
    round(sum(total_spent), 2)  as "Lifetime Spend"
from marts.mart_customers
group by customer_segment
order by 3 desc;

\echo
\echo '=== Page 2: top 10 departements by Net Sales ==='
select
    c.department_code,
    round(sum(s.net_sales_amount), 2) as "Net Sales"
from core.fct_sales s
join core.dim_customers c on c.customer_key = s.customer_key
group by c.department_code
order by 2 desc
limit 10;

\echo
\echo '=== Page 3, Operations: the cards ==='
\pset expanded on
select
    count(*)                                                     as "Orders Placed",
    count(*) filter (where is_cancelled)                         as "Cancelled Orders",
    round(100.0 * count(*) filter (where is_cancelled) / count(*), 2) as "Cancellation Rate (%)",
    count(*) filter (where is_returned)                          as "Returned Orders",
    round(100.0 * count(*) filter (where is_returned)
          / count(*) filter (where not is_cancelled), 2)         as "Return Rate (%)",
    round(sum(billed_amount), 2)                                 as "Billed Amount",
    round(sum(shipping_fee) filter (where not is_cancelled), 2)  as "Shipping Fees",
    round(sum(billed_amount) / count(*) filter (where not is_cancelled), 2) as "Average Order Value",
    round(avg(hours_to_ship), 1)                                 as "Average Hours to Ship",
    round(avg(days_to_deliver), 2)                               as "Average Days to Deliver"
from core.fct_orders;
\pset expanded off

\echo
\echo '=== Page 3: orders by status ==='
select order_status, count(*) as "Orders Placed"
from core.fct_orders
group by order_status
order by 2 desc;

\echo
\echo '=== Page 3: by channel ==='
select
    c.channel_name,
    count(*)                                                          as "Orders Placed",
    round(100.0 * count(*) filter (where o.is_cancelled) / count(*), 2) as "Cancellation Rate (%)",
    round(100.0 * count(*) filter (where o.is_returned)
          / count(*) filter (where not o.is_cancelled), 2)            as "Return Rate (%)",
    round(avg(o.hours_to_ship), 1)                                    as "Average Hours to Ship",
    round(avg(o.days_to_deliver), 2)                                  as "Average Days to Deliver"
from core.fct_orders o
join core.dim_channels c on c.channel = o.channel
group by c.channel_name
order by 2 desc;

\echo
\echo '=== Page 3: by payment method ==='
select payment_method, count(*) as "Orders Placed", round(sum(billed_amount), 2) as "Billed Amount"
from core.fct_orders
group by payment_method
order by 2 desc;

\echo
\echo '=== Page 4, Data quality: the cards (latest run) ==='
\pset expanded on
select
    max(checked_at)                                   as "Last run",
    count(*)                                          as "Checks",
    count(*) filter (where status = 'passed')         as "Checks Passed",
    count(*) filter (where status = 'warning')        as "Checks in Warning",
    count(*) filter (where status = 'failed')         as "Checks Failed",
    round(100.0 * count(*) filter (where status = 'passed') / count(*), 1) as "Pass Rate (%)",
    (select sum(rejected) from marts.mart_reject_history where is_latest_run) as "Rejected Records"
from marts.mart_data_quality
where is_latest_run;
\pset expanded off

\echo
\echo '=== Page 4: checks by layer and dimension (latest run) ==='
select
    layer,
    dimension,
    count(*)                                    as "Checks",
    count(*) filter (where status != 'passed')  as "Not passed"
from marts.mart_data_quality
where is_latest_run
group by layer, dimension
order by case layer when 'bronze' then 1 when 'silver' then 2 else 3 end, dimension;

\echo
\echo '=== Page 4: rejected records by source and reason (latest run) ==='
select source, reason, rejected as "Rejected Records", change_since_previous_run
from marts.mart_reject_history
where is_latest_run
order by rejected desc;
