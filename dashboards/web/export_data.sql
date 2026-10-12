-- Data of the web dashboard, as one JSON document.
--
-- Read by export_data.sh, which wraps it into assets/dashboard-data.js.
-- Facts are pre-aggregated per day and channel so that the page can apply
-- any date range itself and still give the figures of the Power BI report
-- (see dashboards/measures.dax): sums are additive over days and channels,
-- and distinct orders are counted per day, channel and category.
-- Arrays of arrays keep the file small; their columns are listed in "columns".

with

bounds as (
    select min(order_date) as first_day, max(order_date) as last_day
    from core.fct_orders
),

-- Dimensions, numbered in a fixed order. The page refers to them by index.
channels as (
    select c.channel, c.channel_name,
           row_number() over (order by coalesce(sum(f.net_sales_amount), 0) desc, c.channel_name) - 1 as i
    from core.dim_channels as c
    left join core.fct_sales as f using (channel)
    group by c.channel, c.channel_name
),

categories as (
    select p.category,
           row_number() over (order by coalesce(sum(f.net_sales_amount), 0) desc, p.category) - 1 as i
    from core.dim_products as p
    left join core.fct_sales as f using (product_key)
    group by p.category
),

products as (
    select p.product_key, p.product_name, p.brand, k.i as category_i,
           row_number() over (order by p.product_name, p.product_key) - 1 as i
    from core.dim_products as p
    join categories as k using (category)
),

statuses as (
    select order_status,
           row_number() over (order by array_position(
               array['created', 'paid', 'shipped', 'delivered', 'returned', 'cancelled'], order_status
           ), order_status) - 1 as i
    from (select distinct order_status from core.fct_orders) as s
),

payments as (
    select payment_method, row_number() over (order by count(*) desc, payment_method) - 1 as i
    from core.fct_orders
    group by payment_method
),

-- Sales lines per day, channel and category.
sales as (
    select f.order_date - b.first_day as d, ch.i as ch, k.i as cat,
           round(sum(f.net_sales_amount), 2) as net,
           sum(f.units_sold) as units,
           round(coalesce(sum(f.gross_amount) filter (where not f.is_cancelled), 0), 2) as gross,
           round(coalesce(sum(f.discount_amount) filter (where not f.is_cancelled), 0), 2) as disc,
           count(distinct f.order_id) filter (where not f.is_cancelled) as orders
    from core.fct_sales as f
    cross join bounds as b
    join channels as ch using (channel)
    join core.dim_products as p using (product_key)
    join categories as k on k.category = p.category
    group by 1, 2, 3
),

-- Orders per day and channel, for every category at once (an order can hold several).
sales_orders as (
    select f.order_date - b.first_day as d, ch.i as ch,
           count(distinct f.order_id) filter (where not f.is_cancelled) as orders
    from core.fct_sales as f
    cross join bounds as b
    join channels as ch using (channel)
    group by 1, 2
),

product_sales as (
    select f.order_date - b.first_day as d, ch.i as ch, p.i as p,
           round(sum(f.net_sales_amount), 2) as net,
           sum(f.units_sold) as units
    from core.fct_sales as f
    cross join bounds as b
    join channels as ch using (channel)
    join products as p using (product_key)
    group by 1, 2, 3
    having sum(f.units_sold) > 0
),

orders as (
    select o.order_date - b.first_day as d, ch.i as ch, s.i as status, pm.i as pay,
           count(*) as n,
           count(*) filter (where o.is_cancelled) as cancelled,
           count(*) filter (where o.is_returned) as returned,
           round(sum(o.billed_amount), 2) as billed,
           round(coalesce(sum(o.shipping_fee) filter (where not o.is_cancelled), 0), 2) as shipping,
           round(coalesce(sum(o.hours_to_ship), 0), 1) as hours_sum,
           count(o.hours_to_ship) as hours_n,
           round(coalesce(sum(o.days_to_deliver), 0), 2) as days_sum,
           count(o.days_to_deliver) as days_n
    from core.fct_orders as o
    cross join bounds as b
    join channels as ch using (channel)
    join statuses as s using (order_status)
    join payments as pm using (payment_method)
    group by 1, 2, 3, 4
),

events as (
    select d.date_day - b.first_day as d, d.sales_event, d.holiday_name
    from core.dim_date as d
    cross join bounds as b
    where d.date_day between b.first_day and b.last_day
      and (d.is_sales_event or d.is_public_holiday)
),

-- Customers: lifetime figures, per segment and for all segments.
segments as (
    select coalesce(customer_segment, 'All') as segment,
           count(*) as customers,
           count(*) filter (where orders > 0) as buyers,
           count(*) filter (where orders >= 2) as repeat_buyers,
           round(sum(total_spent), 2) as lifetime_spend
    from marts.mart_customers
    group by grouping sets ((), (customer_segment))
),

departments as (
    select coalesce(m.customer_segment, 'All') as segment, c.department_code,
           round(sum(f.net_sales_amount), 2) as net
    from core.fct_sales as f
    join core.dim_customers as c using (customer_key)
    left join marts.mart_customers as m using (customer_key)
    where c.department_code is not null
    group by c.department_code, grouping sets ((), (m.customer_segment))
),

departments_top as (
    select *, row_number() over (partition by segment order by net desc, department_code) as rank
    from departments
),

recency as (
    select coalesce(customer_segment, 'All') as segment,
           (days_since_last_order / 30) * 30 as days_from,
           count(*) as customers
    from marts.mart_customers
    where days_since_last_order is not null
    group by days_since_last_order / 30, grouping sets ((), (customer_segment))
),

customer_rank as (
    select coalesce(customer_segment, 'All') as segment, customer_id
    from marts.mart_customers
    group by customer_id, grouping sets ((), (customer_segment))
),

top_customers as (
    select r.segment, m.full_name, m.customer_id, m.city, m.customer_segment, m.orders,
           round(m.total_spent, 2) as total_spent, m.last_order_date,
           row_number() over (partition by r.segment order by m.total_spent desc, m.customer_id) as rank
    from customer_rank as r
    join marts.mart_customers as m using (customer_id)
    where m.total_spent > 0
),

-- Data quality: the latest run of the quality job.
checks as (
    select * from marts.mart_data_quality where is_latest_run
),

rejects as (
    select * from marts.mart_reject_history where is_latest_run
)

select json_build_object(
    'generated_at', to_char(now() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
    'first_day', (select first_day from bounds),
    'last_day', (select last_day from bounds),

    'channels', (select json_agg(channel_name order by i) from channels),
    'categories', (select json_agg(category order by i) from categories),
    'products', (select json_agg(json_build_array(product_name, category_i) order by i) from products),
    'statuses', (select json_agg(order_status order by i) from statuses),
    'payments', (select json_agg(payment_method order by i) from payments),

    'columns', json_build_object(
        'sales', json_build_array('day', 'channel', 'category', 'net_sales', 'units_sold', 'gross_sales', 'discounts', 'orders'),
        'sales_orders', json_build_array('day', 'channel', 'orders'),
        'product_sales', json_build_array('day', 'channel', 'product', 'net_sales', 'units_sold'),
        'orders', json_build_array('day', 'channel', 'status', 'payment', 'orders_placed', 'cancelled', 'returned',
                                   'billed_amount', 'shipping_fees', 'hours_to_ship_sum', 'hours_to_ship_n',
                                   'days_to_deliver_sum', 'days_to_deliver_n')
    ),
    'sales', (select json_agg(json_build_array(d, ch, cat, net, units, gross, disc, orders) order by d, ch, cat) from sales),
    'sales_orders', (select json_agg(json_build_array(d, ch, orders) order by d, ch) from sales_orders),
    'product_sales', (select json_agg(json_build_array(d, ch, p, net, units) order by d, ch, p) from product_sales),
    'orders', (select json_agg(json_build_array(d, ch, status, pay, n, cancelled, returned, billed, shipping,
                                                hours_sum, hours_n, days_sum, days_n) order by d, ch, status, pay) from orders),
    'events', (select json_agg(json_build_object('day', d, 'sales_event', sales_event, 'holiday', holiday_name) order by d) from events),

    'customers', json_build_object(
        'segments', (select json_agg(s order by s.segment) from segments as s),
        'departments', (select json_agg(json_build_object('segment', segment, 'department', department_code, 'net_sales', net)
                                        order by segment, rank) from departments_top where rank <= 10),
        'recency', (select json_agg(json_build_object('segment', segment, 'days_from', days_from, 'customers', customers)
                                    order by segment, days_from) from recency),
        'top', (select json_agg(json_build_object('segment', segment, 'rank', rank, 'name', full_name, 'id', customer_id,
                                                  'city', city, 'customer_segment', customer_segment, 'orders', orders,
                                                  'total_spent', total_spent, 'last_order', last_order_date)
                                order by segment, rank) from top_customers where rank <= 20)
    ),

    'quality', json_build_object(
        'run_id', (select max(run_id) from checks),
        'checked_at', (select to_char(max(checked_at) at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') from checks),
        'checks', (select json_agg(json_build_object('table', checked_table, 'check', check_name, 'layer', layer,
                                                     'dimension', dimension, 'severity', severity, 'status', status,
                                                     'description', description, 'observed', observed_value)
                                   order by checked_table, check_name) from checks),
        'rejects', (select json_agg(json_build_object('source', source, 'reason', reason, 'rejected', rejected)
                                    order by rejected desc) from rejects)
    )
);
