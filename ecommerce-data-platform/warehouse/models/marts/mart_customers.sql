-- One row per customer with their purchase history: the table behind
-- customer analysis (new against returning, value, recency).
--
-- Recency is measured against the last order date in the warehouse rather
-- than today's date, so rebuilding the table on the same data always gives
-- the same result.
with customers as (

    select * from {{ ref('dim_customers') }}
    where customer_key != {{ unknown_key() }}

),

orders as (

    select * from {{ ref('fct_orders') }}

),

as_of as (

    select max(order_date) as as_of_date from orders

),

history as (

    select
        customer_key,
        count(*) as orders_placed,
        count(*) filter (where not is_cancelled) as orders,
        count(*) filter (where is_cancelled) as cancelled_orders,
        count(*) filter (where is_returned) as returned_orders,
        min(order_date) filter (where not is_cancelled) as first_order_date,
        max(order_date) filter (where not is_cancelled) as last_order_date,
        sum(units_sold) as units_bought,
        sum(billed_amount) as total_spent
    from orders
    group by customer_key

)

select
    customers.customer_key,
    customers.customer_id,
    customers.full_name,
    customers.city,
    customers.department_code,
    customers.signup_date,

    coalesce(history.orders_placed, 0) as orders_placed,
    coalesce(history.orders, 0) as orders,
    coalesce(history.cancelled_orders, 0) as cancelled_orders,
    coalesce(history.returned_orders, 0) as returned_orders,
    history.first_order_date,
    history.last_order_date,
    coalesce(history.units_bought, 0) as units_bought,
    coalesce(history.total_spent, 0) as total_spent,
    round(history.total_spent / nullif(history.orders, 0), 2) as average_order_value,
    as_of.as_of_date - history.last_order_date as days_since_last_order,

    case
        when coalesce(history.orders, 0) = 0 then 'No purchase'
        when history.orders = 1 then 'One-time'
        when history.orders between 2 and 4 then 'Repeat'
        else 'Loyal'
    end as customer_segment

from customers
cross join as_of
left join history
    on history.customer_key = customers.customer_key
