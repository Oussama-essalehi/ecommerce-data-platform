-- No order can be dated after today. One day of margin covers the hours
-- when France is already on the next day and the database clock (UTC) is not.
select order_id, order_date
from {{ ref('fct_orders') }}
where order_date > current_date + 1
