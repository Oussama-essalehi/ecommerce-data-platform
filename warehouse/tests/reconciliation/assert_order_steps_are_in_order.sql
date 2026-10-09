-- An order is shipped after it is placed, delivered after it is shipped,
-- returned after it is delivered. Returns the orders that break the sequence.
select order_id, order_ts, shipped_at, delivered_at, returned_at
from {{ ref('fct_orders') }}
where shipped_at < order_ts
   or delivered_at < shipped_at
   or returned_at < delivered_at
