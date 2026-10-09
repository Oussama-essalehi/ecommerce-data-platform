-- The total computed for a web shop order must match the total the shop
-- declared when the order was placed. The shop rounds with floating-point
-- arithmetic, so one cent per line is tolerated; anything above means a
-- line is missing or a price was misread.
select
    order_id,
    lines_count,
    total_amount,
    source_total_amount,
    abs(total_amount - source_total_amount) as gap
from {{ ref('fct_orders') }}
where source_total_amount is not null
  and abs(total_amount - source_total_amount) > lines_count * 0.01
