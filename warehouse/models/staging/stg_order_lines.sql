select
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    gross_amount,
    discount_amount,
    net_amount
from {{ source('lake', 'order_lines') }}
