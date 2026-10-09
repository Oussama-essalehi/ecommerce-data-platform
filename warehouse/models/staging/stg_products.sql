select
    product_id,
    name as product_name,
    category,
    brand,
    unit_price,
    currency,
    is_active,
    created_at,
    updated_at
from {{ source('lake', 'products') }}
