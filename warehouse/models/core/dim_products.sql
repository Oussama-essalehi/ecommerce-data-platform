select
    {{ surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category,
    brand,
    unit_price as current_unit_price,
    currency,
    is_active,
    created_at::date as created_date,
    -- A product that is no longer active was withdrawn at its last change.
    case when not is_active then updated_at::date end as discontinued_date
from {{ ref('stg_products') }}

union all

select
    {{ unknown_key() }} as product_key,
    'UNKNOWN' as product_id,
    'Unknown product' as product_name,
    'Unknown' as category,
    'Unknown' as brand,
    cast(null as numeric(10, 2)) as current_unit_price,
    cast(null as text) as currency,
    false as is_active,
    cast(null as date) as created_date,
    cast(null as date) as discontinued_date
