select
    {{ surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as full_name,
    email,
    city,
    postal_code,
    -- The first two digits of a French postal code are the département.
    left(postal_code, 2) as department_code,
    country,
    signup_date
from {{ ref('stg_customers') }}

union all

-- Orders whose customer is missing or not in the customer list point here.
select
    {{ unknown_key() }} as customer_key,
    'UNKNOWN' as customer_id,
    cast(null as text) as first_name,
    cast(null as text) as last_name,
    'Unknown customer' as full_name,
    cast(null as text) as email,
    'Unknown' as city,
    cast(null as text) as postal_code,
    cast(null as text) as department_code,
    cast(null as text) as country,
    cast(null as date) as signup_date
