select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    postal_code,
    country,
    signup_ts,
    signup_date
from {{ source('lake', 'customers') }}
