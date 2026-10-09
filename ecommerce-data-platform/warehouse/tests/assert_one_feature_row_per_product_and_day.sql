-- The demand feature table has exactly one row per product and day.
select product_key, date_day, count(*) as row_count
from {{ ref('mart_product_demand_features') }}
group by product_key, date_day
having count(*) > 1
