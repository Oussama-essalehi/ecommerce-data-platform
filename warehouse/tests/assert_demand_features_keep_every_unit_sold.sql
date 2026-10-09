-- Spreading sales over the product-day grid must not lose any: over the days
-- the feature table covers, its units equal the units of the sales fact.
with covered as (

    select min(date_day) as first_day, max(date_day) as last_day
    from {{ ref('mart_product_demand_features') }}

),

features as (

    select sum(units_sold) as units from {{ ref('mart_product_demand_features') }}

),

sales as (

    select sum(sales.units_sold) as units
    from {{ ref('fct_sales') }} as sales
    cross join covered
    where sales.order_date between covered.first_day and covered.last_day
      and sales.product_key != {{ unknown_key() }}

)

select features.units as feature_units, sales.units as fact_units
from features
cross join sales
where features.units != sales.units
