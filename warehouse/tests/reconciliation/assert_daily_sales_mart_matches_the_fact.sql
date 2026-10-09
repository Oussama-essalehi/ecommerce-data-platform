-- Aggregating must not change the total: the daily sales mart sums to the
-- same net sales and units as the sales fact it is built from.
with mart as (

    select sum(net_sales) as net_sales, sum(units_sold) as units_sold
    from {{ ref('mart_daily_sales') }}

),

fact as (

    select sum(net_sales_amount) as net_sales, sum(units_sold) as units_sold
    from {{ ref('fct_sales') }}

)

select mart.net_sales as mart_net_sales, fact.net_sales as fact_net_sales,
       mart.units_sold as mart_units_sold, fact.units_sold as fact_units_sold
from mart
cross join fact
where mart.net_sales != fact.net_sales
   or mart.units_sold != fact.units_sold
