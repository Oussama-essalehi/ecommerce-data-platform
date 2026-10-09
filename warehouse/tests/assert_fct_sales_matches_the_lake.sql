-- The sales fact must account for every order line loaded from the lake, to
-- the cent. A row returned here means lines were lost or duplicated on the
-- way through the star schema.
with lake as (

    select count(*) as line_count, sum(quantity) as units, sum(net_amount) as net_amount
    from {{ source('lake', 'order_lines') }}

),

fact as (

    select count(*) as line_count, sum(quantity) as units, sum(net_amount) as net_amount
    from {{ ref('fct_sales') }}

)

select lake.*, fact.line_count as fact_line_count, fact.units as fact_units,
       fact.net_amount as fact_net_amount
from lake
cross join fact
where lake.line_count != fact.line_count
   or lake.units != fact.units
   or lake.net_amount != fact.net_amount
