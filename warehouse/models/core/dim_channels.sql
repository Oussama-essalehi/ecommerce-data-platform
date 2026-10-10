-- One row per sales channel. Both facts carry the channel, so this small
-- table gives reports a single place to filter it from: a slicer on
-- dim_channels filters fct_sales and fct_orders together.
--
-- The channel code is its own key: a handful of short, stable values with
-- no history to keep, where a hashed key would add nothing.
with channels as (

    select distinct
        channel,
        source
    from {{ ref('stg_orders') }}

)

select
    channel,
    initcap(channel) as channel_name,
    source,
    case source
        when 'marketplace' then 'Partner marketplace (daily CSV export)'
        when 'web_shop' then 'Own shop (order events)'
        else source
    end as source_system,
    source = 'web_shop' as is_own_shop
from channels
