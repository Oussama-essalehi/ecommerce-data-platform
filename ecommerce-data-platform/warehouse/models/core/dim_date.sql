-- One row per calendar day, from the month of the first order to 90 days
-- after the last one. The extra days let forecasts be joined to a calendar
-- that already knows its weekends, holidays and sales periods.
with bounds as (

    select
        date_trunc('month', min(order_date))::date as first_day,
        (max(order_date) + 90) as last_day
    from {{ ref('stg_orders') }}

),

days as (

    select generate_series(first_day, last_day, interval '1 day')::date as date_day
    from bounds

)

select
    {{ date_key('days.date_day') }} as date_key,
    days.date_day,
    extract(year from days.date_day)::int as year,
    extract(quarter from days.date_day)::int as quarter,
    extract(month from days.date_day)::int as month,
    to_char(days.date_day, 'FMMonth') as month_name,
    to_char(days.date_day, 'YYYY-MM') as year_month,
    extract(isoyear from days.date_day)::int as iso_year,
    extract(week from days.date_day)::int as iso_week,
    -- ISO numbering: 1 is Monday, 7 is Sunday
    extract(isodow from days.date_day)::int as day_of_week,
    to_char(days.date_day, 'FMDay') as day_name,
    extract(isodow from days.date_day) in (6, 7) as is_weekend,
    holidays.holiday_date is not null as is_public_holiday,
    holidays.holiday_name,
    events.event_name is not null as is_sales_event,
    events.event_name as sales_event
from days
left join {{ ref('public_holidays') }} as holidays
    on holidays.holiday_date = days.date_day
left join {{ ref('sales_events') }} as events
    on days.date_day between events.start_date and events.end_date
