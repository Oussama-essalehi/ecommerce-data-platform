-- One row per quality check and per run: the history behind a data-quality
-- dashboard. Filter on is_latest_run for the current state, or follow one
-- check over checked_at to see it drift.
with results as (

    select * from {{ ref('stg_quality_check_results') }}

),

latest as (

    select max(checked_at) as latest_checked_at from results

)

select
    results.run_id,
    results.checked_at,
    results.checked_at::date as checked_date,
    results.checked_at = latest.latest_checked_at as is_latest_run,
    results.layer,
    results.table_name,
    results.layer || '.' || results.table_name as checked_table,
    results.check_name,
    results.dimension,
    results.severity,
    results.description,
    results.column_name,
    results.status,
    results.status != 'passed' as is_problem,
    results.element_count,
    results.unexpected_count,
    results.unexpected_percent,
    results.observed_value,
    results.sample
from results
cross join latest
