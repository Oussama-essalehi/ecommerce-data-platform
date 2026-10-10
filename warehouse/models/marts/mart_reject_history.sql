-- Rejected records per source and reason, run after run, with the change
-- since the previous run. A reason whose count jumps is a new problem in a
-- source system. Filter on is_latest_run for the current state.
with history as (

    select * from {{ ref('stg_quality_reject_history') }}

),

latest as (

    select max(checked_at) as latest_checked_at from history

)

select
    history.run_id,
    history.checked_at,
    history.checked_at::date as checked_date,
    history.checked_at = latest.latest_checked_at as is_latest_run,
    history.source,
    history.reason,
    history.rejected,
    history.rejected - lag(history.rejected) over (
        partition by history.source, history.reason
        order by history.checked_at
    ) as change_since_previous_run
from history
cross join latest
