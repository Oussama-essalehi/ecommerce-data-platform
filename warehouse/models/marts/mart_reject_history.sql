-- Rejected records per source and reason, run after run, with the change
-- since the previous run. A reason whose count jumps is a new problem in a
-- source system.
select
    run_id,
    checked_at,
    checked_at::date as checked_date,
    source,
    reason,
    rejected,
    rejected - lag(rejected) over (
        partition by source, reason
        order by checked_at
    ) as change_since_previous_run
from {{ ref('stg_quality_reject_history') }}
