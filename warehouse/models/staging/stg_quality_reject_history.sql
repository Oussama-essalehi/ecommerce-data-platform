select
    run_id,
    checked_at,
    source,
    reason,
    rejected
from {{ source('lake', 'quality_reject_history') }}
