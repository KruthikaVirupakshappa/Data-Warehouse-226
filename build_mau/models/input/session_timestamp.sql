with src as (
    select
        sessionId,
        ts
    from {{ source('raw', 'session_timestamp') }}
)
select *
from src