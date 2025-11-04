with src as (
    select
        userId,
        sessionId,
        channel
    from {{ source('raw', 'user_session_channel') }}
)
select *
from src
