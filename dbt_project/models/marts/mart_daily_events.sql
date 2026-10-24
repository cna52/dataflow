select event_date, event_type, count(*) as event_count
from {{ ref('stg_events') }} group by 1,2
