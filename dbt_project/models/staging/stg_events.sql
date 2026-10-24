select event_id, customer_id, product_id, event_type, occurred_at,
       cast(occurred_at as date) as event_date
from {{ source('raw', 'source_events') }}
