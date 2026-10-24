select order_id, customer_id, ordered_at, cast(ordered_at as date) as order_date,
       status, updated_at
from {{ source('raw', 'source_orders') }}
