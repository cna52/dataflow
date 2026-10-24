select customer_id, email, region, created_at, updated_at
from {{ source('raw', 'source_customers') }}
