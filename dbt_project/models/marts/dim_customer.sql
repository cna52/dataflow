select customer_id, email, region, created_at, updated_at
from {{ ref('stg_customers') }}
