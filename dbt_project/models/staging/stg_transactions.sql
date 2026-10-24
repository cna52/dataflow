select transaction_id, order_id, kind, status, amount_cents, updated_at
from {{ source('raw', 'source_transactions') }}
