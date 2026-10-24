select order_item_id, order_id, product_id, quantity, unit_price_cents,
       currency, updated_at
from {{ source('raw', 'source_order_items') }}
