select order_item_id, order_id, customer_id, product_id, category_id,
       ordered_at, cast(ordered_at as date) as order_date,
       order_status, quantity, unit_price_cents, currency, source_updated_at
from {{ source('raw', 'source_joined_order_items') }}
