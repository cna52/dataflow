select order_item_id from {{ ref('fact_order_items') }}
where revenue_cents < 0 or (order_status <> 'paid' and revenue_cents <> 0)
