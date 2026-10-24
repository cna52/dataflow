select f.order_item_id
from {{ ref('fact_order_items') }} f
join {{ ref('dim_product') }} p using (product_version_id)
where f.ordered_at < p.valid_from
   or (p.valid_to is not null and f.ordered_at >= p.valid_to)
