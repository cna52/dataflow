select o.order_id
from {{ ref('fact_orders') }} o
left join (
  select order_id, sum(revenue_cents) as item_revenue_cents
  from {{ ref('fact_order_items') }} group by 1
) i using (order_id)
where o.revenue_cents <> coalesce(i.item_revenue_cents,0)
