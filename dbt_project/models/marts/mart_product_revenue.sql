select date_trunc('month', i.order_date)::date as month_start,
       i.product_id, p.product_name, p.category_name,
       sum(i.quantity) filter (where i.revenue_cents>0) as units_sold,
       sum(i.revenue_cents) as revenue_cents
from {{ ref('fact_order_items') }} i
join {{ ref('dim_product') }} p using (product_version_id)
group by 1,2,3,4
