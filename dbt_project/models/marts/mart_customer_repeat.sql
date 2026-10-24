select customer_id, count(*) as paid_order_count,
       min(order_date) as first_purchase_date,
       max(order_date) as last_purchase_date,
       sum(revenue_cents) as lifetime_revenue_cents,
       count(*) > 1 as is_repeat_customer
from {{ ref('fact_orders') }}
where status='paid' and revenue_cents>0
group by 1
