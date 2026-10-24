with paid as (
  select order_id, sum(amount_cents) as settled_cents
  from {{ ref('stg_transactions') }}
  where kind='payment' and status='settled'
  group by 1
)
select o.order_id
from {{ ref('fact_orders') }} o
left join paid p using (order_id)
where o.status='paid' and coalesce(p.settled_cents,0) < o.gross_cents
