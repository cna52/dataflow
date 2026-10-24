{{ config(materialized='incremental', unique_key='order_date', incremental_strategy='merge') }}
with daily as (
  select order_date, count(*) as orders_placed,
         count(*) filter (where status='paid') as paid_orders,
         count(*) filter (where status='cancelled') as cancelled_orders,
         count(*) filter (where status='refunded') as refunded_orders,
         sum(revenue_cents) as revenue_cents,
         max(source_updated_at) as source_updated_at
  from {{ ref('fact_orders') }}
  group by 1
)
select *, revenue_cents::double / nullif(paid_orders,0) / 100 as average_order_value_usd,
       cancelled_orders::double / nullif(orders_placed,0) as cancellation_rate
from daily
{% if var('backfill_start', none) and var('backfill_end', none) %}
where order_date between cast('{{ var('backfill_start') }}' as date) and cast('{{ var('backfill_end') }}' as date)
{% elif is_incremental() %}
where source_updated_at >= (select coalesce(max(source_updated_at), timestamp '1970-01-01') from {{ this }})
{% endif %}
