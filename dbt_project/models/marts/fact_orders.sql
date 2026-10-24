{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}
with item_totals as (
  select order_id, count(*) as item_count, sum(gross_cents) as gross_cents,
         sum(revenue_cents) as revenue_cents, max(source_updated_at) as item_updated_at
  from {{ ref('fact_order_items') }} group by 1
), combined as (
  select o.order_id, o.customer_id, o.ordered_at, o.order_date, o.status,
         coalesce(i.item_count,0) as item_count,
         coalesce(i.gross_cents,0) as gross_cents,
         coalesce(i.revenue_cents,0) as revenue_cents,
         greatest(o.updated_at, coalesce(i.item_updated_at,o.updated_at)) as source_updated_at
  from {{ ref('stg_orders') }} o left join item_totals i using (order_id)
)
select * from combined
{% if var('backfill_start', none) and var('backfill_end', none) %}
where order_date between cast('{{ var('backfill_start') }}' as date) and cast('{{ var('backfill_end') }}' as date)
{% elif is_incremental() %}
where source_updated_at >= (select coalesce(max(source_updated_at), timestamp '1970-01-01') from {{ this }})
{% endif %}
