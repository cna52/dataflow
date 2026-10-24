{{ config(materialized='incremental', unique_key='order_item_id', incremental_strategy='merge') }}
with payments as (
  select order_id,
         sum(case when kind='payment' and status='settled' then amount_cents else 0 end) as paid_cents,
         sum(case when kind='refund' and status='settled' then amount_cents else 0 end) as refunded_cents,
         max(updated_at) as payment_updated_at
  from {{ ref('stg_transactions') }} group by 1
), lines as (
  select j.*, p.product_version_id,
         greatest(j.source_updated_at, coalesce(t.payment_updated_at, j.source_updated_at)) as change_at,
         coalesce(t.paid_cents,0) as paid_cents,
         coalesce(t.refunded_cents,0) as refunded_cents
  from {{ ref('stg_joined_order_items') }} j
  join {{ ref('dim_product') }} p on j.product_id=p.product_id
    and j.ordered_at>=p.valid_from and (p.valid_to is null or j.ordered_at<p.valid_to)
  left join payments t on j.order_id=t.order_id
)
select order_item_id, order_id, customer_id, product_id, product_version_id,
       category_id, ordered_at, order_date, order_status, quantity,
       unit_price_cents, quantity*unit_price_cents as gross_cents,
       case when order_status='paid' and paid_cents>0 and refunded_cents=0
            then quantity*unit_price_cents else 0 end as revenue_cents,
       change_at as source_updated_at
from lines
{% if var('backfill_start', none) and var('backfill_end', none) %}
where order_date between cast('{{ var('backfill_start') }}' as date) and cast('{{ var('backfill_end') }}' as date)
{% elif is_incremental() %}
where change_at >= (select coalesce(max(source_updated_at), timestamp '1970-01-01') from {{ this }})
{% endif %}
