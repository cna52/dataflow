with purchases as (
  select distinct customer_id, date_trunc('month', order_date)::date as purchase_month
  from {{ ref('fact_orders') }} where status='paid' and revenue_cents>0
), cohorts as (
  select customer_id, min(purchase_month) as cohort_month from purchases group by 1
), sizes as (
  select cohort_month, count(*) as cohort_size from cohorts group by 1
)
select c.cohort_month, p.purchase_month,
       date_diff('month', c.cohort_month, p.purchase_month) as months_since_first_purchase,
       s.cohort_size, count(*) as active_customers,
       count(*)::double / s.cohort_size as retention_rate
from purchases p join cohorts c using (customer_id)
join sizes s using (cohort_month)
group by 1,2,3,4
