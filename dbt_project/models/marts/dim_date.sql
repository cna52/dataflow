with bounds as (
  select min(order_date) as start_date, max(order_date) as end_date
  from {{ ref('stg_orders') }}
)
select cast(day as date) as date_key,
       year(day) as year,
       month(day) as month,
       day(day) as day_of_month,
       dayofweek(day) as day_of_week,
       date_trunc('month', day)::date as month_start
from bounds, generate_series(start_date, end_date, interval 1 day) as dates(day)
