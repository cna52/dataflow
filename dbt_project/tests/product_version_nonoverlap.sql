with versions as (
  select product_id, valid_from, valid_to,
         lead(valid_from) over (partition by product_id order by valid_from) as next_from
  from {{ ref('dim_product') }}
)
select product_id, valid_from
from versions
where valid_from is null
   or (valid_to is not null and valid_to <= valid_from)
   or (next_from is not null and (valid_to is null or valid_to > next_from))
