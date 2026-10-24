select md5(cast(p.product_id as varchar) || ':' || cast(p.valid_from as varchar)) as product_version_id,
       p.product_id, p.product_name, p.category_id, c.category_name,
       p.valid_from, p.valid_to
from {{ ref('stg_products') }} p
join {{ ref('stg_categories') }} c using (category_id)
