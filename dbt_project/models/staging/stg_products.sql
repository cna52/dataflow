select product_id, product_name, category_id, valid_from, valid_to
from {{ source('raw', 'source_products') }}
