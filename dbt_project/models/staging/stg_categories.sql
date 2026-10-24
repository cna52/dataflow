select category_id, category_name from {{ source('raw', 'source_categories') }}
