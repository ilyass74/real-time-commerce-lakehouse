select
    product_id as product_key,
    product_id as source_product_id,
    sku,
    product_name,
    category,
    price
from {{ ref('stg_products') }}
