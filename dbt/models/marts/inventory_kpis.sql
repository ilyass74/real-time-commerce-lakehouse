select
    p.product_key,
    p.sku,
    p.product_name,
    p.category,
    i.quantity as stock_quantity,
    p.price,

    cast(
        i.quantity * p.price
        as decimal(16,2)
    ) as inventory_value,

    case
        when i.quantity = 0 then 'OUT_OF_STOCK'
        when i.quantity < 10 then 'LOW_STOCK'
        when i.quantity < 20 then 'MEDIUM_STOCK'
        else 'IN_STOCK'
    end as stock_status,

    i.updated_at as inventory_updated_at

from {{ ref('stg_inventory') }} i

inner join {{ ref('dim_product') }} p
    on i.product_id = p.product_key
