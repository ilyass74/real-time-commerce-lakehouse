select
    p.product_key,
    p.sku,
    p.product_name,
    p.category,

    count(distinct f.order_id) as orders,
    sum(f.quantity) as units_sold,

    cast(
        sum(f.revenue)
        as decimal(16,2)
    ) as total_revenue,

    cast(
        avg(f.unit_price)
        as decimal(14,2)
    ) as average_selling_price

from {{ ref('fact_orders') }} f

inner join {{ ref('dim_product') }} p
    on f.product_key = p.product_key

group by
    p.product_key,
    p.sku,
    p.product_name,
    p.category
