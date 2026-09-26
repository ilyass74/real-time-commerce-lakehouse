select
    c.customer_key,
    c.full_name,
    c.city,

    count(distinct f.order_id) as total_orders,
    sum(f.quantity) as units_purchased,

    cast(
        sum(f.revenue)
        as decimal(16,2)
    ) as lifetime_revenue,

    cast(
        sum(f.revenue) /
        count(distinct f.order_id)
        as decimal(16,2)
    ) as average_order_value,

    min(f.order_created_at) as first_order_at,
    max(f.order_created_at) as last_order_at

from {{ ref('fact_orders') }} f

inner join {{ ref('dim_customer') }} c
    on f.customer_key = c.customer_key

group by
    c.customer_key,
    c.full_name,
    c.city
