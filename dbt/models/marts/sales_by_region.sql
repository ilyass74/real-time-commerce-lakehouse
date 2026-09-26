select
    c.city,
    f.currency,
    count(distinct f.order_id) as orders,
    sum(f.quantity) as units_sold,
    cast(sum(f.revenue) as decimal(16,2)) as total_revenue,
    cast(
        sum(f.revenue) / count(distinct f.order_id)
        as decimal(16,2)
    ) as revenue_per_order
from {{ ref('fact_orders') }} f
inner join {{ ref('dim_customer') }} c
    on f.customer_key = c.customer_key
group by
    c.city,
    f.currency
