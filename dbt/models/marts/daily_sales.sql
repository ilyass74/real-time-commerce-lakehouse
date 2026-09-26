select
    d.full_date as order_date,
    f.currency,

    count(distinct f.order_id) as orders,
    sum(f.quantity) as units_sold,

    cast(
        sum(f.revenue)
        as decimal(16,2)
    ) as total_revenue

from {{ ref('fact_orders') }} f

inner join {{ ref('dim_date') }} d
    on f.date_key = d.date_key

group by
    d.full_date,
    f.currency
