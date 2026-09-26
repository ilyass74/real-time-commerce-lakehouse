with orders as (

    select *
    from {{ ref('stg_orders') }}

),

items as (

    select *
    from {{ ref('stg_order_items') }}

)

select
    i.order_item_id as order_item_key,
    o.order_id,
    o.customer_id as customer_key,
    i.product_id as product_key,

    cast(
        date_format(o.created_at, 'yyyyMMdd')
        as int
    ) as date_key,

    o.status,
    o.currency,

    i.quantity,
    i.unit_price,

    cast(
        i.quantity * i.unit_price
        as decimal(14,2)
    ) as revenue,

    o.created_at as order_created_at,
    o.updated_at as order_updated_at

from orders o

inner join items i
    on o.order_id = i.order_id
