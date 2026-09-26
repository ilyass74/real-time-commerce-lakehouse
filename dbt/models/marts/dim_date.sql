with dates as (

    select distinct
        cast(created_at as date) as full_date
    from {{ ref('stg_orders') }}
    where created_at is not null

)

select
    cast(date_format(full_date, 'yyyyMMdd') as int) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    day(full_date) as day_of_month,
    dayofweek(full_date) as day_of_week
from dates
