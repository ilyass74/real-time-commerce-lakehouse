select distinct
    city as location_key,
    city
from {{ ref('stg_customers') }}
where city is not null
