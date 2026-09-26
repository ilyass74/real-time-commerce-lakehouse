select
    customer_id as customer_key,
    customer_id as source_customer_id,
    full_name,
    city
from {{ ref('stg_customers') }}
