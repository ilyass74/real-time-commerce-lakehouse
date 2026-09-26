select
    payment_id as payment_key,
    order_id,
    payment_reference,
    amount,
    currency,
    method,
    status,
    created_at,
    updated_at
from {{ ref('stg_payments') }}
