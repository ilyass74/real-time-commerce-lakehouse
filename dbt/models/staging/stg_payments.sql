select *
from {{ source('silver', 'payments') }}
