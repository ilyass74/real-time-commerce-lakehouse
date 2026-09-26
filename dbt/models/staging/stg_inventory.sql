select *
from {{ source('silver', 'inventory') }}
