select *
from {{ ref('fact_orders') }}
where revenue < 0
   or quantity <= 0
   or unit_price < 0
