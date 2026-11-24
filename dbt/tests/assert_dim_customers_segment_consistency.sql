-- Kiểm tra tính nhất quán giữa total_orders và customer_segment trong dim_customers (grain customer_unique_id)
select
    customer_unique_id,
    total_orders,
    customer_segment
from {{ ref('dim_customers') }}
where
    total_orders < 1
    or (total_orders > 1 and customer_segment <> 'Returning Customer')
    or (total_orders = 1 and customer_segment <> 'One-Time Customer')
