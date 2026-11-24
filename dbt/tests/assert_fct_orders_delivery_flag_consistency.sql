-- Kiểm tra tính nhất quán giữa delivery_status, is_delayed, delay_days và mốc ngày giao
-- Lưu ý: cùng ngày giao với ngày dự kiến (ví dụ 02:03 vs 00:00) có delay_days = 0 nhưng vẫn là Delayed
select
    order_id,
    delivery_status,
    is_delayed,
    delay_days,
    order_delivered_customer_date,
    order_estimated_delivery_date
from {{ ref('fct_orders') }}
where
    (
        delivery_status = 'Pending Delivery'
        and (is_delayed is not null or order_delivered_customer_date is not null)
    )
    or (
        delivery_status = 'Delayed'
        and (
            coalesce(is_delayed, -1) <> 1
            or order_delivered_customer_date is null
            or order_delivered_customer_date <= order_estimated_delivery_date
            or delay_days < 0
        )
    )
    or (
        delivery_status = 'On Time'
        and (
            coalesce(is_delayed, -1) <> 0
            or order_delivered_customer_date is null
            or order_delivered_customer_date > order_estimated_delivery_date
            or delay_days > 0
        )
    )
