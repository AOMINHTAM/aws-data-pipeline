-- Kiểm tra phương trình tài chính ngang trên từng dòng của fct_orders:
-- total_order_cost phải khớp total_item_value + total_freight_value (sai số <= 0.01)
select
    order_id,
    total_item_value,
    total_freight_value,
    total_order_cost
from {{ ref('fct_orders') }}
where abs(total_order_cost - (total_item_value + total_freight_value)) > 0.01
