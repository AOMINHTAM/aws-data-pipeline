-- Đối soát chéo 2 bảng Fact tầng Gold: fct_order_items (line-item) phải khớp fct_orders (order-level)
-- về cả tổng số lượng món hàng lẫn tổng giá trị hóa đơn (total_item_value vs total_order_cost)
with items_fact as (
    select
        count(*) as item_rows,
        coalesce(sum(total_item_value), 0) as items_total_value
    from {{ ref('fct_order_items') }}
),

orders_fact as (
    select
        coalesce(sum(total_items), 0) as orders_total_items,
        coalesce(sum(total_order_cost), 0) as orders_total_cost
    from {{ ref('fct_orders') }}
)

select
    i.item_rows,
    o.orders_total_items,
    i.items_total_value,
    o.orders_total_cost
from items_fact i
cross join orders_fact o
where
    i.item_rows <> o.orders_total_items
    or abs(i.items_total_value - o.orders_total_cost) > 0.01
