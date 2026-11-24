-- Đối soát doanh thu chéo tầng giữa Silver (stg_order_items) và Gold (fct_orders)
-- Nhằm phát hiện sai lệch hoặc thất thoát nếu incremental affected_order_ids bỏ sót dữ liệu
with staging_total as (
    select coalesce(sum(total_item_value), 0) as staging_revenue
    from {{ ref('stg_order_items') }}
),

marts_total as (
    select coalesce(sum(total_order_cost), 0) as marts_revenue
    from {{ ref('fct_orders') }}
)

select
    s.staging_revenue,
    m.marts_revenue,
    abs(m.marts_revenue - s.staging_revenue) as revenue_diff
from staging_total s
cross join marts_total m
where abs(m.marts_revenue - s.staging_revenue) > 0.01
