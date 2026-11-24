-- Chứng minh grain customer_unique_id trong dim_customers không gây nhân đôi LTV:
-- Tổng lifetime_spend trên dim_customers phải khớp tuyệt đối tổng total_payment_value trên fct_orders
with customer_ltv as (
    select coalesce(sum(lifetime_spend), 0) as total_customer_ltv
    from {{ ref('dim_customers') }}
),

order_payments as (
    select coalesce(sum(total_payment_value), 0) as total_order_payments
    from {{ ref('fct_orders') }}
)

select
    c.total_customer_ltv,
    o.total_order_payments,
    abs(c.total_customer_ltv - o.total_order_payments) as ltv_diff
from customer_ltv c
cross join order_payments o
where abs(c.total_customer_ltv - o.total_order_payments) > 0.01
