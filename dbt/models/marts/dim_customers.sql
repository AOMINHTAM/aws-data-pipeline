{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        dist='customer_id',
        sort=['customer_unique_id', 'most_recent_order_date']
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments_by_order as (
    select
        p.order_id,
        sum(p.payment_value) as total_payment
    from {{ ref('stg_payments') }} p
    group by p.order_id
),

{% if is_incremental() %}
current_customer_metrics as (
    select
        c.customer_unique_id,
        count(distinct o.order_id) as total_orders,
        coalesce(sum(p.total_payment), 0) as lifetime_spend
    from customers c
    inner join orders o
        on c.customer_id = o.customer_id
    left join payments_by_order p
        on o.order_id = p.order_id
    group by c.customer_unique_id
),

-- 1. Xác định tập customer_unique_id bị ảnh hưởng:
--    a) Có phát sinh đơn mới trong lookback window 3 ngày,
--    b) Có customer_id mới chưa tồn tại trong bảng đích {{ this }}, HOẶC
--    c) Có thay đổi tổng chi tiêu (lifetime_spend), tổng số đơn (total_orders) hoặc địa chỉ (city/state/zip_code)
--       kể cả khi cập nhật thanh toán trễ (late-arriving payment) trên đơn hàng cũ > 3 ngày!
affected_customers as (
    select distinct c.customer_unique_id
    from customers c
    inner join orders o
        on c.customer_id = o.customer_id
    where
        o.order_purchase_timestamp >= (
            select dateadd(day, -3, coalesce(max(tgt.most_recent_order_date), cast('2016-01-01' as timestamp)))
            from {{ this }} tgt
        )

    union

    select distinct c.customer_unique_id
    from customers c
    where
        not exists (
            select 1
            from {{ this }} target
            where target.customer_id = c.customer_id
        )

    union

    select distinct c.customer_unique_id
    from customers c
    inner join {{ this }} target
        on c.customer_id = target.customer_id
    left join current_customer_metrics cm
        on c.customer_unique_id = cm.customer_unique_id
    where
        coalesce(cm.lifetime_spend, 0) <> coalesce(target.lifetime_spend, 0)
        or coalesce(cm.total_orders, 0) <> coalesce(target.total_orders, 0)
        or coalesce(c.city, '') <> coalesce(target.city, '')
        or coalesce(c.state, '') <> coalesce(target.state, '')
        or coalesce(c.zip_code, '') <> coalesce(target.zip_code, '')
),

-- 2. Lấy toàn bộ các customer_id gắn với những customer_unique_id bị ảnh hưởng (cả cũ lẫn mới)
target_customers as (
    select c.*
    from customers c
    inner join affected_customers ac
        on c.customer_unique_id = ac.customer_unique_id
),

-- 3. Lấy toàn bộ lịch sử đơn hàng của nhóm khách hàng bị ảnh hưởng để aggregate LTV chính xác
target_orders as (
    select o.*
    from orders o
    inner join target_customers tc
        on o.customer_id = tc.customer_id
),
{% else %}
target_customers as (
    select * from customers
),

target_orders as (
    select * from orders
),
{% endif %}

customer_orders as (
    select
        tc.customer_unique_id,
        min(ord.order_purchase_timestamp) as first_order_date,
        max(ord.order_purchase_timestamp) as most_recent_order_date,
        count(distinct ord.order_id) as total_orders,
        coalesce(sum(p.total_payment), 0) as lifetime_spend
    from target_customers tc
    inner join target_orders ord
        on tc.customer_id = ord.customer_id
    left join payments_by_order p
        on ord.order_id = p.order_id
    group by tc.customer_unique_id
),

final as (
    select
        tc.customer_id,
        tc.customer_unique_id,
        tc.city,
        tc.state,
        tc.zip_code,
        co.first_order_date,
        co.most_recent_order_date,
        co.total_orders,
        co.lifetime_spend,
        case
            when co.total_orders > 1 then 'Returning Customer'
            else 'One-Time Customer'
        end as customer_segment
    from target_customers tc
    left join customer_orders co
        on tc.customer_unique_id = co.customer_unique_id
)

select * from final
