{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

{% if is_incremental() %}
-- 1. Xác định tập customer_unique_id có phát sinh đơn mới trong lookback window 3 ngày
--    hoặc có customer_id mới chưa tồn tại trong bảng đích {{ this }}.
--    Phải gom theo customer_unique_id để tính lại toàn bộ chỉ số tích lũy (LTV) không bị thiếu lịch sử!
affected_customers as (
    select distinct c.customer_unique_id
    from customers c
    inner join orders o
        on c.customer_id = o.customer_id
    where o.order_purchase_timestamp >= (
        select dateadd(day, -3, coalesce(max(most_recent_order_date), cast('2016-01-01' as timestamp)))
        from {{ this }}
    )

    union

    select distinct c.customer_unique_id
    from customers c
    where not exists (
        select 1
        from {{ this }} target
        where target.customer_id = c.customer_id
    )
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

payments as (
    select
        p.order_id,
        sum(p.payment_value) as total_payment
    from {{ ref('stg_payments') }} p
    {% if is_incremental() %}
    -- Chỉ quét thanh toán của các đơn hàng thuộc tập khách hàng bị ảnh hưởng
    where exists (
        select 1
        from target_orders ord
        where ord.order_id = p.order_id
    )
    {% endif %}
    group by p.order_id
),

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
    left join payments p
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
