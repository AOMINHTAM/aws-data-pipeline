{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns'
    )
}}

with orders as (
    select *
    from {{ ref('stg_orders') }} o
    {% if is_incremental() %}
    -- Lookback window 3 ngày tính từ high-water mark trên {{ this }}:
    -- Vừa nhận đơn hàng mới, vừa cập nhật các đơn cũ mới có ngày giao hàng (late delivery updates)
    where o.order_purchase_timestamp >= (
        select dateadd(day, -3, coalesce(max(order_purchase_timestamp), cast('2016-01-01' as timestamp)))
        from {{ this }}
    )
    or o.order_delivered_customer_date >= (
        select dateadd(day, -3, coalesce(max(order_delivered_customer_date), cast('2016-01-01' as timestamp)))
        from {{ this }}
    )
    {% endif %}
),

order_items as (
    select
        oi.order_id,
        count(oi.order_item_id) as total_items,
        sum(oi.price) as total_item_value,
        sum(oi.freight_value) as total_freight_value,
        sum(oi.total_item_value) as total_order_cost
    from {{ ref('stg_order_items') }} oi
    {% if is_incremental() %}
    -- Chỉ gom nhóm chi tiết mặt hàng cho các đơn nằm trong tập incremental ở trên
    where exists (
        select 1
        from orders o
        where o.order_id = oi.order_id
    )
    {% endif %}
    group by oi.order_id
),

payments as (
    select
        p.order_id,
        sum(p.payment_value) as total_payment_value,
        max(p.payment_installments) as max_installments,
        min(p.payment_type) as primary_payment_type
    from {{ ref('stg_payments') }} p
    {% if is_incremental() %}
    where exists (
        select 1
        from orders o
        where o.order_id = p.order_id
    )
    {% endif %}
    group by p.order_id
),

reviews as (
    select
        r.order_id,
        avg(r.review_score) as review_score
    from {{ ref('stg_reviews') }} r
    {% if is_incremental() %}
    where exists (
        select 1
        from orders o
        where o.order_id = r.order_id
    )
    {% endif %}
    group by r.order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- Chỉ số tài chính (Financial Metrics)
        coalesce(i.total_items, 0) as total_items,
        coalesce(i.total_item_value, 0) as total_item_value,
        coalesce(i.total_freight_value, 0) as total_freight_value,
        coalesce(p.total_payment_value, 0) as total_payment_value,
        p.primary_payment_type,
        p.max_installments,

        -- Chỉ số vận chuyển (Delivery Performance Metrics)
        datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_duration_days,
        datediff(day, o.order_purchase_timestamp, o.order_estimated_delivery_date) as estimated_delivery_duration_days,
        datediff(day, o.order_estimated_delivery_date, o.order_delivered_customer_date) as delay_days,
        case
            when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1
            else 0
        end as is_delayed,

        -- Chỉ số hài lòng khách hàng (Customer Satisfaction)
        r.review_score

    from orders o
    left join order_items i on o.order_id = i.order_id
    left join payments p on o.order_id = p.order_id
    left join reviews r on o.order_id = r.order_id
)

select * from final
