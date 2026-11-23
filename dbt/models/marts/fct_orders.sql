{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        dist='customer_id',
        sort=['order_purchase_timestamp', 'order_status']
    )
}}

with order_items_agg as (
    select
        oi.order_id,
        count(oi.order_item_id) as total_items,
        sum(oi.price) as total_item_value,
        sum(oi.freight_value) as total_freight_value,
        sum(oi.total_item_value) as total_order_cost
    from {{ ref('stg_order_items') }} oi
    group by oi.order_id
),

payment_by_type as (
    select
        p.order_id,
        p.payment_type,
        sum(p.payment_value) as type_payment_value,
        min(p.payment_sequential) as first_payment_seq
    from {{ ref('stg_payments') }} p
    group by p.order_id, p.payment_type
),

primary_payment as (
    -- Xác định phương thức thanh toán chính theo tổng giá trị lớn nhất (không dùng min theo bảng chữ cái)
    select
        order_id,
        payment_type as primary_payment_type,
        row_number() over (
            partition by order_id
            order by type_payment_value desc, first_payment_seq asc, payment_type asc
        ) as rn
    from payment_by_type
),

payments_agg as (
    select
        p.order_id,
        sum(p.payment_value) as total_payment_value,
        max(p.payment_installments) as max_installments,
        pp.primary_payment_type
    from {{ ref('stg_payments') }} p
    left join primary_payment pp
        on
            p.order_id = pp.order_id
            and pp.rn = 1
    group by p.order_id, pp.primary_payment_type
),

reviews_agg as (
    select
        r.order_id,
        avg(r.review_score) as review_score,
        max(coalesce(r.review_answer_timestamp, r.review_creation_date)) as latest_review_at
    from {{ ref('stg_reviews') }} r
    group by r.order_id
),

{% if is_incremental() %}
-- Tập hợp toàn bộ order_id bị ảnh hưởng từ cả 4 nguồn upstream (orders, items, payments, reviews)
-- để đảm bảo thay đổi ở bảng con (kể cả đơn cũ > 3 ngày) vẫn được cập nhật vào fct_orders.
affected_order_ids as (
    -- 1. Đơn hàng mới, đơn trong lookback 3 ngày (mua/giao), hoặc đơn có thay đổi trạng thái/mốc giao
    select o.order_id
    from {{ ref('stg_orders') }} o
    left join {{ this }} t
        on o.order_id = t.order_id
    where
        t.order_id is null
        or o.order_purchase_timestamp >= (
            select dateadd(day, -3, coalesce(max(tgt.order_purchase_timestamp), cast('2016-01-01' as timestamp)))
            from {{ this }} tgt
        )
        or o.order_delivered_customer_date >= (
            select dateadd(day, -3, coalesce(max(tgt.order_delivered_customer_date), cast('2016-01-01' as timestamp)))
            from {{ this }} tgt
        )
        or coalesce(o.order_status, '') <> coalesce(t.order_status, '')
        or coalesce(o.order_approved_at, cast('1900-01-01' as timestamp))
        <> coalesce(t.order_approved_at, cast('1900-01-01' as timestamp))
        or coalesce(o.order_delivered_customer_date, cast('1900-01-01' as timestamp))
        <> coalesce(t.order_delivered_customer_date, cast('1900-01-01' as timestamp))
        or coalesce(o.order_estimated_delivery_date, cast('1900-01-01' as timestamp))
        <> coalesce(t.order_estimated_delivery_date, cast('1900-01-01' as timestamp))

    union

    -- 2. Thay đổi từ upstream order_items (phát hiện qua đối chiếu aggregate với bảng đích)
    select t.order_id
    from {{ this }} t
    left join order_items_agg oi
        on t.order_id = oi.order_id
    where
        coalesce(oi.total_items, 0) <> coalesce(t.total_items, 0)
        or coalesce(oi.total_item_value, 0) <> coalesce(t.total_item_value, 0)
        or coalesce(oi.total_freight_value, 0) <> coalesce(t.total_freight_value, 0)

    union

    -- 3. Thay đổi từ upstream payments (bảng nguồn không có timestamp -> bắt trực tiếp theo thay đổi giá trị)
    select t.order_id
    from {{ this }} t
    left join payments_agg p
        on t.order_id = p.order_id
    where
        coalesce(p.total_payment_value, 0) <> coalesce(t.total_payment_value, 0)
        or coalesce(p.max_installments, -1) <> coalesce(t.max_installments, -1)
        or coalesce(p.primary_payment_type, '') <> coalesce(t.primary_payment_type, '')

    union

    -- 4. Thay đổi từ upstream reviews (lookback theo review date hoặc thay đổi review_score)
    select t.order_id
    from {{ this }} t
    left join reviews_agg r
        on t.order_id = r.order_id
    where
        r.latest_review_at >= (
            select dateadd(day, -3, coalesce(max(tgt.order_purchase_timestamp), cast('2016-01-01' as timestamp)))
            from {{ this }} tgt
        )
        or coalesce(cast(r.review_score as decimal(10, 4)), -1)
        <> coalesce(cast(t.review_score as decimal(10, 4)), -1)
),

orders as (
    select o.*
    from {{ ref('stg_orders') }} o
    inner join affected_order_ids a
        on o.order_id = a.order_id
),
{% else %}
orders as (
    select *
    from {{ ref('stg_orders') }}
),
{% endif %}

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
        coalesce(i.total_order_cost, 0) as total_order_cost,
        coalesce(p.total_payment_value, 0) as total_payment_value,
        p.primary_payment_type,
        p.max_installments,

        -- Chỉ số vận chuyển (Delivery Performance Metrics)
        datediff(day, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_duration_days,
        datediff(day, o.order_purchase_timestamp, o.order_estimated_delivery_date) as estimated_delivery_duration_days,
        datediff(day, o.order_estimated_delivery_date, o.order_delivered_customer_date) as delay_days,
        case
            when o.order_delivered_customer_date is null then null
            when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1
            else 0
        end as is_delayed,
        case
            when o.order_delivered_customer_date is null then 'Pending Delivery'
            when o.order_delivered_customer_date > o.order_estimated_delivery_date then 'Delayed'
            else 'On Time'
        end as delivery_status,

        -- Chỉ số hài lòng khách hàng (Customer Satisfaction)
        r.review_score

    from orders o
    left join order_items_agg i on o.order_id = i.order_id
    left join payments_agg p on o.order_id = p.order_id
    left join reviews_agg r on o.order_id = r.order_id
)

select * from final
