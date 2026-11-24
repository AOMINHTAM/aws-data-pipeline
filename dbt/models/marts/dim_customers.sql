{{
    config(
        materialized='incremental',
        unique_key='customer_unique_id',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        dist='customer_unique_id',
        sort=['customer_unique_id', 'most_recent_order_date']
    )
}}

-- Grain chuẩn Kimball: 1 dòng = 1 khách hàng thật (customer_unique_id)
-- Xóa bỏ hoàn toàn bẫy nhân đôi lifetime_spend khi SUM() trên BI cho khách quay lại.
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

customer_order_history as (
    select
        c.customer_unique_id,
        c.city,
        c.state,
        c.zip_code,
        o.order_id,
        o.order_purchase_timestamp,
        coalesce(p.total_payment, 0) as order_payment_value
    from customers c
    inner join orders o
        on c.customer_id = o.customer_id
    left join payments_by_order p
        on o.order_id = p.order_id
),

latest_address_ranked as (
    -- Với 252 khách quay lại có thay đổi địa chỉ giữa các đơn, lấy địa chỉ của đơn hàng gần nhất
    select
        customer_unique_id,
        city,
        state,
        zip_code,
        row_number() over (
            partition by customer_unique_id
            order by order_purchase_timestamp desc, order_id desc
        ) as rn
    from customer_order_history
),

latest_address as (
    select
        customer_unique_id,
        city,
        state,
        zip_code
    from latest_address_ranked
    where rn = 1
),

customer_metrics_all as (
    select
        customer_unique_id,
        min(order_purchase_timestamp) as first_order_date,
        max(order_purchase_timestamp) as most_recent_order_date,
        count(distinct order_id) as total_orders,
        sum(order_payment_value) as lifetime_spend
    from customer_order_history
    group by customer_unique_id
),

{% if is_incremental() %}
affected_customers as (
    -- 1. Khách có đơn mới trong cửa sổ lookback 3 ngày
    select distinct customer_unique_id
    from customer_order_history
    where
        order_purchase_timestamp >= (
            select dateadd(day, -3, coalesce(max(tgt.most_recent_order_date), cast('2016-01-01' as timestamp)))
            from {{ this }} tgt
        )

    union

    -- 2. Khách hàng mới chưa tồn tại trong bảng đích {{ this }}
    select cm.customer_unique_id
    from customer_metrics_all cm
    left join {{ this }} target
        on cm.customer_unique_id = target.customer_unique_id
    where target.customer_unique_id is null

    union

    -- 3. Khách cũ có thay đổi lifetime_spend (late payment), total_orders hoặc địa chỉ gần nhất
    select cm.customer_unique_id
    from customer_metrics_all cm
    inner join latest_address la
        on cm.customer_unique_id = la.customer_unique_id
    inner join {{ this }} target
        on cm.customer_unique_id = target.customer_unique_id
    where
        coalesce(cm.lifetime_spend, 0) <> coalesce(target.lifetime_spend, 0)
        or coalesce(cm.total_orders, 0) <> coalesce(target.total_orders, 0)
        or coalesce(la.city, '') <> coalesce(target.city, '')
        or coalesce(la.state, '') <> coalesce(target.state, '')
        or coalesce(la.zip_code, '') <> coalesce(target.zip_code, '')
),

target_customer_metrics as (
    select cm.*
    from customer_metrics_all cm
    inner join affected_customers ac
        on cm.customer_unique_id = ac.customer_unique_id
),
{% else %}
target_customer_metrics as (
    select * from customer_metrics_all
),
{% endif %}

final as (
    select
        cm.customer_unique_id,
        la.city,
        la.state,
        la.zip_code,
        cm.first_order_date,
        cm.most_recent_order_date,
        cm.total_orders,
        cm.lifetime_spend,
        case
            when cm.total_orders > 1 then 'Returning Customer'
            else 'One-Time Customer'
        end as customer_segment
    from target_customer_metrics cm
    inner join latest_address la
        on cm.customer_unique_id = la.customer_unique_id
)

select * from final
