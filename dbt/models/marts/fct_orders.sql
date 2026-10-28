with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(order_item_id) as total_items,
        sum(price) as total_item_value,
        sum(freight_value) as total_freight_value,
        sum(total_item_value) as total_order_cost
    from {{ ref('stg_order_items') }}
    group by order_id
),

payments as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as max_installments,
        min(payment_type) as primary_payment_type
    from {{ ref('stg_payments') }}
    group by order_id
),

reviews as (
    select
        order_id,
        avg(review_score) as review_score
    from {{ ref('stg_reviews') }}
    group by order_id
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
