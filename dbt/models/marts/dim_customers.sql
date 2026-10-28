with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        sum(payment_value) as total_payment
    from {{ ref('stg_payments') }}
    group by order_id
),

customer_orders as (
    select
        c.customer_unique_id,
        min(o.order_purchase_timestamp) as first_order_date,
        max(o.order_purchase_timestamp) as most_recent_order_date,
        count(distinct o.order_id) as total_orders,
        coalesce(sum(p.total_payment), 0) as lifetime_spend
    from customers c
    inner join orders o on c.customer_id = o.customer_id
    left join payments p on o.order_id = p.order_id
    group by c.customer_unique_id
),

final as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.city,
        c.state,
        c.zip_code,
        co.first_order_date,
        co.most_recent_order_date,
        co.total_orders,
        co.lifetime_spend,
        case
            when co.total_orders > 1 then 'Returning Customer'
            else 'One-Time Customer'
        end as customer_segment
    from customers c
    left join customer_orders co
        on c.customer_unique_id = co.customer_unique_id
)

select * from final
