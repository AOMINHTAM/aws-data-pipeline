{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        incremental_strategy='delete+insert',
        on_schema_change='sync_all_columns',
        dist='order_id',
        sort=['order_purchase_timestamp', 'product_id']
    )
}}

-- Fact bảng chi tiết từng món hàng (Line-Item Fact — 1 dòng = 1 (order_id, order_item_id))
-- Kết nối trực tiếp dim_products, fct_orders và dim_customers ở tầng Gold (Star Schema chuẩn Kimball).
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp
    from {{ ref('stg_orders') }}
),

customers as (
    select
        customer_id,
        customer_unique_id
    from {{ ref('stg_customers') }}
),

enriched_items as (
    select
        oi.order_id || '-' || cast(oi.order_item_id as varchar(10)) as order_item_key,
        oi.order_id,
        oi.order_item_id,
        c.customer_unique_id,
        oi.product_id,
        oi.seller_id,
        o.order_status,
        o.order_purchase_timestamp,
        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        oi.total_item_value
    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join customers c
        on o.customer_id = c.customer_id
)

{% if is_incremental() %}
select ei.*
from enriched_items ei
left join {{ this }} t
    on ei.order_item_key = t.order_item_key
where
    t.order_item_key is null
    or ei.order_purchase_timestamp >= (
        select dateadd(day, -3, coalesce(max(tgt.order_purchase_timestamp), cast('2016-01-01' as timestamp)))
        from {{ this }} tgt
    )
    or coalesce(ei.order_status, '') <> coalesce(t.order_status, '')
    or coalesce(ei.product_id, '') <> coalesce(t.product_id, '')
    or coalesce(ei.seller_id, '') <> coalesce(t.seller_id, '')
    or coalesce(ei.customer_unique_id, '') <> coalesce(t.customer_unique_id, '')
    or coalesce(ei.price, 0) <> coalesce(t.price, 0)
    or coalesce(ei.freight_value, 0) <> coalesce(t.freight_value, 0)
{% else %}
select * from enriched_items
{% endif %}
