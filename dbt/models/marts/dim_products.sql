with products as (
    select * from {{ ref('stg_products') }}
),

translations as (
    select * from {{ ref('stg_category_translation') }}
),

final as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name, 'unknown') as category_name,
        p.product_category_name as category_name_portuguese,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    from products p
    left join translations t
        on p.product_category_name = t.product_category_name
)

select * from final
