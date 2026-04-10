select
    product_id,
    seller_id,

    date_trunc('day', collected_at) as date,

    price,
    original_price,
    discount_pct,

    stock,
    free_shipping,
    condition

from {{ ref('stg_smartphones') }}