select
    date,

    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,

    avg(discount_pct) as avg_discount,

    count(*) as total_products

from {{ ref('fct_smartphone_prices') }}
group by 1