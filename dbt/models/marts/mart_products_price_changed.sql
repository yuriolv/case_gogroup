with base as (

    select
        product_id,
        price,
        collected_at
    from {{ ref('fct_smartphone_prices') }}

),

products_with_variation as (

    select
        product_id
    from base
    group by product_id
    having count(distinct price) > 1

),

aggregated as (

    select
        b.product_id,
        min(b.price) as min_price,
        max(b.price) as max_price,
        min(b.collected_at) as first_date,
        max(b.collected_at) as last_date
    from base b
    inner join products_with_variation v
        on b.product_id = v.product_id
    group by b.product_id

),

final as (

    select
        *,
        (max_price - min_price) as price_variation,
        (max_price - min_price) / nullif(min_price, 0) as variation_pct
    from aggregated

)

select * from final