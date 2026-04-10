with base as (

    select
        product_id,
        title,
        brand,
        seller_id,

        collected_at,

        price,
        original_price,
        discount_pct,

        stock,
        free_shipping,
        condition

    from {{ ref('stg_smartphones') }}

),

lagged as (

    select
        *,
        lag(stock) over (
            partition by product_id
            order by collected_at
        ) as prev_stock

    from base

),

final as (

    select
        *,
        case
            when prev_stock is null then 0
            when stock > prev_stock then 0
            else prev_stock - stock
        end as sales_estimated

    from lagged

)

select * from final