with seller_metrics as (

    select
        seller_id,
        sum(sales_estimated) as total_sales_estimated,
        avg(price) as avg_price,
        sum(sales_estimated) * avg(price) as receita_esperada

    from {{ ref('fct_smartphone_prices') }}
    group by seller_id

),

final as (

    select
        *,
        receita_esperada / max(receita_esperada) over () as score_normalizado
    from seller_metrics

)

select * from final