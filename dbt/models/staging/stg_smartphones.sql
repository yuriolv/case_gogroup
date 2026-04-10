with source as (

    select * 
    from {{ source('raw', 'smartphones') }}

),

filtered as (

    select *
    from source
    where not (
        lower(title) like '%cabo%'
        or lower(title) like '%película%'
        or lower(title) like '%pelicula%'
        or lower(title) like '%capinha%'
        or lower(title) like '%capa%'
        or lower(title) like '%smartwatch%'
        or lower(title) like '%conector%'
        or lower(title) like '%caneta%'
        or lower(title) like '%tela de celular%'
    )

),

renamed as (

    select
        product_id,
        title,
        price::numeric as price,
        original_price::numeric as original_price,
        seller_id,
        seller_name,
        condition,
        free_shipping::boolean,
        stock::int,
        collected_at::timestamp as collected_at,

        (price - original_price) as discount_value,

        case 
            when original_price > 0 
            then (original_price - price) / original_price
            else 0
        end as discount_pct

    from filtered

)

select * from renamed