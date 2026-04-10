select distinct
    seller_id,
    seller_name
from {{ ref('stg_smartphones') }}