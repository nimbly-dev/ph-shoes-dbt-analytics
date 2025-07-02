{{ config(
    materialized = 'view',
    alias        = 'raw_product_shoes'
) }}

select *
from PH_SHOES_DB.RAW.RAW_PRODUCT_SHOES_RAW
