{{  
  config(
    materialized = 'view',
    alias        = 'raw_product_shoes'
  )  
}}

with base as (

  select
    id,
    title,
    subTitle,
    url,
    image,
    price_sale,
    price_original,
    gender,
    age_group,
    brand,
    extra
  from {{ source('raw', 'RAW_PRODUCT_SHOES_RAW') }}

)

select * from base
