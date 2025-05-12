{{ config(materialized='table') }}

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
  dwid,
  year,
  month,
  day
from {{ ref('stg_product_shoes') }};
