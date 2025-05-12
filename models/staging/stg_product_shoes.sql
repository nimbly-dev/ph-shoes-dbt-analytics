{{  
  config(
    materialized='incremental',
    unique_key='dwid'
  ) 
}}

with raw as (

  select
    -- 1) core CSV columns by position (adjust if your header order differs)
    t.$1::string    as id,
    t.$2::string    as title,
    t.$3::string    as subTitle,
    t.$4::string    as url,
    t.$5::string    as image,
    t.$6::float     as price_sale,
    t.$7::float     as price_original,
    t.$8::array     as gender,
    t.$9::string    as age_group,

    -- 2) derive the brand from the filename: e.g. "nike_...csv"
    split_part(split_part(metadata$filename, '/', -1), '_', 1)
      as brand,

    -- 3) pull year/month/day out of the path: raw/{year}/{month}/{day}/...
    to_number(split_part(metadata$filename, '/', 3))  as year,
    to_number(split_part(metadata$filename, '/', 4))  as month,
    to_number(split_part(metadata$filename, '/', 5))  as day,

    -- 4) dwid as YYYYMMDD
    to_varchar(
      split_part(metadata$filename, '/', 3) ||
      lpad(split_part(metadata$filename, '/', 4), 2, '0') ||
      lpad(split_part(metadata$filename, '/', 5), 2, '0')
    ) as dwid

  from @raw_product_shoes_stage (file_format => 'csv_raw_format') t

)

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

from raw

{% if is_incremental() %}
  -- only load new batches
  where dwid > (select max(dwid) from {{ this }})
{% endif %}
