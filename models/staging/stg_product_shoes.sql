{{ 
  config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['id','dwid']
  )
}}

with raw_csv as (
  select
    t.$1::string    as id,
    t.$2::string    as title,
    t.$3::string    as subTitle,
    t.$4::string    as url,
    t.$5::string    as image,
    t.$6::float     as price_sale,
    t.$7::float     as price_original,
    t.$8::array     as gender,
    t.$9::string    as age_group,

    metadata$filename     as file_path,

    current_timestamp()   as loaded_at

  from @PH_SHOES_DB.RAW.S3_RAW_PRODUCT_SHOES_STAGE
    (file_format => 'PH_SHOES_DB.RAW.CSV_RAW_FORMAT') t
),

enriched as (
  select
    -- batch identifiers
    to_varchar(date_trunc('day', loaded_at), 'YYYYMMDD') as dwid,
    year(loaded_at)::int   as year,
    month(loaded_at)::int  as month,
    day(loaded_at)::int    as day,

    -- 9 CSV columns
    id, title, subTitle, url, image,
    price_sale, price_original, gender, age_group,

    -- derive brand from the filename
    split_part(split_part(file_path, '/', -1), '_', 1) as brand

  from raw_csv
),

to_load as (
  select *
  from enriched

  {% if is_incremental() %}
    where dwid > coalesce((select max(dwid) from {{ this }}), '00000000')
  {% endif %}
)

select * from to_load
