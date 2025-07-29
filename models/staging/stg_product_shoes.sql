{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["id", "dwid"]
) }}

with

  -- 1) Read raw landing, pull in the raw extra string
  raw_csv as (
    select
      id,
      title,
      subtitle,
      url,
      nullif(image, '')                   as image,
      price_sale,
      price_original,
      gender                              as gender_raw,
      age_group,
      brand                               as brand_raw,
      extra                               as extra_raw,
      loaded_at
    from {{ ref('raw_product_shoes') }}
  ),

  -- 2) Enrich: compute dwid/year/month/day, parse gender array, detect brand, parse extra JSON
  enriched as (
    select
      to_char(date_trunc('day', loaded_at), 'YYYYMMDD')      as dwid,
      extract(year  from loaded_at)::int                    as year,
      extract(month from loaded_at)::int                    as month,
      extract(day   from loaded_at)::int                    as day,

      id,
      title,
      subtitle,
      url,
      image,
      price_sale,
      price_original,
      age_group,

      case
        when try_parse_json(gender_raw) is not null
             and typeof(try_parse_json(gender_raw)) = 'ARRAY'
          then try_parse_json(gender_raw)
        when gender_raw is not null
          then array_construct(gender_raw)
        else null
      end                                                    as gender_arr,

      case
        when gender_arr is null            then null
        when array_size(gender_arr) > 1    then 'unisex'
        when array_size(gender_arr) = 1    then lower(gender_arr[0]::string)
        else null
      end                                                    as gender,

      case
        when lower(url) like '%://www.nike.%'                        then 'nike'
        when lower(url) like '%://www.adidas.%'                      then 'adidas'
        when lower(url) like '%://atmos.ph/collections/new-balance/%' then 'newbalance'
        when lower(url) like '%://worldbalance.%'                    then 'worldbalance'
        when lower(url) like '%://www.asics.%'                       then 'asics'
        when lower(url) like '%://www.hoka.%'                        then 'hoka'
        else brand_raw
      end                                                    as brand,

      try_parse_json(extra_raw)                              as extra

    from raw_csv
  ),

  -- 3) Filter out any row where extra_raw was non‐NULL but failed to parse
  filtered as (
    select *
    from enriched
    where
      extra_raw is null    -- no JSON to begin with ⇒ keep
      or extra     is not null  -- parsed successfully ⇒ keep
  ),

  -- 4) Finally pick rows to load (incrementally or full)
  to_load as (
    select
      dwid, year, month, day,
      id, title, subtitle, url, image,
      price_sale, price_original,
      gender, age_group, brand,
      extra
    from filtered

    {% if is_incremental() %}
      where
        dwid > coalesce((select max(dwid) from {{ this }}), '00000000')
        and price_original <> 0
    {% else %}
      where price_original <> 0
    {% endif %}
  )

select * from to_load;
