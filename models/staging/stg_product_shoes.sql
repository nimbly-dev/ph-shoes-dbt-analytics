{{ config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key = ["id","dwid"]
) }}

-- Stage 1: read from the RAW view/model
with raw_csv as (
  select
    id,
    title,
    subtitle,
    url,
    nullif(image, '')                  as image,
    price_sale,
    price_original,
    gender                              as gender_raw,
    age_group,
    brand                               as brand_raw,
    extra                               as extra_raw,   -- JSON string or free text
    loaded_at
  from {{ ref('raw_product_shoes') }}   -- your raw view model
),

-- Stage 2: derive date keys, normalize gender & brand, coerce extra to JSON
enriched as (
  select
    to_char(date_trunc('day', loaded_at), 'YYYYMMDD')   as dwid,
    extract(year  from loaded_at)::int                  as year,
    extract(month from loaded_at)::int                  as month,
    extract(day   from loaded_at)::int                  as day,

    id,
    title,
    subtitle,
    url,
    image,
    price_sale,
    price_original,
    age_group,

    /* gender normalization:
       - if JSON array => keep array
       - if scalar     => wrap to array
    */
    case
      when try_parse_json(gender_raw) is not null
           and typeof(try_parse_json(gender_raw)) = 'ARRAY'
        then try_parse_json(gender_raw)
      when gender_raw is not null
        then array_construct(gender_raw)
      else null
    end                                                 as gender_arr,

    /* roll array -> single normalized label:
       - >1 entry => 'unisex'
       - 1 entry  => lowercase scalar
    */
    case
      when gender_arr is null             then null
      when array_size(gender_arr) > 1     then 'unisex'
      when array_size(gender_arr) = 1     then lower(gender_arr[0]::string)
      else null
    end                                                 as gender,

    /* brand normalization, prefer URL host if present */
    case
      when lower(url) like '%://www.nike.%'                          then 'nike'
      when lower(url) like '%://www.adidas.%'                        then 'adidas'
      when lower(url) like '%://atmos.ph/collections/new-balance/%'  then 'newbalance'
      when lower(url) like '%://worldbalance.%'                      then 'worldbalance'
      when lower(url) like '%://www.asics.%'                         then 'asics'
      when lower(url) like '%://www.hoka.%'                          then 'hoka'
      else coalesce(lower(brand_raw), brand_raw)
    end                                                 as brand,

    /* extra to VARIANT:
       - null/'' => null
       - valid JSON string => parsed
       - otherwise => {} to keep downstream JSON paths safe
    */
    case
      when extra_raw is null or extra_raw = ''        then null
      when try_parse_json(extra_raw) is not null       then try_parse_json(extra_raw)
      else parse_json('{}')
    end                                                 as extra

  from raw_csv
),

-- Stage 3: final filter & incremental fence
to_load as (
  select
    dwid, year, month, day,
    id, title, subtitle, url, image,
    price_sale, price_original,
    gender, age_group, brand, extra
  from enriched
  {% if is_incremental() %}
    where dwid > coalesce((select max(dwid) from {{ this }}), '00000000')
      and price_original <> 0
  {% else %}
    where price_original <> 0
  {% endif %}
)

select * from to_load
