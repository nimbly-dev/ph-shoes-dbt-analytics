{{ config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key = ["id","dwid"]
) }}

-- 1) Read RAW
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
    extra                               as extra_raw,
    loaded_at
  from {{ ref('raw_product_shoes') }}
),

-- 2) Normalize base fields
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

    case
      when try_parse_json(gender_raw) is not null
           and typeof(try_parse_json(gender_raw)) = 'ARRAY'
        then try_parse_json(gender_raw)
      when gender_raw is not null
        then array_construct(gender_raw)
      else null
    end                                                 as gender_arr,

    case
      when gender_arr is null             then null
      when array_size(gender_arr) > 1     then 'unisex'
      when array_size(gender_arr) = 1     then lower(gender_arr[0]::string)
      else null
    end                                                 as gender,

    case
      when lower(url) like '%://www.nike.%'                          then 'nike'
      when lower(url) like '%://www.adidas.%'                        then 'adidas'
      when lower(url) like '%://atmos.ph/collections/new-balance/%'  then 'newbalance'
      when lower(url) like '%://worldbalance.%'                      then 'worldbalance'
      when lower(url) like '%://www.asics.%'                         then 'asics'
      when lower(url) like '%://www.hoka.%'                          then 'hoka'
      else coalesce(lower(brand_raw), brand_raw)
    end                                                 as brand,

    case
      when extra_raw is null or extra_raw = ''        then null
      when try_parse_json(extra_raw) is not null       then try_parse_json(extra_raw)
      else parse_json('{}')
    end                                                 as extra
  from raw_csv
),

-- 3) Category + brand key for mapping
categorized as (
  select
    e.*,
    case
      when age_group ilike 'older%'                              then 'older-kids-shoes'
      when age_group ilike 'little%'                             then 'little-kids-shoes'
      when age_group ilike '%baby%' or age_group ilike '%toddl%' then 'baby-toddlers-shoes'
      when coalesce(gender,'') = 'women'                         then 'women-shoes'
      else 'men-shoes'
    end as conv_category,
    lower(replace(brand, ' ', '')) as brand_key
  from enriched e
),

-- 4) CSV seed (UK/EU/CM → US)
size_map as (
  select
    lower(replace(brand, ' ', ''))  as brand_key,
    category,
    lower(gender)                   as gender_map,
    us,
    uk::float         as uk,
    eu::float         as eu,
    mondo_cm::float   as cm
  from {{ ref('shoe_size_map') }}
),

-- 5) Pull sizes from extra, supporting either `sizes` or `variants[*].size`
sizes_flat as (
  select
    p.dwid, p.year, p.month, p.day,
    p.id, p.title, p.subtitle, p.url, p.image,
    p.price_sale, p.price_original,
    p.gender, p.age_group, p.brand,
    p.conv_category,
    p.brand_key,
    p.extra,
    coalesce(nullif(f.value:label::string,''),
             nullif(f.value:size::string,''),
             nullif(f.value::string,'')) as size_raw
  from categorized p,
  lateral flatten(
    input =>
      iff(try_parse_json(p.extra):sizes    is not null, try_parse_json(p.extra):sizes,
      iff(try_parse_json(p.extra):variants is not null, try_parse_json(p.extra):variants, parse_json('[]')))
  ) f
),

-- 6) Parse & detect size system
sizes_parsed as (
  select
    s.*,
    upper(trim(size_raw)) as size_u,

    -- Kids: "1Y", "12C", or "1 Y"/"12 C"
    iff(regexp_like(upper(size_raw), '([0-9]+(\.[0-9])?)\s*[YC]$'),
        regexp_replace(upper(size_raw), '.*?([0-9]+(\.[0-9])?)\s*([YC])$', '\\1\\3'),
        null) as us_kids,

    -- Adult US: explicit "US" or bare number when adult category
    iff(
      regexp_like(upper(size_raw), '\bUS\b')
      or (regexp_like(upper(size_raw), '^[0-9]+(\.[0-9])?$')
          and conv_category in ('men-shoes','women-shoes')),
      regexp_substr(upper(size_raw), '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1),
      null
    ) as us_adult,

    -- UK/EU/CM numbers
    iff(regexp_like(upper(size_raw), '\bUK\b'),
        regexp_substr(upper(size_raw), '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1), null) as uk_str,

    iff(regexp_like(upper(size_raw), '\b(EU|EUR)\b'),
        regexp_substr(upper(size_raw), '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1), null) as eu_str,

    iff(regexp_like(upper(size_raw), '\b(CM|MONDO|JP)\b'),
        regexp_substr(upper(size_raw), '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1), null) as cm_str
  from sizes_flat s
),

-- 7) Decide source system + numeric helpers
sizes_typed as (
  select
    p.*,
    case
      when p.us_kids is not null then 'US'
      when p.us_adult is not null then 'US'
      when p.uk_str   is not null then 'UK'
      when p.eu_str   is not null then 'EU'
      when p.cm_str   is not null then 'CM'
      else null
    end as size_system,
    try_to_decimal(p.us_adult) as us_adult_num,
    try_to_decimal(p.uk_str)   as uk_num,
    try_to_decimal(p.eu_str)   as eu_num,
    try_to_decimal(p.cm_str)   as cm_num
  from sizes_parsed p
),

-- 8) Map non‑US → US via seed (brand + category + value)
sizes_mapped as (
  select
    t.*,
    m.us as us_from_map
  from sizes_typed t
  left join size_map m
    on m.brand_key = t.brand_key
   and m.category  = t.conv_category
   and (
        (t.size_system = 'UK' and m.uk = t.uk_num)
     or (t.size_system = 'EU' and m.eu = t.eu_num)
     or (t.size_system = 'CM' and abs(m.cm - t.cm_num) <= 0.1)
   )
),

-- 9) Final US size per flattened size row
sizes_final as (
  select
    dwid, id,
    case
      when size_system = 'US' and us_kids is not null      then us_kids
      when size_system = 'US' and us_adult_num is not null then to_varchar(round(us_adult_num, 1))
      else us_from_map
    end as size_us
  from sizes_mapped
  where size_raw is not null
),

-- 10) Aggregate to US array per product
sizes_agg as (
  select
    dwid, id,
    array_agg(distinct size_us) as sizes_us
  from sizes_final
  where size_us is not null
  group by dwid, id
),

-- 11) Join back and overwrite extra.sizes with the normalized array
to_load as (
  select
    e.dwid, e.year, e.month, e.day,
    e.id, e.title, e.subtitle, e.url, e.image,
    e.price_sale, e.price_original,
    e.gender, e.age_group, e.brand,

    /* Replace extra.sizes with US-normalized sizes if we have them */
    case
      when sa.sizes_us is not null
        then object_insert(coalesce(e.extra, parse_json('{}')), 'sizes', sa.sizes_us, true)
      else e.extra
    end as extra

  from categorized e
  left join sizes_agg sa
    on sa.dwid = e.dwid and sa.id = e.id

  {% if is_incremental() %}
    where e.dwid > coalesce((select max(dwid) from {{ this }}), '00000000')
      and e.price_original <> 0
  {% else %}
    where e.price_original <> 0
  {% endif %}
)

select * from to_load
