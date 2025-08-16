{# Replace rows when the same product code appears again (merge on brand+id) #}
{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["brand","id"]
  )
}}


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
  from {{ source('raw','raw_product_shoes') }}
),

-- 2A) Normalize base fields: build date parts and VARIANT extra
enriched_a as (
  select
    /* YYYYMMDD string for date portion */
    to_char(date_trunc('day', loaded_at), 'YYYYMMDD')   as ymd,

    extract(year  from loaded_at)::int                  as year,
    extract(month from loaded_at)::int                  as month,
    extract(day   from loaded_at)::int                  as day,

    /* Integer date key for incremental pruning */
    (extract(year  from loaded_at)::int)*10000
    + (extract(month from loaded_at)::int)*100
    + (extract(day   from loaded_at)::int)              as date_key,

    id,
    title,
    subtitle,
    url,
    image,
    price_sale,
    price_original,
    age_group,

    /* gender_raw can be JSON array or a single string */
    case
      when try_parse_json(gender_raw) is not null
           and typeof(try_parse_json(gender_raw)) = 'ARRAY'
        then try_parse_json(gender_raw)
      when gender_raw is not null
        then array_construct(gender_raw)
      else null
    end                                                 as gender_arr,

    /* Normalize brand from domain first; else keep raw */
    case
      when lower(url) like '%://www.nike.%'                          then 'nike'
      when lower(url) like '%://www.adidas.%'                        then 'adidas'
      when lower(url) like '%://atmos.ph/collections/new-balance/%'  then 'newbalance'
      when lower(url) like '%://worldbalance.%'                      then 'worldbalance'
      when lower(url) like '%://www.asics.%'                         then 'asics'
      when lower(url) like '%://www.hoka.%'                          then 'hoka'
      else coalesce(lower(replace(brand_raw,' ','')), lower(brand_raw))
    end                                                 as brand,

    /* EXTRAS: normalize to VARIANT; accept either VARIANT or string JSON */
    case
      when extra_raw is null or extra_raw = ''          then parse_json('{}')
      when try_parse_json(extra_raw) is not null         then try_parse_json(extra_raw)
      else parse_json('{}')
    end                                                 as extra
  from raw_csv
),

-- 2B) Compute gender and final DWID = id||YYYYMMDD (globally unique & replaceable)
enriched as (
  select
    -- Globally unique daily key derived from the product code + date
    (id || ymd)                                as dwid,

    year, month, day, date_key,
    id, title, subtitle, url, image,
    price_sale, price_original, age_group, brand, extra,

    case
      when gender_arr is null             then null
      when array_size(gender_arr) > 1     then 'unisex'
      when array_size(gender_arr) = 1     then lower(gender_arr[0]::string)
      else null
    end as gender
  from enriched_a
),

-- 3A) Category + brand key
categorized_a as (
  select
    e.*,
    case
      when e.age_group ilike 'older%'                              then 'older-kids-shoes'
      when e.age_group ilike 'little%'                             then 'little-kids-shoes'
      when e.age_group ilike '%baby%' or e.age_group ilike '%toddl%' then 'baby-toddlers-shoes'
      when coalesce(e.gender,'') = 'women'                         then 'women-shoes'
      else 'men-shoes'
    end as conv_category,
    lower(replace(e.brand, ' ', '')) as brand_key
  from enriched e
),

-- 3B) Canonical group (men|women|kids) for size mapping
categorized as (
  select
    c.*,
    case
      when coalesce(c.gender,'') = 'women' then 'women'
      when c.conv_category in ('older-kids-shoes','little-kids-shoes','baby-toddlers-shoes') then 'kids'
      else 'men'
    end as conv_group
  from categorized_a c
),

-- 4) CSV size map (UK/EU/CM → US), canonicalize category for the join
size_map as (
  select
    lower(replace(brand, ' ', ''))  as brand_key,
    case
      when lower(category) in ('men','mens','male','m','men-shoes') then 'men'
      when lower(category) in ('women','womens','female','f','women-shoes') then 'women'
      when lower(category) in (
          'kids','kid','children','child','youth',
          'older-kids-shoes','little-kids-shoes','baby-toddlers-shoes',
          'boys','girls'
      ) then 'kids'
      else lower(category)
    end as cat_group,
    lower(gender)                   as gender_map,
    us,
    uk::float         as uk,
    eu::float         as eu,
    mondo_cm::float   as cm
  from {{ ref('shoe_size_map') }}
),

-- 5) Flatten sizes from extra (VARIANT) — support extra.sizes or extra.variants[*].size
sizes_flat as (
  select
    p.dwid, p.year, p.month, p.day, p.date_key,
    p.id, p.title, p.subtitle, p.url, p.image,
    p.price_sale, p.price_original,
    p.gender, p.age_group, p.brand,
    p.conv_category, p.conv_group,
    p.brand_key, p.extra,
    coalesce(nullif(f.value:label::string,''), nullif(f.value:size::string,''), nullif(f.value::string,'')) as size_raw
  from categorized p,
  lateral flatten(
    input =>
      iff(p.extra:sizes    is not null, p.extra:sizes,
      iff(p.extra:variants is not null, p.extra:variants, parse_json('[]')))
  ) f
),

-- 6) Parse & detect size system
sizes_parsed as (
  select
    s.*,
    upper(trim(size_raw)) as size_u,

    -- Kids: "1Y", "12C", or "1 Y"/"12 C"
    iff(regexp_like(size_u, '([0-9]+(\\.[0-9])?)\\s*[YC]$'),
        regexp_replace(size_u, '.*?([0-9]+(\\.[0-9])?)\\s*([YC])$', '\\1\\3'),
        null) as us_kids,

    -- Adult US: explicit "US" anywhere OR bare number if adult group
    iff(
      regexp_like(size_u, '\\bUS\\b')
      or (regexp_like(size_u, '^[0-9]+(\\.[0-9])?$') and conv_group in ('men','women')),
      regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1),
      null
    ) as us_adult,

    -- UK/EU/CM numbers
    iff(regexp_like(size_u, '\\bUK\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), null) as uk_str,

    iff(regexp_like(size_u, '\\b(EU|EUR)\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), null) as eu_str,

    iff(regexp_like(size_u, '\\b(CM|MONDO|JP)\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), null) as cm_str
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

-- 8) Map non-US → US via seed (brand_key + cat_group + numeric value)
sizes_mapped as (
  select
    t.*,
    m.us as us_from_map
  from sizes_typed t
  left join size_map m
    on m.brand_key = t.brand_key
   and m.cat_group = t.conv_group
   and (
        (t.size_system = 'UK' and m.uk = t.uk_num)
     or (t.size_system = 'EU' and m.eu = t.eu_num)
     or (t.size_system = 'CM' and abs(m.cm - t.cm_num) <= 0.1)
   )
),

-- 9) Final US size per flattened row
sizes_final as (
  select
    dwid, id,
    case
      when size_system = 'US' and us_kids is not null then us_kids
      when size_system = 'US' and us_adult_num is not null then
           case
             when round(us_adult_num, 1) = trunc(round(us_adult_num, 1))
               then to_varchar(trunc(round(us_adult_num, 1)))
             else to_varchar(round(us_adult_num, 1))
           end
      else us_from_map
    end as size_us
  from sizes_mapped
  where size_raw is not null
),

-- 10) Aggregate US sizes per product
sizes_agg as (
  select
    dwid, id,
    array_agg(distinct size_us) as sizes_us
  from sizes_final
  where size_us is not null
  group by dwid, id
),

-- 11) Final projection + ensure non-null gender and overwrite extra.sizes
to_load as (
  select
    e.dwid, e.year, e.month, e.day,
    e.id, e.title, e.subtitle, e.url, e.image,
    e.price_sale, e.price_original,

    /* Never null: derive from conv_category if missing, else 'unisex' */
    case
      when e.gender is not null then e.gender
      when e.conv_category = 'women-shoes' then 'women'
      when e.conv_category in ('older-kids-shoes','little-kids-shoes','baby-toddlers-shoes') then 'kids'
      else 'unisex'
    end as gender,

    e.age_group,
    e.brand,

    case
      when sa.sizes_us is not null
        then object_insert(coalesce(e.extra, parse_json('{}')), 'sizes', sa.sizes_us, true)
      else e.extra
    end as extra

  from categorized e
  left join sizes_agg sa
    on sa.dwid = e.dwid and sa.id = e.id

  {% if is_incremental() %}
    -- Prune to only rows newer than what target already has (by date, not by dwid string)
    where e.date_key > coalesce((select max(year*10000+month*100+day) from {{ this }}), 0)
      and e.price_original <> 0
  {% else %}
    where e.price_original <> 0
  {% endif %}
)

select * from to_load
