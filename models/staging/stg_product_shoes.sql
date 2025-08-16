{% do config(materialized='incremental', incremental_strategy='merge', unique_key=['brand','id']) %}

WITH
-- 0) RAW (DW view) with row-level de-dupe per id+day
raw_base AS (
  SELECT
    r.*,
    to_char(date_trunc('day', r.loaded_at), 'YYYYMMDD') AS ymd,
    row_number() OVER (
      PARTITION BY trim(r.id), to_char(date_trunc('day', r.loaded_at), 'YYYYMMDD')
      ORDER BY r.loaded_at DESC, r.url DESC
    ) AS _rn
  FROM {{ ref('raw_product_shoes') }} r
),

-- 1) Read RAW (deduped)
raw_csv AS (
  SELECT
    trim(id)                              AS id,       -- normalize id
    title,
    subtitle,
    url,
    nullif(image, '')                     AS image,
    price_sale,
    price_original,
    gender                                 AS gender_raw,
    age_group,
    brand                                  AS brand_raw,
    extra                                  AS extra_raw,
    loaded_at,
    ymd
  FROM raw_base
  WHERE _rn = 1
),

-- 2A) Normalize base fields: build date parts and VARIANT extra
enriched_a AS (
  SELECT
    ymd,  -- from raw_csv
    EXTRACT(year  FROM loaded_at)::int    AS year,
    EXTRACT(month FROM loaded_at)::int    AS month,
    EXTRACT(day   FROM loaded_at)::int    AS day,

    /* Integer date key for incremental pruning */
    (EXTRACT(year  FROM loaded_at)::int)*10000
    + (EXTRACT(month FROM loaded_at)::int)*100
    + (EXTRACT(day   FROM loaded_at)::int)          AS date_key,

    id,
    title,
    subtitle,
    url,
    image,
    price_sale,
    price_original,
    age_group,

    /* gender_raw can be JSON array or a single string */
    CASE
      WHEN try_parse_json(gender_raw) IS NOT NULL
           AND typeof(try_parse_json(gender_raw)) = 'ARRAY'
        THEN try_parse_json(gender_raw)
      WHEN gender_raw IS NOT NULL
        THEN array_construct(gender_raw)
      ELSE NULL
    END                                             AS gender_arr,

    /* Normalize brand from domain first; else keep raw */
    CASE
      WHEN lower(url) LIKE '%://www.nike.%'                          THEN 'nike'
      WHEN lower(url) LIKE '%://www.adidas.%'                        THEN 'adidas'
      WHEN lower(url) LIKE '%://atmos.ph/collections/new-balance/%'  THEN 'newbalance'
      WHEN lower(url) LIKE '%://worldbalance.%'                      THEN 'worldbalance'
      WHEN lower(url) LIKE '%://www.asics.%'                         THEN 'asics'
      WHEN lower(url) LIKE '%://www.hoka.%'                          THEN 'hoka'
      ELSE coalesce(lower(replace(brand_raw,' ','')), lower(brand_raw))
    END                                             AS brand,

    /* EXTRAS: normalize to VARIANT; accept either VARIANT or string JSON */
    CASE
      WHEN extra_raw IS NULL OR extra_raw = ''      THEN parse_json('{}')
      WHEN try_parse_json(extra_raw) IS NOT NULL     THEN try_parse_json(extra_raw)
      ELSE parse_json('{}')
    END                                             AS extra
  FROM raw_csv
),

-- 2B) Compute gender and final DWID = id||YYYYMMDD (globally unique & replaceable)
enriched AS (
  SELECT
    /* Globally unique per product per day */
    (id || ymd)                             AS dwid,

    year, month, day, date_key,
    id, title, subtitle, url, image,
    price_sale, price_original, age_group, brand, extra,

    CASE
      WHEN gender_arr IS NULL             THEN NULL
      WHEN array_size(gender_arr) > 1     THEN 'unisex'
      WHEN array_size(gender_arr) = 1     THEN lower(gender_arr[0]::string)
      ELSE NULL
    END AS gender
  FROM enriched_a
),

-- 3A) Category + brand key
categorized_a AS (
  SELECT
    e.*,
    CASE
      WHEN e.age_group ILIKE 'older%'                              THEN 'older-kids-shoes'
      WHEN e.age_group ILIKE 'little%'                             THEN 'little-kids-shoes'
      WHEN e.age_group ILIKE '%baby%' OR e.age_group ILIKE '%toddl%' THEN 'baby-toddlers-shoes'
      WHEN coalesce(e.gender,'') = 'women'                         THEN 'women-shoes'
      ELSE 'men-shoes'
    END AS conv_category,
    lower(replace(e.brand, ' ', '')) AS brand_key
  FROM enriched e
),

-- 3B) Canonical group (men|women|kids) for size mapping
categorized AS (
  SELECT
    c.*,
    CASE
      WHEN coalesce(c.gender,'') = 'women' THEN 'women'
      WHEN c.conv_category IN ('older-kids-shoes','little-kids-shoes','baby-toddlers-shoes') THEN 'kids'
      ELSE 'men'
    END AS conv_group
  FROM categorized_a c
),

-- 4) CSV size map (UK/EU/CM → US), canonicalize category for the join
size_map AS (
  SELECT
    lower(replace(brand, ' ', ''))  AS brand_key,
    CASE
      WHEN lower(category) IN ('men','mens','male','m','men-shoes') THEN 'men'
      WHEN lower(category) IN ('women','womens','female','f','women-shoes') THEN 'women'
      WHEN lower(category) IN (
          'kids','kid','children','child','youth',
          'older-kids-shoes','little-kids-shoes','baby-toddlers-shoes',
          'boys','girls'
      ) THEN 'kids'
      ELSE lower(category)
    END AS cat_group,
    lower(gender)                   AS gender_map,
    us,
    uk::float         AS uk,
    eu::float         AS eu,
    mondo_cm::float   AS cm
  FROM {{ ref('shoe_size_map') }}
),

-- 5) Flatten sizes from extra (VARIANT) — support extra.sizes or extra.variants[*].size
sizes_flat AS (
  SELECT
    p.dwid, p.year, p.month, p.day, p.date_key,
    p.id, p.title, p.subtitle, p.url, p.image,
    p.price_sale, p.price_original,
    p.gender, p.age_group, p.brand,
    p.conv_category, p.conv_group,
    p.brand_key, p.extra,
    coalesce(nullif(f.value:label::string,''), nullif(f.value:size::string,''), nullif(f.value::string,'')) AS size_raw
  FROM categorized p,
  LATERAL FLATTEN(
    input =>
      iff(p.extra:sizes    IS NOT NULL, p.extra:sizes,
      iff(p.extra:variants IS NOT NULL, p.extra:variants, parse_json('[]')))
  ) f
),

-- 6) Parse & detect size system
sizes_parsed AS (
  SELECT
    s.*,
    upper(trim(size_raw)) AS size_u,

    -- Kids: "1Y", "12C", or "1 Y"/"12 C"
    iff(regexp_like(size_u, '([0-9]+(\\.[0-9])?)\\s*[YC]$'),
        regexp_replace(size_u, '.*?([0-9]+(\\.[0-9])?)\\s*([YC])$', '\\1\\3'),
        NULL) AS us_kids,

    -- Adult US: explicit "US" anywhere OR bare number if adult group
    iff(
      regexp_like(size_u, '\\bUS\\b')
      OR (regexp_like(size_u, '^[0-9]+(\\.[0-9])?$') AND conv_group IN ('men','women')),
      regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1),
      NULL
    ) AS us_adult,

    -- UK/EU/CM numbers
    iff(regexp_like(size_u, '\\bUK\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), NULL) AS uk_str,

    iff(regexp_like(size_u, '\\b(EU|EUR)\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), NULL) AS eu_str,

    iff(regexp_like(size_u, '\\b(CM|MONDO|JP)\\b'),
        regexp_substr(size_u, '([0-9]+(\\.[0-9])?)', 1, 1, 'e', 1), NULL) AS cm_str
  FROM sizes_flat s
),

-- 7) Decide source system + numeric helpers
sizes_typed AS (
  SELECT
    p.*,
    CASE
      WHEN p.us_kids  IS NOT NULL THEN 'US'
      WHEN p.us_adult IS NOT NULL THEN 'US'
      WHEN p.uk_str   IS NOT NULL THEN 'UK'
      WHEN p.eu_str   IS NOT NULL THEN 'EU'
      WHEN p.cm_str   IS NOT NULL THEN 'CM'
      ELSE NULL
    END AS size_system,
    try_to_decimal(p.us_adult) AS us_adult_num,
    try_to_decimal(p.uk_str)   AS uk_num,
    try_to_decimal(p.eu_str)   AS eu_num,
    try_to_decimal(p.cm_str)   AS cm_num
  FROM sizes_parsed p
),

-- 8) Map non-US → US via seed (brand_key + cat_group + numeric value)
sizes_mapped AS (
  SELECT
    t.*,
    m.us AS us_from_map
  FROM sizes_typed t
  LEFT JOIN size_map m
    ON m.brand_key = t.brand_key
   AND m.cat_group = t.conv_group
   AND (
        (t.size_system = 'UK' AND m.uk = t.uk_num)
     OR (t.size_system = 'EU' AND m.eu = t.eu_num)
     OR (t.size_system = 'CM' AND abs(m.cm - t.cm_num) <= 0.1)
   )
),

-- 9) Final US size per flattened row
sizes_final AS (
  SELECT
    dwid, id,
    CASE
      WHEN size_system = 'US' AND us_kids IS NOT NULL THEN us_kids
      WHEN size_system = 'US' AND us_adult_num IS NOT NULL THEN
           CASE
             WHEN round(us_adult_num, 1) = trunc(round(us_adult_num, 1))
               THEN to_varchar(trunc(round(us_adult_num, 1)))
             ELSE to_varchar(round(us_adult_num, 1))
           END
      ELSE us_from_map
    END AS size_us
  FROM sizes_mapped
  WHERE size_raw IS NOT NULL
),

-- 10) Aggregate US sizes per product
sizes_agg AS (
  SELECT
    dwid, id,
    array_agg(DISTINCT size_us) AS sizes_us
  FROM sizes_final
  WHERE size_us IS NOT NULL
  GROUP BY dwid, id
),

-- 11) Final projection + ensure non-null gender and overwrite extra.sizes
to_load AS (
  SELECT
    e.dwid, e.year, e.month, e.day,
    e.id, e.title, e.subtitle, e.url, e.image,
    e.price_sale, e.price_original,

    /* Never null: derive from conv_category if missing, else 'unisex' */
    CASE
      WHEN e.gender IS NOT NULL THEN e.gender
      WHEN e.conv_category = 'women-shoes' THEN 'women'
      WHEN e.conv_category IN ('older-kids-shoes','little-kids-shoes','baby-toddlers-shoes') THEN 'kids'
      ELSE 'unisex'
    END AS gender,

    e.age_group,
    e.brand,

    CASE
      WHEN sa.sizes_us IS NOT NULL
        THEN object_insert(coalesce(e.extra, parse_json('{}')), 'sizes', sa.sizes_us, TRUE)
      ELSE e.extra
    END AS extra

  FROM categorized e
  LEFT JOIN sizes_agg sa
    ON sa.dwid = e.dwid AND sa.id = e.id

  {% if is_incremental() %}
    -- Prune to only rows newer than what target already has (by date, not dwid prefix)
    WHERE e.date_key > coalesce((SELECT max(year*10000+month*100+day) FROM {{ this }}), 0)
      AND e.price_original <> 0
  {% else %}
    WHERE e.price_original <> 0
  {% endif %}
),

-- 12) Safety de-dupe on dwid (belt & suspenders)
final_dedup AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      row_number() OVER (PARTITION BY t.dwid ORDER BY t.year DESC, t.month DESC, t.day DESC) AS rn
    FROM to_load t
  )
  WHERE rn = 1
)

SELECT * FROM final_dedup
