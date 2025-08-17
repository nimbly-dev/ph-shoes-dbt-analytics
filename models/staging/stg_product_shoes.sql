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
    trim(id)                              AS id,
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

-- 2A) Normalize base fields
enriched_a AS (
  SELECT
    ymd,
    EXTRACT(year  FROM loaded_at)::int    AS year,
    EXTRACT(month FROM loaded_at)::int    AS month,
    EXTRACT(day   FROM loaded_at)::int    AS day,

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

    CASE
      WHEN try_parse_json(gender_raw) IS NOT NULL
           AND typeof(try_parse_json(gender_raw)) = 'ARRAY'
        THEN try_parse_json(gender_raw)
      WHEN gender_raw IS NOT NULL
        THEN array_construct(gender_raw)
      ELSE NULL
    END                                             AS gender_arr,

    CASE
      WHEN lower(url) LIKE '%://www.nike.%'                          THEN 'nike'
      WHEN lower(url) LIKE '%://www.adidas.%'                        THEN 'adidas'
      WHEN lower(url) LIKE '%://atmos.ph/collections/new-balance/%'  THEN 'newbalance'
      WHEN lower(url) LIKE '%://worldbalance.%'                      THEN 'worldbalance'
      WHEN lower(url) LIKE '%://www.asics.%'                         THEN 'asics'
      WHEN lower(url) LIKE '%://www.hoka.%'                          THEN 'hoka'
      ELSE coalesce(lower(replace(brand_raw,' ','')), lower(brand_raw))
    END                                             AS brand,

    CASE
      WHEN extra_raw IS NULL OR extra_raw = ''      THEN parse_json('{}')
      WHEN try_parse_json(extra_raw) IS NOT NULL     THEN try_parse_json(extra_raw)
      ELSE parse_json('{}')
    END                                             AS extra
  FROM raw_csv
),

-- 2B) Gender + DWID
enriched AS (
  SELECT
    (id || ymd) AS dwid,
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

-- 3B) Canonical group for size mapping (men|women|kids)
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

-- 4) Grouped size map from seed (grp: men|women|kids)
size_map_raw AS (
  SELECT
    lower(grp)       AS grp,
    us,
    uk::float        AS uk,
    eu::float        AS eu,
    mondo_cm::float  AS cm,
    CASE WHEN upper(us) LIKE '%Y' OR upper(us) LIKE '%C' THEN 2 ELSE 1 END AS pref_rank
  FROM {{ ref('shoe_size_map') }}
),
size_map AS (
  SELECT grp, us, uk, eu, cm
  FROM (
    SELECT
      grp, us, uk, eu, cm,
      row_number() OVER (
        PARTITION BY grp, uk, eu, cm
        ORDER BY pref_rank ASC
      ) AS rn
    FROM size_map_raw
  )
  WHERE rn = 1
),

-- 5) Flatten sizes: extra.sizes, extra.variants[*].size, OR nested string JSON extra.extra.sizes
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
      iff(p.extra:variants IS NOT NULL, p.extra:variants,
      iff( try_parse_json(p.extra:extra::string) IS NOT NULL
           AND try_parse_json(p.extra:extra::string):sizes IS NOT NULL,
           try_parse_json(p.extra:extra::string):sizes,
           parse_json('[]')
      )))
  ) f
),

-- 6) Parse tokens + number
sizes_parsed AS (
  SELECT
    s.*,
    upper(trim(size_raw)) AS size_u,

    /* kids US token like 1Y / 12C (anywhere) */
    iff(regexp_like(size_u, '([0-9]+(\.[0-9])?)\s*[YC]\b'),
        regexp_substr(size_u, '([0-9]+(\.[0-9])?)\s*[YC]\b', 1, 1, 'e'), NULL) AS us_kids_token,

    -- token flags (loose to catch 'US M 9', 'UKM8', etc.)
    iff(regexp_like(size_u, 'US'),                 1, 0) AS has_us,
    iff(regexp_like(size_u, 'UK'),                 1, 0) AS has_uk,
    iff(regexp_like(size_u, '(EU|EUR)'),           1, 0) AS has_eu,
    iff(regexp_like(size_u, '(CM|MONDO|JP)'),      1, 0) AS has_cm,

    -- first numeric anywhere
    try_to_decimal(regexp_substr(size_u, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)) AS raw_num,

    -- heuristics
    iff(try_to_decimal(regexp_substr(size_u, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)) BETWEEN 31 AND 52, 1, 0) AS is_two_digit_eu_hint,
    iff(lower(s.brand) IN ('asics','hoka'), 1, 0) AS is_uk_brand_fallback
  FROM sizes_flat s
),

-- 7) Decide source system per your rule set
sizes_typed AS (
  SELECT
    p.*,
    CASE
      WHEN p.us_kids_token IS NOT NULL THEN 'US'
      WHEN p.has_us = 1                 THEN 'US'
      WHEN p.has_uk = 1                 THEN 'UK'
      WHEN p.has_eu = 1                 THEN 'EU'
      WHEN p.has_cm = 1                 THEN 'CM'
      WHEN p.raw_num IS NOT NULL AND p.is_uk_brand_fallback = 1 THEN 'UK'  -- asics/hoka bare numbers => UK
      WHEN p.raw_num IS NOT NULL AND p.is_two_digit_eu_hint = 1 THEN 'EU'  -- 41, 43, 45, 47 ...
      WHEN p.raw_num IS NOT NULL THEN 'NUM'                                 -- fallback: try EU → UK → CM
      ELSE NULL
    END AS size_system,

    -- numeric helpers
    CASE WHEN (p.has_us = 1 AND p.us_kids_token IS NULL) THEN p.raw_num END AS us_adult_num,
    CASE WHEN p.has_uk = 1 THEN p.raw_num END AS uk_num,
    CASE WHEN p.has_eu = 1 THEN p.raw_num END AS eu_num,
    CASE WHEN p.has_cm = 1 THEN p.raw_num END AS cm_num
  FROM sizes_parsed p
),

-- 8) Map to US via seed by group (NUM => EU → UK → CM)
sizes_mapped AS (
  SELECT
    t.*,
    m.us AS us_from_map
  FROM sizes_typed t
  LEFT JOIN size_map m
    ON m.grp = t.conv_group
   AND (
         (t.size_system = 'UK'  AND m.uk = t.uk_num)
      OR (t.size_system = 'EU'  AND m.eu = t.eu_num)
      OR (t.size_system = 'CM'  AND abs(m.cm - t.cm_num) <= 0.1)
      OR (t.size_system = 'NUM' AND m.eu = t.raw_num)
      OR (t.size_system = 'NUM' AND m.uk = t.raw_num)
      OR (t.size_system = 'NUM' AND abs(m.cm - t.raw_num) <= 0.1)
   )
),

-- 9) Final US size (numeric string only)
sizes_final AS (
  SELECT
    dwid, id,
    CASE
      WHEN size_system = 'US' AND us_adult_num IS NOT NULL THEN
        CASE
          WHEN round(us_adult_num, 1) = trunc(round(us_adult_num, 1))
            THEN to_varchar(trunc(round(us_adult_num, 1)))
          ELSE to_varchar(round(us_adult_num, 1))
        END
      WHEN size_system = 'US' AND us_adult_num IS NULL AND us_kids_token IS NOT NULL THEN
        to_varchar(try_to_decimal(regexp_substr(us_kids_token, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)))
      ELSE
        CASE WHEN us_from_map IS NOT NULL THEN
          CASE
            WHEN round(try_to_decimal(regexp_substr(us_from_map, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)), 1)
                 = trunc(round(try_to_decimal(regexp_substr(us_from_map, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)), 1))
              THEN to_varchar(trunc(round(try_to_decimal(regexp_substr(us_from_map, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)), 1)))
            ELSE to_varchar(round(try_to_decimal(regexp_substr(us_from_map, '([0-9]+(\.[0-9])?)', 1, 1, 'e', 1)), 1))
          END
        ELSE NULL END
    END AS size_us_num
  FROM sizes_mapped
  WHERE size_raw IS NOT NULL
),

-- 10) Aggregate numeric US sizes per product
sizes_agg AS (
  SELECT
    dwid, id,
    array_agg(DISTINCT size_us_num) AS sizes_us
  FROM sizes_final
  WHERE size_us_num IS NOT NULL
  GROUP BY dwid, id
),

-- 11) Final projection (ALWAYS overwrite extra.sizes with numeric US array)
to_load AS (
  SELECT
    e.dwid, e.year, e.month, e.day,
    e.id, e.title, e.subtitle, e.url, e.image,
    e.price_sale, e.price_original,

    CASE
      WHEN e.gender IS NOT NULL THEN e.gender
      WHEN e.conv_category = 'women-shoes' THEN 'women'
      WHEN e.conv_category IN ('older-kids-shoes','little-kids-shoes','baby-toddlers-shoes') THEN 'kids'
      ELSE 'unisex'
    END AS gender,

    e.age_group,
    e.brand,

    object_insert(
      coalesce(e.extra, parse_json('{}')),
      'sizes',
      coalesce(sa.sizes_us, array_construct()),
      TRUE
    ) AS extra

  FROM categorized e
  LEFT JOIN sizes_agg sa
    ON sa.dwid = e.dwid AND sa.id = e.id

  {% if is_incremental() %}
    WHERE e.date_key > coalesce((SELECT max(year*10000+month*100+day) FROM {{ this }}), 0)
      AND e.price_original <> 0
  {% else %}
    WHERE e.price_original <> 0
  {% endif %}
),

-- 12) Safety de-dupe
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
