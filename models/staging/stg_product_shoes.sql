{{ 
  config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['id','dwid']
  )
}}

WITH raw_csv AS (
  SELECT
    id,
    title,
    subTitle,
    url,
    image,
    price_sale,
    price_original,
    gender     AS gender_raw,
    age_group,
    brand      AS brand_raw,
    loaded_at
  FROM PH_SHOES_DB.RAW.RAW_PRODUCT_SHOES_RAW
),

enriched AS (
  SELECT
    -- batch identifiers
    TO_VARCHAR( DATE_TRUNC('day', loaded_at), 'YYYYMMDD') AS dwid,
    YEAR(loaded_at)::INT   AS year,
    MONTH(loaded_at)::INT  AS month,
    DAY(loaded_at)::INT    AS day,

    -- raw fields
    id,
    title,
    subTitle,
    url,
    image,
    price_sale,
    price_original,
    age_group,

    -- STEP 1: normalize to ARRAY
    CASE
      WHEN TYPEOF(gender_raw) = 'ARRAY' THEN gender_raw
      WHEN TRY_PARSE_JSON(gender_raw) IS NOT NULL
           AND TYPEOF(TRY_PARSE_JSON(gender_raw)) = 'ARRAY'
        THEN TRY_PARSE_JSON(gender_raw)
      WHEN gender_raw IS NOT NULL
        THEN ARRAY_CONSTRUCT(gender_raw::STRING)
      ELSE NULL
    END AS gender_arr,

    -- STEP 2: collapse array → single string
    CASE
      WHEN gender_arr IS NULL         THEN NULL
      WHEN ARRAY_SIZE(gender_arr) > 1 THEN 'unisex'
      WHEN ARRAY_SIZE(gender_arr) = 1 THEN LOWER(gender_arr[0]::STRING)
      ELSE NULL
    END AS gender,

    -- use the existing brand column
    brand_raw AS brand

  FROM raw_csv
),

to_load AS (
  SELECT
    dwid,
    year,
    month,
    day,
    id,
    title,
    subTitle,
    url,
    image,
    price_sale,
    price_original,
    gender,
    age_group,
    brand
  FROM enriched

  {% if is_incremental() %}
    WHERE dwid > COALESCE(
      (SELECT MAX(dwid) FROM {{ this }}), 
      '00000000'
    )
  {% endif %}
)

SELECT * FROM to_load;
