{{ 
  config(
    materialized         = 'incremental',
    incremental_strategy = 'merge',
    unique_key           = ['id','dwid']
  )
}}

WITH raw_csv AS (
  SELECT
    t.$1::STRING      AS id,
    t.$2::STRING      AS title,
    t.$3::STRING      AS subTitle,
    t.$4::STRING      AS url,
    t.$5::STRING      AS image,
    t.$6::FLOAT       AS price_sale,
    t.$7::FLOAT       AS price_original,
    -- read gender as VARIANT so we can branch on its true type
    t.$8::VARIANT     AS gender_raw,
    t.$9::STRING      AS age_group,
    metadata$filename AS file_path,
    CURRENT_TIMESTAMP() AS loaded_at
  FROM @PH_SHOES_DB.RAW.S3_RAW_PRODUCT_SHOES_STAGE
    (file_format => 'PH_SHOES_DB.RAW.CSV_RAW_FORMAT') t
),

enriched AS (
  SELECT
    -- batch identifiers
    TO_VARCHAR(DATE_TRUNC('day', loaded_at), 'YYYYMMDD') AS dwid,
    YEAR(loaded_at)::INT    AS year,
    MONTH(loaded_at)::INT   AS month,
    DAY(loaded_at)::INT     AS day,

    -- raw fields
    id,
    title,
    subTitle,
    url,
    image,
    price_sale,
    price_original,
    age_group,
    file_path,

    -- STEP 1: normalize anything → an ARRAY variant:
    CASE
      WHEN TYPEOF(gender_raw) = 'ARRAY' THEN
        gender_raw

      WHEN TRY_PARSE_JSON(gender_raw) IS NOT NULL
           AND TYPEOF(TRY_PARSE_JSON(gender_raw)) = 'ARRAY' THEN
        TRY_PARSE_JSON(gender_raw)

      WHEN gender_raw IS NOT NULL THEN
        ARRAY_CONSTRUCT(gender_raw::STRING)

      ELSE
        NULL
    END AS gender_arr,

    -- STEP 2: collapse that ARRAY → single string
    CASE
      WHEN gender_arr IS NULL THEN NULL
      WHEN ARRAY_SIZE(gender_arr) > 1 THEN 'unisex'
      WHEN ARRAY_SIZE(gender_arr) = 1 THEN LOWER(gender_arr[0]::STRING)
      ELSE NULL
    END AS gender,

    -- derive brand from filename
    SPLIT_PART(SPLIT_PART(file_path, '/', -1), '_', 1) AS brand

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
    WHERE dwid > COALESCE((SELECT MAX(dwid) FROM {{ this }}), '00000000')
  {% endif %}
)

SELECT * FROM to_load
